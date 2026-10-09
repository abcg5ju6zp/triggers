/*
Copyright 2021 The Tekton Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package adapter

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	clusterinterceptorsinformer "github.com/tektoncd/triggers/pkg/client/injection/informers/triggers/v1alpha1/clusterinterceptor"
	interceptorsinformer "github.com/tektoncd/triggers/pkg/client/injection/informers/triggers/v1alpha1/interceptor"
	clustertriggerbindingsinformer "github.com/tektoncd/triggers/pkg/client/injection/informers/triggers/v1beta1/clustertriggerbinding"
	eventlistenerinformer "github.com/tektoncd/triggers/pkg/client/injection/informers/triggers/v1beta1/eventlistener"
	triggersinformer "github.com/tektoncd/triggers/pkg/client/injection/informers/triggers/v1beta1/trigger"
	triggerbindingsinformer "github.com/tektoncd/triggers/pkg/client/injection/informers/triggers/v1beta1/triggerbinding"
	triggertemplatesinformer "github.com/tektoncd/triggers/pkg/client/injection/informers/triggers/v1beta1/triggertemplate"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/tektoncd/triggers/pkg/client/clientset/versioned/scheme"
	"github.com/tektoncd/triggers/pkg/sink"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing/pkg/adapter/v2"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection/clients/dynamicclient"
	"knative.dev/pkg/logging"
)

type envConfig struct {
	adapter.EnvConfig
}

func NewEnvConfig() adapter.EnvConfigAccessor {
	return &envConfig{}
}

var (
	interval = 10 * time.Second
	timeout  = 1 * time.Minute

	// serverShutdownTimeout bounds how long the HTTP listener waits for
	// in-flight HTTP handlers (which return as soon as the 202 is written).
	serverShutdownTimeout = 10 * time.Second
	// drainTimeout bounds the wind-down after the grace period expired:
	// in-flight interceptor calls are cancelled, while Kubernetes create calls
	// already submitted run on a detached context and must return first.
	drainTimeout = 8 * time.Second
	// notificationsTimeout bounds the wait for handler-level CloudEvents that
	// were already being delivered when shutdown started.
	notificationsTimeout = 5 * time.Second
)

// sinker implements the adapter for an event listener.
type sinker struct {
	Logger    *zap.SugaredLogger
	Namespace string

	Args     sink.Args
	Clients  sink.Clients
	Recorder *sink.Recorder

	injCtx context.Context //nolint:containedctx

	// recorderStop is closed only after all accepted events have converged
	// during shutdown, so Kubernetes Event deliveries for terminal trigger
	// states are not cut off ahead of the result they describe.
	recorderStop chan struct{}
}

var _ adapter.Adapter = (*sinker)(nil)

func (s *sinker) createRecorder(ctx context.Context, agentName string) record.EventRecorder {
	logger := logging.FromContext(ctx)

	recorder := controller.GetEventRecorder(ctx)
	if recorder == nil {
		// Create event broadcaster
		logger.Debug("Creating event broadcaster")
		eventBroadcaster := record.NewBroadcaster()
		watches := []watch.Interface{
			eventBroadcaster.StartLogging(logger.Named("event-broadcaster").Infof),
			eventBroadcaster.StartRecordingToSink(
				&v1.EventSinkImpl{Interface: s.Clients.K8sClient.CoreV1().Events("")}),
		}

		recorder = eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: agentName})
		go func() {
			// Do not stop on the signal context: shutdown closes recorderStop
			// only after all in-flight events and their terminal events have
			// been processed, so terminal Kubernetes Events are not dropped.
			<-s.recorderStop
			for _, w := range watches {
				w.Stop()
			}
		}()
	}

	return recorder
}

func (s *sinker) getHTTPClient() (*http.Client, error) {
	var tlsConfig *tls.Config

	certPool := x509.NewCertPool()

	err := s.getCertFromInterceptor(certPool)
	if err != nil {
		return &http.Client{}, fmt.Errorf("Timed out waiting on CaBundle to available for clusterInterceptor: %w", err)
	}

	// running go routine here to add/update certPool if there is new or change in caCert bundle.
	// caCert changes if certs expired, if someone adds new ClusterInterceptor with caBundle
	ticker := time.NewTicker(time.Minute)
	go func() {
		for {
			<-ticker.C
			if err := s.getCertFromInterceptor(certPool); err != nil {
				s.Logger.Fatalf("Timed out waiting on CaBundle to available for clusterInterceptor: %v", err)
			}
		}
	}()

	tlsConfig = &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12, // Added MinVersion to avoid  G402: TLS MinVersion too low. (gosec)
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
			Dial: (&net.Dialer{
				Timeout:   s.Args.ElHTTPClientReadTimeOut * time.Second, //nolint:durationcheck
				KeepAlive: s.Args.ElHTTPClientKeepAlive * time.Second,   //nolint:durationcheck
			}).Dial,
			TLSHandshakeTimeout:   s.Args.ElHTTPClientTLSHandshakeTimeout * time.Second,         //nolint:durationcheck
			ResponseHeaderTimeout: s.Args.ElHTTPClientResponseHeaderTimeout * time.Second,       //nolint:durationcheck
			ExpectContinueTimeout: s.Args.ElHTTPClientExpectContinueTimeout * time.Second}}, nil //nolint:durationcheck
}

func (s *sinker) getCertFromInterceptor(certPool *x509.CertPool) error {
	var (
		caCert     []byte
		count      int
		httpsCILen int
	)
	ctx := context.Background()
	if err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(_ context.Context) (bool, error) {
		clusterInterceptorList, err := clusterinterceptorsinformer.Get(s.injCtx).Lister().List(labels.NewSelector()) //nolint:contextcheck
		if err != nil {
			return false, err
		}

		for i := range clusterInterceptorList {
			if v, k := clusterInterceptorList[i].Labels["server/type"]; k && v == "https" {
				httpsCILen++
				if !bytes.Equal(clusterInterceptorList[i].Spec.ClientConfig.CaBundle, []byte{}) {
					caCert = clusterInterceptorList[i].Spec.ClientConfig.CaBundle
					if ok := certPool.AppendCertsFromPEM(caCert); !ok {
						return false, fmt.Errorf("unable to parse cert from %s", caCert)
					}
					count++
				}
			}
		}

		if httpsCILen != count {
			return false, errors.New("empty caBundle in clusterInterceptor spec")
		}

		httpsCILen = 0
		count = 0

		interceptorList, err := interceptorsinformer.Get(s.injCtx).Lister().Interceptors(s.Namespace).List(labels.Everything()) //nolint:contextcheck
		if err != nil {
			return false, err
		}

		for i := range interceptorList {
			if v, k := interceptorList[i].Labels["server/type"]; k && v == "https" {
				httpsCILen++
				if !bytes.Equal(interceptorList[i].Spec.ClientConfig.CaBundle, []byte{}) {
					caCert = interceptorList[i].Spec.ClientConfig.CaBundle
					if ok := certPool.AppendCertsFromPEM(caCert); !ok {
						return false, fmt.Errorf("unable to parse cert from %s", caCert)
					}
					count++
				}
			}
		}
		if httpsCILen != count {
			return false, errors.New("empty caBundle in interceptor spec")
		}

		return true, nil
	}); err != nil {
		return fmt.Errorf("Timed out waiting on CaBundle to available for Interceptor: %w", err)
	}
	return nil
}

func (s *sinker) Start(ctx context.Context) error {
	clientObj, err := s.getHTTPClient()
	if err != nil {
		return err
	}
	// Create EventListener Sink

	dynamicClient := dynamicclient.Get(ctx)

	// A single tracker accounts for every accepted eventID and its outer
	// triggers/groups as well as group-internal branches. The WaitGroup is
	// retained as the legacy accounting hook.
	wg := &sync.WaitGroup{}
	inflight := sink.NewInflightTracker(wg)

	r := sink.Sink{
		KubeClientSet:          kubeclient.Get(ctx),
		DiscoveryClient:        s.Clients.DiscoveryClient,
		DynamicClient:          dynamicClient,
		TriggersClient:         s.Clients.TriggersClient,
		HTTPClient:             clientObj,
		CEClient:               s.Clients.CEClient,
		EventListenerName:      s.Args.ElName,
		EventListenerNamespace: s.Args.ElNamespace,
		PayloadValidation:      s.Args.PayloadValidation,
		MaxBodySize:            s.Args.ELMaxBodySize,
		Logger:                 s.Logger,
		Recorder:               s.Recorder,
		CloudEventURI:          s.Args.CloudEventURI,
		Auth:                   sink.DefaultAuthOverride{},
		WGProcessTriggers:      wg,
		Inflight:               inflight,
		EventRecorder:          s.createRecorder(s.injCtx, "EventListener"), //nolint:contextcheck

		// Register all the listers we'll need
		EventListenerLister:         eventlistenerinformer.Get(s.injCtx).Lister(),          //nolint:contextcheck
		TriggerLister:               triggersinformer.Get(s.injCtx).Lister(),               //nolint:contextcheck
		TriggerBindingLister:        triggerbindingsinformer.Get(s.injCtx).Lister(),        //nolint:contextcheck
		ClusterTriggerBindingLister: clustertriggerbindingsinformer.Get(s.injCtx).Lister(), //nolint:contextcheck
		TriggerTemplateLister:       triggertemplatesinformer.Get(s.injCtx).Lister(),       //nolint:contextcheck
		ClusterInterceptorLister:    clusterinterceptorsinformer.Get(s.injCtx).Lister(),    //nolint:contextcheck
		InterceptorLister:           interceptorsinformer.Get(s.injCtx).Lister(),           //nolint:contextcheck
	}

	mux := http.NewServeMux()
	eventHandler := http.HandlerFunc(r.HandleEvent)
	metricsRecorder := &sink.MetricsHandler{Handler: r.IsValidPayload(eventHandler)}

	mux.HandleFunc("/", metricsRecorder.Intercept(r.NewMetricsRecorderInterceptor()))

	// For handling Liveness Probe
	// TODO(dibyom): Livness, metrics etc. should be on a separate port
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	srv := &http.Server{
		Addr:              ":" + s.Args.Port,
		ReadHeaderTimeout: s.Args.ELReadTimeOut * time.Second,  //nolint:durationcheck
		ReadTimeout:       s.Args.ELReadTimeOut * time.Second,  //nolint:durationcheck
		WriteTimeout:      s.Args.ELWriteTimeOut * time.Second, //nolint:durationcheck
		IdleTimeout:       s.Args.ELIdleTimeOut * time.Second,  //nolint:durationcheck
		Handler: http.TimeoutHandler(mux,
			s.Args.ELTimeOutHandler*time.Second, "EventListener Timeout!\n"), //nolint:durationcheck
	}

	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", srv.Addr)
	if err != nil {
		return err
	}
	serverErr := make(chan error, 1)
	go func() {
		var serveErr error
		if s.Args.Cert == "" && s.Args.Key == "" {
			serveErr = srv.Serve(ln)
		} else {
			serveErr = srv.ServeTLS(ln, s.Args.Cert, s.Args.Key)
		}
		if !errors.Is(serveErr, http.ErrServerClosed) {
			serverErr <- serveErr
		}
	}()

	// Serve until the listener fails or a termination signal cancels ctx.
	select {
	case serveErr := <-serverErr:
		return serveErr
	case <-ctx.Done():
		s.Logger.Info("termination signal received: stop accepting new events and wait for accepted events to finish")
	}

	return s.shutdown(ctx, srv, inflight, serverErr)
}

// shutdown runs the ordered graceful shutdown of the sink:
//  1. stop accepting new HTTP requests (accepted events keep processing),
//  2. wait for every accepted eventID's outer triggers/groups and group
//     branches to converge during the grace period,
//  3. if the grace period expires, cancel work that has not started yet and
//     wait for Kubernetes calls already submitted to return; those branches
//     then report a deterministic incomplete (failed) terminal result,
//  4. wait for handler-level CloudEvent notifications in flight,
//  5. only then release the Kubernetes event recorder.
func (s *sinker) shutdown(sigCtx context.Context, srv *http.Server, inflight *sink.InflightTracker, serverErr <-chan error) error {
	inflight.BeginShutdown()

	// Phase 1: close the listener first. Handlers return as soon as the 202 is
	// written; the actual trigger processing is tracked by inflight below.
	serverCtx, cancelServer := context.WithTimeout(context.WithoutCancel(sigCtx), serverShutdownTimeout)
	if err := srv.Shutdown(serverCtx); err != nil {
		s.Logger.Warnf("HTTP server did not stop within %s, forcing close: %v", serverShutdownTimeout, err)
		if closeErr := srv.Close(); closeErr != nil {
			s.Logger.Warnf("error closing HTTP server: %v", closeErr)
		}
	}
	cancelServer()

	// Phase 2: give accepted events the grace period to converge normally.
	gracePeriod := s.Args.ELShutdownGracePeriod * time.Second //nolint:durationcheck
	if gracePeriod <= 0 {
		gracePeriod = 20 * time.Second
	}
	graceCtx, cancelGrace := context.WithTimeout(context.WithoutCancel(sigCtx), gracePeriod)
	completed := inflight.WaitAll(graceCtx)
	cancelGrace()

	if !completed {
		// Phase 3: cancel events that have not finished so branches skip work
		// they have not started; branches with a Kubernetes create in flight
		// wait for that call (it uses a context detached from cancellation)
		// and then emit the deterministic interrupted terminal result.
		s.Logger.Warnf("shutdown grace period of %s expired; cancelling unstarted work for events: %v", gracePeriod, inflight.InflightEventIDs())
		inflight.CancelUnfinished()

		drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(sigCtx), drainTimeout)
		if !inflight.WaitAll(drainCtx) {
			s.Logger.Errorf("event processing did not wind down within %s for events: %v", drainTimeout, inflight.InflightEventIDs())
		}
		cancelDrain()
	}

	// Phase 4: let handler-level CloudEvent notifications already in flight
	// finish delivering.
	notifyCtx, cancelNotify := context.WithTimeout(context.WithoutCancel(sigCtx), notificationsTimeout)
	if !inflight.WaitNotifications(notifyCtx) {
		s.Logger.Warnf("not all status CloudEvents were delivered within %s", notificationsTimeout)
	}
	cancelNotify()

	// Phase 5: terminal Kubernetes Events have all been emitted now; release
	// the event broadcaster so queued events can be flushed.
	close(s.recorderStop)

	select {
	case serveErr := <-serverErr:
		return serveErr
	default:
		return nil
	}
}

func New(sinkArgs sink.Args, sinkClients sink.Clients, recorder *sink.Recorder) adapter.AdapterConstructor {
	return func(ctx context.Context, processed adapter.EnvConfigAccessor, _ cloudevents.Client) adapter.Adapter {
		env := processed.(*envConfig)
		logger := logging.FromContext(ctx)

		return &sinker{
			Logger:       logger,
			Namespace:    env.Namespace,
			Args:         sinkArgs,
			Clients:      sinkClients,
			Recorder:     recorder,
			injCtx:       ctx,
			recorderStop: make(chan struct{}),
		}
	}
}
