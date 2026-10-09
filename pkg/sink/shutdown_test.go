/*
Copyright 2026 The Tekton Authors

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

package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	triggersv1beta1 "github.com/tektoncd/triggers/pkg/apis/triggers/v1beta1"
	"github.com/tektoncd/triggers/pkg/reconciler/events"
	"github.com/tektoncd/triggers/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	"knative.dev/pkg/ptr"
)

func TestInflightTracker_RejectsAfterBeginShutdown(t *testing.T) {
	tracker := NewInflightTracker(nil)

	if _, err := tracker.accept("a", 0); err != nil {
		t.Fatalf("accept before shutdown should succeed, got: %v", err)
	}
	if tracker.inflightCount() != 0 {
		t.Fatalf("event with no branches should not be tracked, got %d", tracker.inflightCount())
	}

	tracker.BeginShutdown()

	if _, err := tracker.accept("b", 1); !errors.Is(err, ErrSinkShuttingDown) {
		t.Fatalf("accept after BeginShutdown should fail with ErrSinkShuttingDown, got: %v", err)
	}
}

func TestInflightTracker_WaitsForOuterAndGroupBranches(t *testing.T) {
	tracker := NewInflightTracker(nil)

	// One trigger group outer branch, which fans out into 3 triggers.
	group, err := tracker.accept("event-with-group", 1)
	if err != nil {
		t.Fatalf("accept() unexpected error: %v", err)
	}
	group.add(3)

	if tracker.inflightCount() != 1 {
		t.Fatalf("expected 1 inflight event, got %d", tracker.inflightCount())
	}

	if tracker.WaitAll(shortCtx(t, 50*time.Millisecond)) {
		t.Fatal("WaitAll should time out while branches are still running")
	}

	// Outer group branch finishes; the inner branches must still keep the
	// event in flight.
	group.done()
	if tracker.WaitAll(shortCtx(t, 50*time.Millisecond)) {
		t.Fatal("event should remain in flight while group-internal branches run")
	}

	for range 3 {
		group.done()
	}

	if !tracker.WaitAll(shortCtx(t, 2*time.Second)) {
		t.Fatal("WaitAll should return once every branch converged")
	}
	if tracker.inflightCount() != 0 {
		t.Fatalf("expected no inflight events after drain, got %d", tracker.inflightCount())
	}
}

func TestInflightTracker_GraceExpiryCancelsUnfinished(t *testing.T) {
	tracker := NewInflightTracker(nil)
	ev, err := tracker.accept("slow-event", 1)
	if err != nil {
		t.Fatalf("accept() unexpected error: %v", err)
	}

	finished := make(chan struct{})
	go func() {
		<-ev.ctx.Done()
		ev.done()
		close(finished)
	}()

	tracker.BeginShutdown()
	if tracker.WaitAll(shortCtx(t, 50*time.Millisecond)) {
		t.Fatal("WaitAll should report unfinished work during the grace period")
	}

	tracker.CancelUnfinished()
	if ev.ctx.Err() == nil {
		t.Fatal("CancelUnfinished should cancel the per-event context")
	}

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled branch did not finish")
	}
	if !tracker.WaitAll(shortCtx(t, 2*time.Second)) {
		t.Fatal("WaitAll should drain after cancellation")
	}
}

func TestInflightTracker_Notifications(t *testing.T) {
	tracker := NewInflightTracker(nil)
	release := make(chan struct{})
	tracker.notifyGo(func() {
		<-release
	})

	if tracker.WaitNotifications(shortCtx(t, 50*time.Millisecond)) {
		t.Fatal("WaitNotifications should time out while a notification is in flight")
	}
	close(release)
	if !tracker.WaitNotifications(shortCtx(t, 2*time.Second)) {
		t.Fatal("WaitNotifications should return after tracked notifications finish")
	}
}

func shortCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestHandleEvent_RejectsNewEventsDuringShutdown(t *testing.T) {
	const elName = "test-el"
	res := test.Resources{
		EventListeners: []*triggersv1beta1.EventListener{{
			ObjectMeta: metav1.ObjectMeta{Name: elName, Namespace: namespace, UID: types.UID(elUID)},
		}},
	}
	sink, dynamicClient := getSinkAssets(t, res, elName, nil)
	sink.Inflight.BeginShutdown()

	ts := httptest.NewServer(http.HandlerFunc(sink.HandleEvent))
	defer ts.Close()

	resp, err := http.Post(ts.URL, "application/json", bytes.NewReader([]byte(`{"repository":{"url":"testurl"}}`)))
	if err != nil {
		t.Fatalf("error sending request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 while shutting down, got %d", resp.StatusCode)
	}
	var body Response
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.EventID == "" {
		t.Error("expected the rejected event to still carry an eventID")
	}
	if !strings.Contains(body.ErrorMessage, ErrSinkShuttingDown.Error()) {
		t.Errorf("expected error message %q, got %q", ErrSinkShuttingDown, body.ErrorMessage)
	}
	if len(dynamicClient.Actions()) != 0 {
		t.Errorf("rejected event must not create resources, got actions: %v", dynamicClient.Actions())
	}
}

func TestHandleEvent_GracefulDrainCompletesAcceptedEvent(t *testing.T) {
	const elName = "test-el"
	res := gitCloneEventListenerRes(t, elName)
	sink, dynamicClient := getSinkAssets(t, res, elName, nil)

	ts := httptest.NewServer(http.HandlerFunc(sink.HandleEvent))
	defer ts.Close()

	resp, err := http.Post(ts.URL, "application/json",
		bytes.NewReader([]byte(`{"head_commit":{"id":"testrevision"},"repository":{"url":"testurl"},"foo":"bar"}`)))
	if err != nil {
		t.Fatalf("error sending request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 during normal operation, got %d", resp.StatusCode)
	}

	if !sink.Inflight.WaitAll(shortCtx(t, 5*time.Second)) {
		t.Fatal("accepted event did not finish draining")
	}
	if sink.Inflight.inflightCount() != 0 {
		t.Fatalf("expected no inflight events after drain, got %d", sink.Inflight.inflightCount())
	}
	if len(dynamicClient.Actions()) != 1 {
		t.Fatalf("expected exactly one resource creation, got %d actions", len(dynamicClient.Actions()))
	}
}

func TestHandleEvent_InterruptedBranchEmitsDeterministicTerminal(t *testing.T) {
	t.Setenv("EL_EVENT", "enable")
	const elName = "test-el"

	// A webhook interceptor that stays in flight until its request is aborted
	// by shutdown, simulating a branch blocked in the interceptor phase. The
	// release channel lets the handler return at cleanup: the httptest proxy
	// connection does not always observe client-side context cancellation on
	// the server side promptly.
	interceptorEntered := make(chan struct{}, 1)
	releaseInterceptor := make(chan struct{})
	blockingInterceptor := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case interceptorEntered <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-releaseInterceptor:
		}
	})

	res := test.Resources{
		EventListeners: []*triggersv1beta1.EventListener{{
			ObjectMeta: metav1.ObjectMeta{Name: elName, Namespace: namespace, UID: types.UID(elUID)},
			Spec: triggersv1beta1.EventListenerSpec{
				Triggers: []triggersv1beta1.EventListenerTrigger{{
					Name: "blocking-trigger",
					Bindings: []*triggersv1beta1.EventListenerBinding{
						{Name: "url", Value: ptr.String("https://github.com/tektoncd/triggers")},
						{Name: "revision", Value: ptr.String("master")},
						{Name: "name", Value: ptr.String("shutdown-run")},
						{Name: "app", Value: ptr.String("triggers")},
						{Name: "type", Value: ptr.String("application/json")},
					},
					Template: &triggersv1beta1.EventListenerTemplate{Spec: makeGitCloneTTSpec(t, "shutdown-run")},
					Interceptors: []*triggersv1beta1.EventInterceptor{{
						Webhook: &triggersv1beta1.WebhookInterceptor{
							ObjectRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Service", Name: "foo"},
						},
					}},
				}},
			},
		}},
	}
	sink, dynamicClient := getSinkAssets(t, res, elName, blockingInterceptor)
	// Registered after the interceptor httptest server's own cleanup (LIFO), so
	// the blocking handler is released before that server tries to Close.
	t.Cleanup(func() { close(releaseInterceptor) })
	recorder := newRecordingEventRecorder()
	sink.EventRecorder = recorder

	ts := httptest.NewServer(http.HandlerFunc(sink.HandleEvent))
	defer ts.Close()

	resp, err := http.Post(ts.URL, "application/json", bytes.NewReader([]byte(`{"repository":{"url":"testurl"}}`)))
	if err != nil {
		t.Fatalf("error sending request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 for an accepted event, got %d", resp.StatusCode)
	}

	select {
	case <-interceptorEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking interceptor was never invoked")
	}

	// Grace period elapses while the interceptor call is still in flight.
	sink.Inflight.BeginShutdown()
	if sink.Inflight.WaitAll(shortCtx(t, 100*time.Millisecond)) {
		t.Fatal("WaitAll should not complete while the interceptor call is blocked")
	}
	sink.Inflight.CancelUnfinished()

	if !sink.Inflight.WaitAll(shortCtx(t, 5*time.Second)) {
		t.Fatal("branches did not wind down after cancellation")
	}

	// No resource creation started; the deterministic incomplete terminal
	// result must have been emitted for the same eventID.
	if len(dynamicClient.Actions()) != 0 {
		t.Errorf("no resources should be created for an interrupted branch, got: %v", dynamicClient.Actions())
	}
	calls := recorder.snapshot()
	var failed *recordedEvent
	for i := range calls {
		if calls[i].reason == events.TriggerProcessingSuccessfulV1 {
			t.Errorf("successful terminal event must not be emitted after interruption: %+v", calls[i])
		}
		if calls[i].reason == events.TriggerProcessingFailedV1 && strings.Contains(calls[i].message, ErrEventInterrupted.Error()) {
			failed = &calls[i]
		}
	}
	if failed == nil {
		t.Fatalf("expected a failed terminal event containing %q, got: %+v", ErrEventInterrupted, calls)
	}
	if !strings.Contains(failed.message, eventID) {
		t.Errorf("interrupted terminal event must carry the eventID %q for retries, got: %q", eventID, failed.message)
	}
}

func TestCreateResources_SkipsUnstartedAfterCancellation(t *testing.T) {
	const elName = "test-el"
	sink, dynamicClient := getSinkAssets(t, gitCloneEventListenerRes(t, elName), elName, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sink.CreateResources(ctx, namespace, "", shutdownTaskRunResources("a", "b"), "trigger", eventID, sink.Logger)
	if !errors.Is(err, ErrEventInterrupted) {
		t.Fatalf("expected ErrEventInterrupted, got: %v", err)
	}
	if len(dynamicClient.Actions()) != 0 {
		t.Fatalf("no resource creation should start after cancellation, got: %v", dynamicClient.Actions())
	}
}

func TestCreateResources_InFlightCallReturnsBeforeSkippingRest(t *testing.T) {
	const elName = "test-el"
	sink, dynamicClient := getSinkAssets(t, gitCloneEventListenerRes(t, elName), elName, nil)

	createEntered := make(chan struct{}, 2)
	release := make(chan struct{})
	dynamicClient.PrependReactor("create", "*", func(ktesting.Action) (bool, runtime.Object, error) {
		select {
		case createEntered <- struct{}{}:
		default:
		}
		<-release
		// Delegate to the default fake tracker so the create is recorded.
		return false, nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- sink.CreateResources(ctx, namespace, "", shutdownTaskRunResources("a", "b"), "trigger", eventID, sink.Logger)
	}()

	select {
	case <-createEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("resource creation never reached the Kubernetes API call")
	}

	// Cancel while the first create is being submitted: the in-flight call must
	// be allowed to return; the second resource must never be started.
	cancel()
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, ErrEventInterrupted) {
			t.Fatalf("expected ErrEventInterrupted after the in-flight call returned, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CreateResources did not return after the in-flight call completed")
	}

	creates := 0
	for _, action := range dynamicClient.Actions() {
		if action.GetVerb() == "create" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("expected the single in-flight create to complete but no new work to start, got %d creates", creates)
	}
}

func shutdownTaskRunResources(names ...string) []json.RawMessage {
	var out []json.RawMessage
	for _, name := range names {
		out = append(out, json.RawMessage(fmt.Sprintf(
			`{"apiVersion":"tekton.dev/v1","kind":"TaskRun","metadata":{"name":"%s","namespace":"%s"}}`, name, namespace)))
	}
	return out
}

func gitCloneEventListenerRes(t *testing.T, elName string) test.Resources {
	t.Helper()
	return test.Resources{
		EventListeners: []*triggersv1beta1.EventListener{{
			ObjectMeta: metav1.ObjectMeta{Name: elName, Namespace: namespace, UID: types.UID(elUID)},
			Spec: triggersv1beta1.EventListenerSpec{
				Triggers: []triggersv1beta1.EventListenerTrigger{{
					Name: "git-clone-trigger",
					Bindings: []*triggersv1beta1.EventListenerBinding{
						{Name: "url", Value: ptr.String("$(body.repository.url)")},
						{Name: "revision", Value: ptr.String("$(body.head_commit.id)")},
						{Name: "name", Value: ptr.String("git-clone-run")},
						{Name: "app", Value: ptr.String("$(body.foo)")},
						{Name: "type", Value: ptr.String("$(header.Content-Type)")},
					},
					Template: &triggersv1beta1.EventListenerTemplate{Spec: makeGitCloneTTSpec(t, "git-clone-run")},
				}},
			},
		}},
	}
}

type recordedEvent struct {
	eventtype string
	reason    string
	message   string
}

// recordingEventRecorder is a minimal record.EventRecorder that captures the
// terminal Kubernetes Events emitted by the sink.
type recordingEventRecorder struct {
	mu     sync.Mutex
	events []recordedEvent
}

func newRecordingEventRecorder() *recordingEventRecorder {
	return &recordingEventRecorder{}
}

func (r *recordingEventRecorder) Event(_ runtime.Object, eventtype, reason, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, recordedEvent{eventtype: eventtype, reason: reason, message: message})
}

func (r *recordingEventRecorder) Eventf(obj runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Event(obj, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *recordingEventRecorder) AnnotatedEventf(obj runtime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Event(obj, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *recordingEventRecorder) snapshot() []recordedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedEvent, len(r.events))
	copy(out, r.events)
	return out
}
