/*
Copyright 2019 The Tekton Authors

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

package resources

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/google/go-cmp/cmp"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/triggers/pkg/apis/triggers"
	"github.com/tektoncd/triggers/test"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const (
	resourceLabel = triggers.GroupName + triggers.EventListenerLabelKey
	triggerLabel  = triggers.GroupName + triggers.TriggerLabelKey
	eventIDLabel  = triggers.GroupName + triggers.EventIDLabelKey
	triggerName   = "trigger"
	eventID       = "12345"
)

func Test_FindAPIResource_error(t *testing.T) {
	dc := fakekubeclientset.NewSimpleClientset().Discovery()
	if _, err := findAPIResource("v1", "Pod", dc); err == nil {
		t.Error("findAPIResource() did not return error when expected")
	}
}

func TestFindAPIResource(t *testing.T) {
	// Create fake kubeclient with list of resources
	kubeClient := fakekubeclientset.NewSimpleClientset()
	kubeClient.Resources = []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{{
			Name:       "pods",
			Namespaced: true,
			Kind:       "Pod",
		}, {
			Name:       "namespaces",
			Namespaced: false,
			Kind:       "Namespace",
		}},
	}}
	test.AddTektonResources(kubeClient)
	dc := kubeClient.Discovery()

	tests := []struct {
		apiVersion string
		kind       string
		want       *metav1.APIResource
	}{{
		apiVersion: "v1",
		kind:       "Pod",
		want: &metav1.APIResource{
			Name:       "pods",
			Namespaced: true,
			Version:    "v1",
			Kind:       "Pod",
		},
	}, {
		apiVersion: "v1",
		kind:       "Namespace",
		want: &metav1.APIResource{
			Name:       "namespaces",
			Namespaced: false,
			Version:    "v1",
			Kind:       "Namespace",
		},
	}, {
		apiVersion: "tekton.dev/v1alpha1",
		kind:       "TriggerTemplate",
		want: &metav1.APIResource{
			Group:      "tekton.dev",
			Version:    "v1alpha1",
			Name:       "triggertemplates",
			Namespaced: true,
			Kind:       "TriggerTemplate",
		},
	}, {
		apiVersion: "tekton.dev/v1alpha1",
		kind:       "PipelineRun",
		want: &metav1.APIResource{
			Group:      "tekton.dev",
			Version:    "v1alpha1",
			Name:       "pipelineruns",
			Namespaced: true,
			Kind:       "PipelineRun",
		},
	},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%s", tt.apiVersion, tt.kind), func(t *testing.T) {
			got, err := findAPIResource(tt.apiVersion, tt.kind, dc)
			if err != nil {
				t.Errorf("findAPIResource() returned error: %s", err)
			} else if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("findAPIResource() Diff: -want +got: %s", diff)
			}
		})
	}
}

func TestCreateResource(t *testing.T) {
	elName := "foo-el"
	elNamespace := "foo"

	kubeClient := fakekubeclientset.NewSimpleClientset()
	test.AddTektonResources(kubeClient)

	dynamicClient := fakedynamic.NewSimpleDynamicClient(runtime.NewScheme())

	logger := zaptest.NewLogger(t)

	tests := []struct {
		name string
		json []byte
		want pipelinev1.TaskRun
	}{{
		name: "TaskRun without namespace",
		json: json.RawMessage(`{"kind":"TaskRun","apiVersion":"tekton.dev/v1beta1","metadata":{"name":"my-taskrun","creationTimestamp":null,"labels":{"someLabel":"bar"}},"spec":{"serviceAccountName":"","taskRef":{"name":"my-task"}},"status":{"podName": ""}}`),
		want: pipelinev1.TaskRun{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "tekton.dev/v1beta1",
				Kind:       "TaskRun",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "my-taskrun",
				Labels: map[string]string{
					"someLabel":   "bar", // replaced with the value of foo from bar
					resourceLabel: "foo-el",
					triggerLabel:  triggerName,
					eventIDLabel:  eventID,
				},
			},
			Spec: pipelinev1.TaskRunSpec{
				TaskRef: &pipelinev1.TaskRef{
					Name: "my-task", // non-existent task; just for testing
				},
			},
			Status: pipelinev1.TaskRunStatus{},
		},
	},

		{
			name: "TaskRun with namespace",
			json: json.RawMessage(`{"kind":"TaskRun","apiVersion":"tekton.dev/v1beta1","metadata":{"name":"my-taskrun","namespace":"bar","creationTimestamp":null,"labels":{"someLabel":"bar"}},"spec":{"serviceAccountName":"","taskRef":{"name":"my-task"}},"status":{"podName":""}}`),
			want: pipelinev1.TaskRun{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "tekton.dev/v1beta1",
					Kind:       "TaskRun",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-taskrun",
					Namespace: "bar",
					Labels: map[string]string{
						"someLabel":   "bar", // replaced with the value of foo from bar
						resourceLabel: "foo-el",
						triggerLabel:  triggerName,
						eventIDLabel:  eventID,
					},
				},
				Spec: pipelinev1.TaskRunSpec{
					TaskRef: &pipelinev1.TaskRef{
						Name: "my-task", // non-existent task; just for testing
					},
				},
				Status: pipelinev1.TaskRunStatus{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dynamicClient.ClearActions()
			if err := Create(logger.Sugar(), tt.json, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient); err != nil {
				t.Errorf("createTaskRun() returned error: %s", err)
			}

			gvr := schema.GroupVersionResource{
				Group:    "tekton.dev",
				Version:  "v1beta1",
				Resource: "taskruns",
			}
			namespace := tt.want.Namespace
			if namespace == "" {
				namespace = elNamespace
			}
			// Create now reconciles against the cluster first, so it performs
			// a GET before the CREATE. Only the CREATE action carries the
			// submitted object.
			want := []ktesting.Action{ktesting.NewCreateAction(gvr, namespace, test.ToUnstructured(t, tt.want))}
			if diff := cmp.Diff(want, createActions(t, dynamicClient.Actions())); diff != "" {
				fmt.Println("diff", diff)
				t.Error(diff)
			}
		})
	}
}

// createActions filters recorded fake-client actions down to create actions.
func createActions(t *testing.T, actions []ktesting.Action) []ktesting.Action {
	t.Helper()
	creates := []ktesting.Action{}
	for _, a := range actions {
		if a.GetVerb() == "create" {
			creates = append(creates, a)
		}
	}
	return creates
}

func taskRunResource(t *testing.T, name, namespace, taskName string) *unstructured.Unstructured {
	t.Helper()
	return test.ToUnstructured(t, pipelinev1.TaskRun{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "tekton.dev/v1beta1",
			Kind:       "TaskRun",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"someLabel": "bar",
			},
		},
		Spec: pipelinev1.TaskRunSpec{
			TaskRef: &pipelinev1.TaskRef{
				Name: taskName,
			},
		},
	})
}

func resourceJSON(t *testing.T, u *unstructured.Unstructured) json.RawMessage {
	t.Helper()
	b, err := u.MarshalJSON()
	if err != nil {
		t.Fatalf("failed to marshal unstructured: %v", err)
	}
	return b
}

func resourceTestClients(t *testing.T, seeds ...runtime.Object) (*fakekubeclientset.Clientset, *fakedynamic.FakeDynamicClient) {
	t.Helper()
	kubeClient := fakekubeclientset.NewSimpleClientset()
	test.AddTektonResources(kubeClient)
	dynamicClient := fakedynamic.NewSimpleDynamicClient(runtime.NewScheme(), seeds...)
	return kubeClient, dynamicClient
}

func TestCreateIdempotentRetrySkipsExisting(t *testing.T) {
	elName := "foo-el"
	elNamespace := "foo"

	existing := taskRunResource(t, "my-taskrun", elNamespace, "my-task")
	// Seed the object as the sink would have created it during the first
	// attempt (it carries the tracking labels).
	if _, err := addLabels(existing, map[string]string{
		triggers.EventListenerLabelKey: elName,
		triggers.EventIDLabelKey:       eventID,
		triggers.TriggerLabelKey:       triggerName,
	}); err != nil {
		t.Fatal(err)
	}

	kubeClient, dynamicClient := resourceTestClients(t, existing)
	logger := zaptest.NewLogger(t)

	rt := resourceJSON(t, taskRunResource(t, "my-taskrun", elNamespace, "my-task"))

	// First "retry": the object already exists and matches.
	if err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient); err != nil {
		t.Fatalf("Create() returned error: %s", err)
	}
	// A second delivery of the same event must behave identically.
	if err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient); err != nil {
		t.Fatalf("Create() returned error on retry: %s", err)
	}

	if creates := createActions(t, dynamicClient.Actions()); len(creates) != 0 {
		t.Errorf("expected no create actions for an already confirmed resource, got %d: %v", len(creates), creates)
	}
	for _, a := range dynamicClient.Actions() {
		if a.GetVerb() == "update" || a.GetVerb() == "patch" {
			t.Errorf("existing object must never be overwritten, got %s action", a.GetVerb())
		}
	}
}

func TestCreateConflictOnDifferentContent(t *testing.T) {
	elName := "foo-el"
	elNamespace := "foo"

	existing := taskRunResource(t, "my-taskrun", elNamespace, "previously-rendered-task")
	if _, err := addLabels(existing, map[string]string{
		triggers.EventListenerLabelKey: elName,
		triggers.EventIDLabelKey:       eventID,
		triggers.TriggerLabelKey:       triggerName,
	}); err != nil {
		t.Fatal(err)
	}

	kubeClient, dynamicClient := resourceTestClients(t, existing)
	logger := zaptest.NewLogger(t)

	rt := resourceJSON(t, taskRunResource(t, "my-taskrun", elNamespace, "a-different-task"))

	err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient)
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("expected ErrResourceConflict, got: %v", err)
	}
	if creates := createActions(t, dynamicClient.Actions()); len(creates) != 0 {
		t.Errorf("expected no create actions on conflict, got %d", len(creates))
	}
}

func TestCreateConflictOnForeignObject(t *testing.T) {
	elName := "foo-el"
	elNamespace := "foo"

	// An object with the same name that was not created for this event.
	existing := taskRunResource(t, "my-taskrun", elNamespace, "my-task")
	kubeClient, dynamicClient := resourceTestClients(t, existing)
	logger := zaptest.NewLogger(t)

	rt := resourceJSON(t, taskRunResource(t, "my-taskrun", elNamespace, "my-task"))

	err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient)
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("expected ErrResourceConflict for a pre-existing foreign object, got: %v", err)
	}
	if creates := createActions(t, dynamicClient.Actions()); len(creates) != 0 {
		t.Errorf("expected no create actions on conflict, got %d", len(creates))
	}
}

func TestCreateGenerateNameIsStableAcrossRetries(t *testing.T) {
	elName := "foo-el"
	elNamespace := "foo"

	kubeClient, dynamicClient := resourceTestClients(t)
	logger := zaptest.NewLogger(t)

	rt := json.RawMessage(`{"kind":"TaskRun","apiVersion":"tekton.dev/v1beta1","metadata":{"generateName":"my-run-","namespace":"` + elNamespace + `"},"spec":{"taskRef":{"name":"my-task"}}}`)

	if err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient); err != nil {
		t.Fatalf("first Create() returned error: %s", err)
	}
	firstCreates := createActions(t, dynamicClient.Actions())
	if len(firstCreates) != 1 {
		t.Fatalf("expected exactly 1 create action, got %d", len(firstCreates))
	}
	created := firstCreates[0].(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
	firstName := created.GetName()
	if firstName == "" || !strings.HasPrefix(firstName, "my-run-") {
		t.Errorf("expected deterministic name with prefix my-run-, got %q", firstName)
	}
	if got := created.GetGenerateName(); got != "" {
		t.Errorf("expected generateName to be resolved into a name, but it was kept as %q", got)
	}
	if got := created.GetLabels()[triggers.GroupName+triggers.EventIDLabelKey]; got != eventID {
		t.Errorf("expected event ID label %q, got %q", eventID, got)
	}

	// Redelivery of the same event: the generated object is found, no second
	// create and the name is identical.
	if err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient); err != nil {
		t.Fatalf("retried Create() returned error: %s", err)
	}
	allCreates := createActions(t, dynamicClient.Actions())
	if len(allCreates) != 1 {
		t.Errorf("expected exactly 1 create action after retry, got %d", len(allCreates))
	}
	if allCreates[0].(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).GetName() != firstName {
		t.Errorf("generated name changed across retries")
	}
}

func TestContainsSubmittedContent(t *testing.T) {
	tests := []struct {
		name     string
		wanted   map[string]interface{}
		existing map[string]interface{}
		want     bool
	}{{
		name: "server defaults and extra metadata are allowed",
		wanted: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm", "labels": map[string]interface{}{"a": "b"}},
			"data":       map[string]interface{}{"k": "v"},
		},
		existing: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name": "cm", "namespace": "foo",
				"labels":            map[string]interface{}{"a": "b", "c": "d"},
				"resourceVersion":   "1",
				"uid":               "abc",
				"creationTimestamp": "2024-01-01T00:00:00Z",
			},
			"data":   map[string]interface{}{"k": "v"},
			"status": map[string]interface{}{},
		},
		want: true,
	}, {
		name: "submitted value changed is a mismatch",
		wanted: map[string]interface{}{
			"spec": map[string]interface{}{"x": "1"},
		},
		existing: map[string]interface{}{
			"spec": map[string]interface{}{"x": "2"},
		},
		want: false,
	}, {
		name: "owner references are kept, server defaulted fields are ignored",
		wanted: map[string]interface{}{
			"metadata": map[string]interface{}{"ownerReferences": []interface{}{
				map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "name": "owner", "uid": "u-1"},
			}},
		},
		existing: map[string]interface{}{
			"metadata": map[string]interface{}{"ownerReferences": []interface{}{
				map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "name": "owner", "uid": "u-1", "blockOwnerDeletion": true},
			}},
		},
		want: true,
	}, {
		name: "removed owner reference is a mismatch",
		wanted: map[string]interface{}{
			"metadata": map[string]interface{}{"ownerReferences": []interface{}{
				map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "name": "owner", "uid": "u-1"},
			}},
		},
		existing: map[string]interface{}{
			"metadata": map[string]interface{}{},
		},
		want: false,
	}, {
		name: "null values in the template do not constrain the object",
		wanted: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "cm", "creationTimestamp": nil},
		},
		existing: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "cm", "creationTimestamp": "2024-01-01T00:00:00Z"},
		},
		want: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsSubmittedContent(
				&unstructured.Unstructured{Object: tt.wanted},
				&unstructured.Unstructured{Object: tt.existing})
			if got != tt.want {
				t.Errorf("containsSubmittedContent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGeneratedResourceName(t *testing.T) {
	first := generatedResourceName("run-", "event-1", "trig", 0)
	secondSlot := generatedResourceName("run-", "event-1", "trig", 1)
	otherTrigger := generatedResourceName("run-", "event-1", "other-trigger", 0)
	retry := generatedResourceName("run-", "event-1", "trig", 0)

	if first == secondSlot {
		t.Errorf("resources at different template positions need distinct names, both got %q", first)
	}
	if first == otherTrigger {
		t.Errorf("different triggers processing one event need distinct names, both got %q", first)
	}
	if first != retry {
		t.Errorf("name must be stable across retries: %q != %q", first, retry)
	}
	if !strings.HasPrefix(first, "run-") {
		t.Errorf("generated name %q should keep the generateName prefix", first)
	}
	if len(first) > 63 {
		t.Errorf("generated name %q exceeds DNS-1123 label length", first)
	}

	longName := generatedResourceName(strings.Repeat("a", 60)+"-", "event-1", "trig", 0)
	if len(longName) > 63 {
		t.Errorf("long generateName prefix must be truncated to 63 chars, got %q (%d)", longName, len(longName))
	}
	if !strings.HasPrefix(longName, strings.Repeat("a", 53)) {
		t.Errorf("truncated name %q should keep as much of the prefix as possible", longName)
	}
}

func TestCreateFallsBackWhenLookupForbidden(t *testing.T) {
	elName := "foo-el"
	elNamespace := "foo"

	kubeClient, dynamicClient := resourceTestClients(t)
	// A trigger service account that is allowed to create but not to get.
	dynamicClient.PrependReactor("get", "taskruns", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, kerrors.NewForbidden(schema.GroupResource{Group: "tekton.dev", Resource: "taskruns"}, "my-taskrun", errors.New("denied"))
	})
	logger := zaptest.NewLogger(t)

	rt := resourceJSON(t, taskRunResource(t, "my-taskrun", elNamespace, "my-task"))
	if err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient); err != nil {
		t.Fatalf("Create() should fall back to a direct create when lookup is forbidden, got: %s", err)
	}
	if creates := createActions(t, dynamicClient.Actions()); len(creates) != 1 {
		t.Errorf("expected the resource to be created, got %d create actions", len(creates))
	}
}

func TestCreateDifferentGVKAndNamespace(t *testing.T) {
	elName := "foo-el"
	elNamespace := "foo"

	kubeClient := fakekubeclientset.NewSimpleClientset()
	kubeClient.Resources = []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{{
			Name:       "configmaps",
			Namespaced: true,
			Kind:       "ConfigMap",
		}},
	}}
	dynamicClient := fakedynamic.NewSimpleDynamicClient(runtime.NewScheme())
	logger := zaptest.NewLogger(t)

	// A ConfigMap in another namespace than the EventListener.
	rt := json.RawMessage(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"my-cm","namespace":"other-ns","creationTimestamp":null},"data":{"key":"value"}}`)
	if err := Create(logger.Sugar(), rt, triggerName, eventID, elName, elNamespace, 0, kubeClient.Discovery(), dynamicClient); err != nil {
		t.Fatalf("Create() returned error: %s", err)
	}

	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	creates := createActions(t, dynamicClient.Actions())
	want := []ktesting.Action{ktesting.NewCreateAction(gvr, "other-ns",
		test.ToUnstructured(t, corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cm",
				Namespace: "other-ns",
				Labels: map[string]string{
					triggers.GroupName + triggers.EventListenerLabelKey: elName,
					triggers.GroupName + triggers.EventIDLabelKey:       eventID,
					triggers.GroupName + triggers.TriggerLabelKey:       triggerName,
				},
			},
			Data: map[string]string{"key": "value"},
		}))}
	if diff := cmp.Diff(want, creates); diff != "" {
		t.Errorf("create action mismatch (-want +got): %s", diff)
	}
}

func Test_AddLabels(t *testing.T) {
	tests := []struct {
		name        string
		us          *unstructured.Unstructured
		labelsToAdd map[string]string
		want        *unstructured.Unstructured
	}{
		{
			name: "add to empty labels",
			us: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{},
				}},
			labelsToAdd: map[string]string{"foo": "bar"},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"triggers.tekton.dev/foo": "bar",
						},
					},
				},
			},
		},
		{
			name: "overwrite label",
			us: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"triggers.tekton.dev/foo": "bar",
						},
					},
				}},
			labelsToAdd: map[string]string{"foo": "foo"},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"triggers.tekton.dev/foo": "foo",
						},
					},
				},
			},
		},
		{
			name: "add and overwrite labels",
			us: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							// should be overwritten
							"triggers.tekton.dev/foo":   "bar",
							"triggers.tekton.dev/hello": "world",
							// should be preserved
							"triggers.tekton.dev/z": "0",
							"best-palindrome":       "tacocat",
						},
					},
				}},
			labelsToAdd: map[string]string{
				"foo":   "foo",
				"hello": "there",
				"a":     "a",
				"b":     "b",
			},
			want: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"triggers.tekton.dev/foo":   "foo",
							"triggers.tekton.dev/hello": "there",
							"triggers.tekton.dev/z":     "0",
							"best-palindrome":           "tacocat",
							"triggers.tekton.dev/a":     "a",
							"triggers.tekton.dev/b":     "b",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := addLabels(tt.us, tt.labelsToAdd)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("addLabels(): -want +got: %s", diff)
			}
		})
	}

	t.Run("non-string label", func(t *testing.T) {
		in := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						"foo": 0,
					},
				},
			},
		}
		if got, err := addLabels(in, map[string]string{"a": "b"}); err == nil {
			t.Errorf("expected error, got: %v", got)
		}
	})
}
