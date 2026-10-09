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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/tektoncd/triggers/pkg/apis/triggers"
	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"k8s.io/client-go/dynamic"

	"go.uber.org/zap"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryclient "k8s.io/client-go/discovery"
)

// ErrResourceConflict is returned when an object with the same identity as a
// resource about to be created already exists but cannot be attributed to the
// same creation plan, or exists with a different configuration. The caller must
// not retry the creation and the existing object is never overwritten.
var ErrResourceConflict = errors.New("resource conflict")

// findAPIResource returns the APIResource definition using the discovery client c.
func findAPIResource(apiVersion, kind string, c discoveryclient.ServerResourcesInterface) (*metav1.APIResource, error) {
	resourceList, err := c.ServerResourcesForGroupVersion(apiVersion)
	if err != nil {
		return nil, fmt.Errorf("error getting kubernetes server resources for apiVersion %s: %w", apiVersion, err)
	}
	for i := range resourceList.APIResources {
		r := &resourceList.APIResources[i]
		if r.Kind != kind {
			continue
		}

		// Resolve GroupVersion from parent list to have consistent resource identifiers.
		if r.Version == "" || r.Group == "" {
			gv, err := schema.ParseGroupVersion(resourceList.GroupVersion)
			if err != nil {
				return nil, fmt.Errorf("error parsing GroupVersion: %w", err)
			}
			r.Group = gv.Group
			r.Version = gv.Version
		}
		return r, nil
	}
	return nil, fmt.Errorf("error could not find resource with apiVersion %s and kind %s", apiVersion, kind)
}

// Create uses the kubeClient to create the resource defined in the
// TriggerResourceTemplate and returns any errors with this process.
//
// Create is idempotent for retried deliveries of the same event: resourceIndex
// is the position of the resource template within the TriggerTemplate and,
// together with eventID, it identifies the resource within one creation plan.
// Before creating anything, an object already stored in the cluster with the
// same identity is looked up and verified. If it was created for the same
// event/trigger and its content matches, creation is skipped; otherwise an
// ErrResourceConflict error is returned and the existing object is never
// overwritten. Templates using generateName get a deterministic, stable name
// so retries resolve to the same object instead of generating a second one.
func Create(logger *zap.SugaredLogger, rt json.RawMessage, triggerName, eventID, elName, elNamespace string, resourceIndex int, c discoveryclient.ServerResourcesInterface, dc dynamic.Interface) error {
	// Assume the TriggerResourceTemplate is valid (it has an apiVersion and Kind)
	data := new(unstructured.Unstructured)
	if err := data.UnmarshalJSON(rt); err != nil {
		return fmt.Errorf("couldn't unmarshal json from the TriggerTemplate: %w", err)
	}

	data, err := addLabels(data, map[string]string{
		triggers.EventListenerLabelKey: elName,
		triggers.EventIDLabelKey:       eventID,
		triggers.TriggerLabelKey:       triggerName,
	})
	if err != nil {
		return err
	}

	namespace := data.GetNamespace()
	// Default the resource creation to the EventListenerNamespace if not found in the resource template
	if namespace == "" {
		namespace = elNamespace
	}

	// Resolve resource kind to the underlying API Resource type.
	apiResource, err := findAPIResource(data.GetAPIVersion(), data.GetKind(), c)
	if err != nil {
		return fmt.Errorf("couldn't find API resource for json: %w", err)
	}

	gvr := schema.GroupVersionResource{
		Group:    apiResource.Group,
		Version:  apiResource.Version,
		Resource: apiResource.Name,
	}

	var client dynamic.ResourceInterface
	if apiResource.Namespaced {
		client = dc.Resource(gvr).Namespace(namespace)
	} else {
		namespace = ""
		client = dc.Resource(gvr)
	}

	name := data.GetName()
	if name == "" && data.GetGenerateName() != "" {
		// Resolve generateName client-side using a name that is stable for the
		// lifetime of this event/trigger plan. A redelivery looks up the same
		// name again and cannot create a second generated object.
		name = generatedResourceName(data.GetGenerateName(), eventID, triggerName, resourceIndex)
		data.SetName(name)
		data.SetGenerateName("")
	}

	logger.Infof("Generating resource: kind: %s, name: %s", apiResource, name)
	logger.Infof("For event ID %q creating resource %v", eventID, gvr)

	if name != "" {
		existing, err := client.Get(context.Background(), name, metav1.GetOptions{})
		switch {
		case err == nil:
			return verifyExistingResource(data, existing, gvr, namespace, eventID, triggerName, logger)
		case kerrors.IsNotFound(err):
			// Not created yet; submit it below.
		case kerrors.IsUnauthorized(err) || kerrors.IsForbidden(err):
			// The trigger service account may only be granted the create verb
			// (the historical requirement). Without read access the plan
			// cannot be reconciled here, so fall back to submitting the create
			// directly instead of failing deliveries that used to succeed.
			logger.Warnf("For event ID %q not allowed to look up resource %v %q before creating it; creating without deduplication: %v", eventID, gvr, name, err)
		default:
			return fmt.Errorf("couldn't look up existing resource with group version kind %q before creating it: %w", gvr, err)
		}
	}

	if _, err := client.Create(context.Background(), data, metav1.CreateOptions{}); err != nil {
		if kerrors.IsUnauthorized(err) || kerrors.IsForbidden(err) {
			return err
		}
		if kerrors.IsAlreadyExists(err) && name != "" {
			// A concurrent delivery may have won the race. Re-fetch and verify
			// the existing object instead of failing or resubmitting.
			existing, getErr := client.Get(context.Background(), name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("couldn't create resource with group version kind %q: %w (error looking up the existing object: %w)", gvr, err, getErr)
			}
			return verifyExistingResource(data, existing, gvr, namespace, eventID, triggerName, logger)
		}
		return fmt.Errorf("couldn't create resource with group version kind %q: %w", gvr, err)
	}
	return nil
}

// verifyExistingResource checks an object that already exists at the identity of
// a resource the sink is about to create. It returns nil when the object belongs
// to the same event/trigger plan and still contains the submitted configuration
// (server-side additions such as defaults and metadata are allowed). Otherwise
// it returns an ErrResourceConflict error so the object is never overwritten.
func verifyExistingResource(wanted, existing *unstructured.Unstructured, gvr schema.GroupVersionResource, namespace, eventID, triggerName string, logger *zap.SugaredLogger) error {
	labels := existing.GetLabels()
	if labels[triggers.GroupName+triggers.EventIDLabelKey] != eventID ||
		labels[triggers.GroupName+triggers.TriggerLabelKey] != triggerName {
		return fmt.Errorf("%w: resource %s/%s already exists but was not created by event %q for trigger %q; refusing to overwrite it",
			ErrResourceConflict, gvr.Resource, nameAndNamespace(existing.GetName(), namespace), eventID, triggerName)
	}
	if !containsSubmittedContent(wanted, existing) {
		return fmt.Errorf("%w: resource %s/%s already exists for event %q but its configuration differs from the rendered TriggerTemplate; refusing to overwrite it",
			ErrResourceConflict, gvr.Resource, nameAndNamespace(existing.GetName(), namespace), eventID)
	}
	logger.Infof("For event ID %q resource %v %s/%s already exists and is up to date; skipping creation",
		eventID, gvr, gvr.Resource, nameAndNamespace(existing.GetName(), namespace))
	return nil
}

func nameAndNamespace(name, namespace string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

// generatedResourceName derives a DNS-1123 compatible name for a generateName
// template. The name is a deterministic function of the event ID, the trigger
// and the resource position in the template, so every retry of the same event
// resolves to the same object while distinct resources (and distinct triggers
// processing one event) still get distinct names.
func generatedResourceName(generateName, eventID, triggerName string, resourceIndex int) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{eventID, triggerName, strconv.Itoa(resourceIndex)}, "/")))
	suffix := hex.EncodeToString(sum[:])[:9]

	prefix := strings.TrimSuffix(generateName, "-")
	name := prefix + "-" + suffix
	// Keep within the most restrictive DNS-1123 label length limit regardless of
	// the resource kind.
	const maxNameLength = 63
	if len(name) > maxNameLength {
		prefixMax := maxNameLength - len(suffix) - 1
		name = strings.TrimRight(prefix[:prefixMax], "-") + "-" + suffix
	}
	return name
}

// containsSubmittedContent reports whether every field the sink is about to
// submit is already present in the existing object. Extra fields set by the
// API server (defaults, metadata, status, ...) are ignored.
func containsSubmittedContent(wanted, existing *unstructured.Unstructured) bool {
	wantedObject := wanted.DeepCopy().Object
	// Status is managed by the server and is usually stripped from create
	// requests.
	delete(wantedObject, "status")
	return containsSubset(wantedObject, existing.Object)
}

// containsSubset implements containsSubmittedContent over generic decoded JSON
// values: maps must contain all wanted keys with matching values, lists must
// match element by element in order, and scalars must be equal.
func containsSubset(wanted, existing interface{}) bool {
	switch w := wanted.(type) {
	case map[string]interface{}:
		e, ok := existing.(map[string]interface{})
		if !ok {
			return false
		}
		for k, wantedValue := range w {
			// null fields in a template (e.g. creationTimestamp: null) do not
			// constrain the object stored by the server.
			if wantedValue == nil {
				continue
			}
			existingValue, ok := e[k]
			if !ok || !containsSubset(wantedValue, existingValue) {
				return false
			}
		}
		return true
	case []interface{}:
		e, ok := existing.([]interface{})
		if !ok || len(e) != len(w) {
			return false
		}
		for i := range w {
			if !containsSubset(w[i], e[i]) {
				return false
			}
		}
		return true
	default:
		return scalarEqual(w, existing)
	}
}

// scalarEqual compares JSON scalars tolerating int64/float64 differences
// introduced by the different decoders used for templates and API responses.
func scalarEqual(wanted, existing interface{}) bool {
	wantedNumber, wantedIsNumber := asFloat64(wanted)
	existingNumber, existingIsNumber := asFloat64(existing)
	if wantedIsNumber || existingIsNumber {
		return wantedIsNumber == existingIsNumber && wantedNumber == existingNumber
	}
	return wanted == existing
}

func asFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	default:
		return 0, false
	}
}

// addLabels adds autogenerated Tekton labels to created resources.
func addLabels(us *unstructured.Unstructured, labelsToAdd map[string]string) (*unstructured.Unstructured, error) {
	labels, _, err := unstructured.NestedStringMap(us.Object, "metadata", "labels")
	if err != nil {
		return nil, err
	}

	if labels == nil {
		labels = make(map[string]string)
	}
	for k, v := range labelsToAdd {
		l := fmt.Sprintf("%s/%s", triggers.GroupName, strings.TrimLeft(k, "/"))
		labels[l] = v
	}

	us.SetLabels(labels)
	return us, nil
}
