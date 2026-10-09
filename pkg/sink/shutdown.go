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
	"context"
	"errors"
	"sync"
)

var (
	// ErrSinkShuttingDown is returned when the sink has started shutting down
	// and can no longer accept a new event. The HTTP handler replies with a
	// non-202 status so that the sender knows the event was never accepted and
	// can retry it against another EventListener pod (or after restart).
	ErrSinkShuttingDown = errors.New("event listener is shutting down and is no longer accepting events")

	// ErrEventInterrupted marks a trigger branch whose processing was stopped
	// by graceful shutdown before all of its resources were created. A terminal
	// failed event is emitted for the eventID so that senders have a
	// deterministic, observable "incomplete" result and can safely retry the
	// same event. Kubernetes create calls that were already in flight when the
	// grace period expired are allowed to return; this error is only produced
	// before starting a new unit of work.
	ErrEventInterrupted = errors.New("event listener shut down before trigger processing completed")
)

// InflightTracker keeps track of every event for which the sink has returned
// (or is about to return) 202 Accepted, and of every goroutine doing work on
// behalf of that event:
//
//   - the outer goroutine of each ungrouped Trigger,
//   - the outer goroutine of each TriggerGroup,
//   - the per-trigger branches spawned inside a TriggerGroup.
//
// During graceful shutdown the tracker first flips into the "closing" state so
// that new events are rejected, then waits for all accepted events to converge.
// If the grace period expires, it cancels the per-event context so that work
// which has not started yet is skipped, while work in the Kubernetes commit
// phase keeps running on a context detached from cancellation.
type InflightTracker struct {
	mu      sync.Mutex
	closing bool
	events  map[string]*acceptedEvent

	// globalWG, when set, mirrors the branch accounting on the legacy
	// Sink.WGProcessTriggers WaitGroup which tests use to wait for processing.
	globalWG *sync.WaitGroup

	// notifyWG tracks fire-and-forget CloudEvents emitted at HTTP-handler level
	// (started/done/failed notifications) so that shutdown does not exit while
	// those deliveries are still in progress.
	notifyWG sync.WaitGroup
}

// acceptedEvent represents one accepted eventID and the work associated with it.
type acceptedEvent struct {
	tracker *InflightTracker
	id      string

	// ctx is cancelled when the shutdown grace period expires. It must not be
	// derived from the HTTP request context because processing continues after
	// the 202 response is written.
	ctx    context.Context //nolint:containedctx
	cancel context.CancelFunc

	wg      sync.WaitGroup
	mu      sync.Mutex
	pending int
}

// NewInflightTracker creates a tracker. globalWG, when non-nil, mirrors the
// branch accounting on the legacy Sink.WGProcessTriggers WaitGroup.
func NewInflightTracker(globalWG *sync.WaitGroup) *InflightTracker {
	return &InflightTracker{
		events:   map[string]*acceptedEvent{},
		globalWG: globalWG,
	}
}

// accept reserves the given number of outer branches (ungrouped triggers plus
// trigger groups) for an eventID. It returns ErrSinkShuttingDown once shutdown
// has started, so the caller must not return 202 for that event.
//
// All outer branch counts are reserved atomically here (rather than added from
// the spawned goroutines) so that a concurrent shutdown cannot observe a
// zero counter for an event that is about to start.
func (t *InflightTracker) accept(id string, branches int) (*acceptedEvent, error) {
	ctx, cancel := context.WithCancel(context.Background())

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing {
		cancel()
		return nil, ErrSinkShuttingDown
	}

	e := &acceptedEvent{
		tracker: t,
		id:      id,
		ctx:     ctx,
		cancel:  cancel,
	}

	if branches == 0 {
		// No Trigger and no TriggerGroup matched: nothing will ever touch the
		// wait group, so do not register the event (and free its context).
		cancel()
		return e, nil
	}

	e.pending = branches
	e.wg.Add(branches)
	if t.globalWG != nil {
		t.globalWG.Add(branches)
	}
	t.events[id] = e
	return e, nil
}

// BeginShutdown stops the tracker from accepting new events. Already accepted
// events keep running until they finish or are cancelled after the grace
// period.
func (t *inflightTracker) BeginShutdown() {
	t.mu.Lock()
	t.closing = true
	t.mu.Unlock()
}

// add reserves branches spawned from within an outer branch (the triggers
// selected by a TriggerGroup). The outer branch always holds its own count
// until these adds are done, so the counter can never reach zero in between.
func (e *acceptedEvent) add(n int) {
	if n <= 0 {
		return
	}
	e.mu.Lock()
	e.pending += n
	e.mu.Unlock()
	e.wg.Add(n)
	if e.tracker.globalWG != nil {
		e.tracker.globalWG.Add(n)
	}
}

// done marks one branch (outer or group-internal) as finished.
func (e *acceptedEvent) done() {
	e.mu.Lock()
	if e.pending <= 0 {
		e.mu.Unlock()
		panic("inflightTracker: negative branch counter for event " + e.id)
	}
	e.pending--
	last := e.pending == 0
	e.mu.Unlock()

	if e.tracker.globalWG != nil {
		e.tracker.globalWG.Done()
	}
	e.wg.Done()
	if last {
		e.tracker.remove(e)
	}
}

func (t *inflightTracker) remove(e *acceptedEvent) {
	t.mu.Lock()
	if current, ok := t.events[e.id]; ok && current == e {
		delete(t.events, e.id)
	}
	t.mu.Unlock()
	// Release the per-event context once every branch has converged.
	e.cancel()
}

func (t *inflightTracker) snapshot() []*acceptedEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*acceptedEvent, 0, len(t.events))
	for _, e := range t.events {
		out = append(out, e)
	}
	return out
}

// inflightCount returns the number of accepted events that still have branches
// running. It is primarily intended for tests and diagnostics.
func (t *inflightTracker) inflightCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.events)
}

// InflightEventIDs returns the eventIDs of events still being processed.
func (t *inflightTracker) InflightEventIDs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0, len(t.events))
	for id := range t.events {
		ids = append(ids, id)
	}
	return ids
}

// WaitAll blocks until every currently accepted event has converged, or until
// ctx expires. It returns true when all events finished. New events cannot be
// accepted concurrently because callers must invoke BeginShutdown first.
func (t *inflightTracker) WaitAll(ctx context.Context) bool {
	events := t.snapshot()
	done := make(chan struct{})
	go func() {
		for _, e := range events {
			e.wg.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// CancelUnfinished cancels the context of events that are still in flight.
// Branches use this context to skip work that has not started yet; Kubernetes
// calls already being submitted deliberately use a detached context and are
// not interrupted.
func (t *inflightTracker) CancelUnfinished() {
	for _, e := range t.snapshot() {
		e.cancel()
	}
}

// notifyGo runs fn while accounting for it so that waitNotifications can wait
// for handler-level CloudEvent deliveries during shutdown.
func (t *inflightTracker) notifyGo(fn func()) {
	t.notifyWG.Add(1)
	go func() {
		defer t.notifyWG.Done()
		fn()
	}()
}

// WaitNotifications waits for tracked handler-level notifications to be sent,
// or until ctx expires.
func (t *inflightTracker) WaitNotifications(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		t.notifyWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
