// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// slotRecheckInterval is how often a blocked acquirer re-reads the (live,
// /settings-overridable) concurrency limit. Releases wake waiters directly;
// this slow tick exists only so a limit that is raised while every slot is
// busy still takes effect without waiting for the next release.
const slotRecheckInterval = 250 * time.Millisecond

// The low-priority lanes of slotSem, in the order they are served.
const (
	laneAhead   = iota // transcode-ahead (ahead.go): someone is watching
	lanePrepare        // preparation (prepare.go): nobody is waiting yet
	numLanes
)

// slotSem is a FIFO counting semaphore whose limit is supplied on every call,
// so it can be resized live (the limit comes from /settings) without
// rebuilding the primitive. Waiters block on a per-waiter channel closed by
// release — there is no polling on the hot path. The zero value is ready to
// use.
//
// It also has low-priority lanes for background work (transcode-ahead, see
// ahead.go, and HLS preparation, see prepare.go): acquireLow gets a slot only
// while no regular waiter is queued, and at most the lane's limit (the value
// passed to the lane's latest acquireLow) go to a lane at once. Lanes are
// served in order, so a queued transcode-ahead waiter goes before a queued
// preparation one. A regular acquire never waits behind a low-priority one,
// but it does not preempt one that is already running. With a lane limit of
// 0 (the zero value) the lane never gets a slot, and with every lane unused
// the semaphore behaves exactly as before the lanes existed.
type slotSem struct {
	mu      sync.Mutex
	held    int       // every slot held, the lanes' included
	waiters list.List // of *slotWaiter, oldest first
	lanes   [numLanes]slotLane
}

// slotLane is one low-priority lane of a slotSem.
type slotLane struct {
	held    int       // slots held through acquireLow on this lane
	limit   int       // most slots the lane may hold at once (its last acquireLow's)
	waiters list.List // of *slotWaiter, oldest first
}

type slotWaiter struct {
	ready   chan struct{} // closed when the slot has been handed over
	granted bool          // guarded by slotSem.mu
	lane    int           // the low-priority lane it is queued on; -1 for a regular waiter
}

// grantLocked hands free slots (per limit) to waiters in arrival order,
// regular waiters first; a lane's only once none is queued, the lanes in
// order.
func (s *slotSem) grantLocked(limit int) { s.grantQueuedLocked(limit, true) }

// grantQueuedLocked is grantLocked; withLow false leaves the lanes alone.
func (s *slotSem) grantQueuedLocked(limit int, withLow bool) {
	for s.held < limit {
		if front := s.waiters.Front(); front != nil {
			w := s.waiters.Remove(front).(*slotWaiter)
			w.granted = true
			s.held++
			close(w.ready)
			continue
		}
		if !withLow || !s.grantLaneLocked() {
			return
		}
	}
}

// grantLaneLocked hands one slot to the first queued waiter of the first
// lane that is under its limit, and reports whether it did.
func (s *slotSem) grantLaneLocked() bool {
	for i := range s.lanes {
		ln := &s.lanes[i]
		front := ln.waiters.Front()
		if front == nil || ln.held >= ln.limit {
			continue
		}
		w := ln.waiters.Remove(front).(*slotWaiter)
		w.granted = true
		s.held++
		ln.held++
		close(w.ready)
		return true
	}
	return false
}

// acquire blocks until a slot is available under limit (re-evaluated via
// limitFn while blocked) or ctx is done. limitFn is invoked outside the lock.
func (s *slotSem) acquire(ctx context.Context, limitFn func() int) error {
	limit := limitFn()
	s.mu.Lock()
	// A raised limit may free slots for queued regular waiters first; the
	// low-priority lanes must not take one ahead of this caller.
	s.grantQueuedLocked(limit, false)
	if s.held < limit && s.waiters.Len() == 0 {
		s.held++
		s.mu.Unlock()
		return nil
	}
	w := &slotWaiter{ready: make(chan struct{}), lane: -1}
	el := s.waiters.PushBack(w)
	s.mu.Unlock()
	return s.wait(ctx, w, el, limitFn)
}

// acquireLow is acquire for low-priority lane lane: it blocks until a slot
// is free, no regular waiter (nor an earlier lane's waiter that could take
// it) is queued and the lane holds fewer than laneLimit slots, or until ctx
// is done. Release the slot with releaseLow.
func (s *slotSem) acquireLow(ctx context.Context, limitFn func() int, lane, laneLimit int) error {
	limit := limitFn()
	s.mu.Lock()
	ln := &s.lanes[lane]
	ln.limit = laneLimit
	// Serve whoever is queued first; an earlier lane's waiter still queued
	// afterwards is at its own lane's limit, so it can't use a free slot.
	s.grantLocked(limit)
	if s.held < limit && ln.held < ln.limit && s.waiters.Len() == 0 && ln.waiters.Len() == 0 {
		s.held++
		ln.held++
		s.mu.Unlock()
		return nil
	}
	w := &slotWaiter{ready: make(chan struct{}), lane: lane}
	el := ln.waiters.PushBack(w)
	s.mu.Unlock()
	return s.wait(ctx, w, el, limitFn)
}

// wait blocks a queued waiter w (list element el) until it is granted a slot
// or ctx is done.
func (s *slotSem) wait(ctx context.Context, w *slotWaiter, el *list.Element, limitFn func() int) error {
	tick := time.NewTicker(slotRecheckInterval)
	defer tick.Stop()
	for {
		select {
		case <-w.ready:
			return nil
		case <-ctx.Done():
			// Re-read the live limit (outside the lock): it may have been
			// lowered since this waiter queued, and the hand-back below must
			// not over-grant against a stale value.
			limit := limitFn()
			s.mu.Lock()
			switch {
			case w.granted:
				// Lost the race: a release already handed us a slot. Give
				// it straight back so it is not leaked.
				s.held--
				if w.lane >= 0 {
					s.lanes[w.lane].held--
				}
				s.grantLocked(limit)
			case w.lane >= 0:
				s.lanes[w.lane].waiters.Remove(el)
			default:
				s.waiters.Remove(el)
			}
			s.mu.Unlock()
			return ctx.Err()
		case <-tick.C:
			limit := limitFn()
			s.mu.Lock()
			s.grantLocked(limit)
			s.mu.Unlock()
		}
	}
}

// queuedLocked reports whether any waiter, regular or low-priority, is
// queued.
func (s *slotSem) queuedLocked() bool {
	if s.waiters.Len() > 0 {
		return true
	}
	for i := range s.lanes {
		if s.lanes[i].waiters.Len() > 0 {
			return true
		}
	}
	return false
}

// release returns a slot and wakes the next waiter(s) if the limit allows.
func (s *slotSem) release(limitFn func() int) {
	s.mu.Lock()
	if s.held > 0 {
		s.held--
	}
	queued := s.queuedLocked()
	s.mu.Unlock()
	if !queued {
		return // nobody to wake: skip the (settings-mutex) limit lookup
	}
	limit := limitFn()
	s.mu.Lock()
	s.grantLocked(limit)
	s.mu.Unlock()
}

// releaseLow returns a slot acquired with acquireLow on lane.
func (s *slotSem) releaseLow(limitFn func() int, lane int) {
	s.mu.Lock()
	if ln := &s.lanes[lane]; ln.held > 0 {
		ln.held--
	}
	s.mu.Unlock()
	s.release(limitFn)
}
