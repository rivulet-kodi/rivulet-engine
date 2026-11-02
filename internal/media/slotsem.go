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

// slotSem is a FIFO counting semaphore whose limit is supplied on every call,
// so it can be resized live (the limit comes from /settings) without
// rebuilding the primitive. Waiters block on a per-waiter channel closed by
// release — there is no polling on the hot path. The zero value is ready to
// use.
//
// It also has a low-priority lane for background work (HLS preparation, see
// prepare.go): acquireLow gets a slot only while no regular waiter is queued,
// and at most lowLimit slots (the value passed to the latest acquireLow) go
// to the lane at once. A regular acquire never
// waits behind a low-priority one, but it does not preempt one that is
// already running. With lowLimit 0 (the zero value) the lane never gets a
// slot and the semaphore behaves exactly as before the lane existed.
type slotSem struct {
	mu       sync.Mutex
	held     int
	lowHeld  int       // slots held through acquireLow; included in held
	lowLimit int       // most slots the low-priority lane may hold at once (last acquireLow's)
	waiters  list.List // of *slotWaiter, oldest first
	low      list.List // low-priority *slotWaiter, oldest first
}

type slotWaiter struct {
	ready   chan struct{} // closed when the slot has been handed over
	granted bool          // guarded by slotSem.mu
	low     bool          // queued on slotSem.low
}

// grantLocked hands free slots (per limit) to waiters in arrival order,
// regular waiters first; low-priority ones only once none is queued.
func (s *slotSem) grantLocked(limit int) { s.grantQueuedLocked(limit, true) }

// grantQueuedLocked is grantLocked; withLow false leaves the low-priority
// lane alone.
func (s *slotSem) grantQueuedLocked(limit int, withLow bool) {
	for s.held < limit {
		if front := s.waiters.Front(); front != nil {
			w := s.waiters.Remove(front).(*slotWaiter)
			w.granted = true
			s.held++
			close(w.ready)
			continue
		}
		front := s.low.Front()
		if !withLow || front == nil || s.lowHeld >= s.lowLimit {
			return
		}
		w := s.low.Remove(front).(*slotWaiter)
		w.granted = true
		s.held++
		s.lowHeld++
		close(w.ready)
	}
}

// acquire blocks until a slot is available under limit (re-evaluated via
// limitFn while blocked) or ctx is done. limitFn is invoked outside the lock.
func (s *slotSem) acquire(ctx context.Context, limitFn func() int) error {
	limit := limitFn()
	s.mu.Lock()
	// A raised limit may free slots for queued regular waiters first; the
	// low-priority lane must not take one ahead of this caller.
	s.grantQueuedLocked(limit, false)
	if s.held < limit && s.waiters.Len() == 0 {
		s.held++
		s.mu.Unlock()
		return nil
	}
	w := &slotWaiter{ready: make(chan struct{})}
	el := s.waiters.PushBack(w)
	s.mu.Unlock()
	return s.wait(ctx, w, el, limitFn)
}

// acquireLow is acquire for the low-priority lane: it blocks until a slot is
// free, no regular waiter is queued and the lane holds fewer than lowLimit
// slots, or until ctx is done. Release the slot with releaseLow.
func (s *slotSem) acquireLow(ctx context.Context, limitFn func() int, lowLimit int) error {
	limit := limitFn()
	s.mu.Lock()
	s.lowLimit = lowLimit
	s.grantLocked(limit)
	if s.held < limit && s.lowHeld < s.lowLimit && s.waiters.Len() == 0 && s.low.Len() == 0 {
		s.held++
		s.lowHeld++
		s.mu.Unlock()
		return nil
	}
	w := &slotWaiter{ready: make(chan struct{}), low: true}
	el := s.low.PushBack(w)
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
				if w.low {
					s.lowHeld--
				}
				s.grantLocked(limit)
			case w.low:
				s.low.Remove(el)
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

// release returns a slot and wakes the next waiter(s) if the limit allows.
func (s *slotSem) release(limitFn func() int) {
	s.mu.Lock()
	if s.held > 0 {
		s.held--
	}
	queued := s.waiters.Len()+s.low.Len() > 0
	s.mu.Unlock()
	if !queued {
		return // nobody to wake: skip the (settings-mutex) limit lookup
	}
	limit := limitFn()
	s.mu.Lock()
	s.grantLocked(limit)
	s.mu.Unlock()
}

// releaseLow returns a slot acquired with acquireLow.
func (s *slotSem) releaseLow(limitFn func() int) {
	s.mu.Lock()
	if s.lowHeld > 0 {
		s.lowHeld--
	}
	s.mu.Unlock()
	s.release(limitFn)
}
