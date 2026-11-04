// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Transcode-ahead (HLSConfig.TranscodeAhead / RIVULET_HLS_TRANSCODE_AHEAD).
//
// Once a client's request for segment n of a rendition has been answered,
// a background worker transcodes the next TranscodeAhead segments of that
// rendition (n+1 … n+TranscodeAhead, not past the end), so they are cached
// by the time the client asks for them; after a seek the first segment is
// the only one the client waits for. The worker:
//
//   - is one per session and rendition (seg<n>.ts, or a<k>seg<n>.ts per audio
//     stream k), and transcodes one segment at a time, in order, skipping
//     cached ones;
//   - moves its window along while the client plays on through it, and is
//     replaced when the client seeks out of it. The window covers the
//     segments from TranscodeAhead before the furthest one requested to its
//     last one, so requests that finish out of order, or a short step back,
//     don't count as a seek. On a seek the worker is cancelled when the
//     request arrives, before the requested segment is transcoded, which
//     stops a transcode it is running, since nobody wants that segment now;
//     the new worker starts once the request is answered. The worker is per
//     session, not per client: clients sharing a session far apart replace
//     each other's;
//   - transcodes through the transcode-ahead lane of the shared transcode
//     semaphore (acquireBackgroundSlot): at most AheadConcurrency at once
//     across every session, only while no live request is waiting for a
//     slot, and before preparation (prepare.go). A running transcode is not
//     interrupted for a live request, as with preparation. A segment a live
//     request (or preparation) is transcoding is left to it;
//   - stops at the first failed transcode: the client's next request
//     starts a new one;
//   - holds the session's inFlight count while it runs, like HLSFile, so the
//     reaper never evicts the session under it. It doesn't count as an
//     access, so a session nobody plays still goes idle by its TTL.
//     DeleteHLS and CloseHLS stop it.
//
// Segments it transcodes are cached like any other, under the session's
// cache cap (pinned, when a preparation of the session covers the
// rendition). A session of unknown duration gets no worker.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

// aheadState is one session's transcode-ahead workers.
type aheadState struct {
	mu      sync.Mutex
	workers map[string]*aheadWorker // by segment file prefix: "" or "a<k>"
}

// aheadWorker is one rendition's worker. base, next and end are guarded by
// aheadState.mu.
type aheadWorker struct {
	cancel context.CancelFunc
	base   int // the furthest segment a client asked for
	next   int // the next segment the worker looks at
	end    int // the window's last segment
}

// cancelAll stops every worker of the session without waiting for them.
func (a *aheadState) cancelAll() {
	a.mu.Lock()
	for _, w := range a.workers {
		w.cancel()
	}
	a.workers = nil
	a.mu.Unlock()
}

// inWindow reports whether a request for segment n continues w's window
// rather than seeking out of it. Guarded by aheadState.mu.
func (w *aheadWorker) inWindow(n, ahead int) bool {
	return n >= w.base-ahead && n <= w.end
}

// aheadKey is the workers key of a rendition: its segment file prefix.
func aheadKey(kind segKind, audioIdx int) string {
	if kind == segAudioOnly {
		return fmt.Sprintf("a%d", audioIdx)
	}
	return ""
}

// aheadSeek is called by HLSFile before it transcodes segment n of a
// rendition: when the request seeks out of the rendition's worker's window,
// it cancels the worker now, so a transcode nobody wants no longer holds a
// slot the request may need. transcodeAhead starts the new worker once the
// request is answered.
func (m *hlsManager) aheadSeek(s *hlsSession, n int, kind segKind, audioIdx int) {
	ahead := m.cfg.TranscodeAhead
	if ahead <= 0 {
		return
	}
	key := aheadKey(kind, audioIdx)
	a := &s.ahead
	a.mu.Lock()
	defer a.mu.Unlock()
	if w := a.workers[key]; w != nil && !w.inWindow(n, ahead) {
		w.cancel()
		delete(a.workers, key)
	}
}

// transcodeAhead is called by HLSFile once it has answered a request for
// segment n of a rendition (kind and audioIdx, as for transcodeSegment) of
// session id, whose duration is dur. It starts, moves along or replaces the
// rendition's worker (see the file comment). The caller holds s.inFlight.
func (m *hlsManager) transcodeAhead(id string, s *hlsSession, n int, kind segKind, audioIdx int, dur float64) {
	ahead := m.cfg.TranscodeAhead
	if ahead <= 0 || m.closing.Load() {
		return
	}
	end := min(n+ahead, segmentCount(dur)-1)
	if end <= n {
		return // the last segment, or the duration is unknown
	}
	key := aheadKey(kind, audioIdx)
	a := &s.ahead
	a.mu.Lock()
	defer a.mu.Unlock()
	if w := a.workers[key]; w != nil {
		if w.inWindow(n, ahead) {
			// The client plays on through the window: move it along.
			w.base = max(w.base, n)
			w.next = max(w.next, n+1)
			w.end = max(w.end, end)
			return
		}
		w.cancel() // a seek: what it is transcoding is no longer wanted
		delete(a.workers, key)
	}
	// DeleteHLS sets deleted before it cancels the workers under a.mu, so
	// either it finds the worker started below or this sees deleted.
	if s.deleted.Load() {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &aheadWorker{cancel: cancel, base: n, next: n + 1, end: end}
	if a.workers == nil {
		a.workers = map[string]*aheadWorker{}
	}
	a.workers[key] = w
	// HLSFile holds inFlight, so the session can't be removed before this
	// hold is taken; the worker releases it on exit.
	s.inFlight.Add(1)
	go m.runAhead(ctx, id, s, key, w, kind, audioIdx)
}

// runAhead is the worker goroutine (see the file comment).
func (m *hlsManager) runAhead(ctx context.Context, id string, s *hlsSession, key string, w *aheadWorker, kind segKind, audioIdx int) {
	a := &s.ahead
	defer func() {
		w.cancel()
		a.mu.Lock()
		if a.workers[key] == w {
			delete(a.workers, key)
		}
		a.mu.Unlock()
		if s.inFlight.Add(-1) == 0 && s.deleted.Load() {
			m.removeDeletedSession(id, s)
		}
	}()
	go func() {
		select {
		case <-m.stopCh: // CloseHLS
			w.cancel()
		case <-ctx.Done():
		}
	}()
	seg := m.transcode
	if seg == nil {
		seg = m.transcodeSegment
	}
	for {
		a.mu.Lock()
		i := w.next
		if i > w.end || ctx.Err() != nil {
			// Unregister under the lock that transcodeAhead extends the
			// window under, so a request either moves this worker along
			// before it looks again or finds it gone and starts another.
			if a.workers[key] == w {
				delete(a.workers, key)
			}
			a.mu.Unlock()
			return
		}
		w.next++
		a.mu.Unlock()
		if m.closing.Load() || s.deleted.Load() {
			return
		}
		name := fmt.Sprintf("%sseg%d.ts", key, i)
		if segCached(s.dir, name) {
			continue
		}
		_, err := seg(ctx, s, i, kind, audioIdx, segAhead)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil, errors.Is(err, errSegmentBusy):
		default:
			logging.For("media").Debug("hls transcode-ahead stopped", "id", id, "segment", name, "err", err)
			return
		}
	}
}
