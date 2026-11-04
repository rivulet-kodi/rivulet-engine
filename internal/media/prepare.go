// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Engine-side preparation (HLSConfig.Prepare / RIVULET_HLS_PREPARE).
//
// POST /hlsv2/{id}/prepare starts a background job that transcodes every
// segment of an existing session, so a front end can have a film ready
// before anyone plays it without pulling each segment over the network. The
// job:
//
//   - walks the session's segments in playback order (video or muxed, then
//     the selected audio renditions of each index), skipping cached ones;
//   - transcodes through the low-priority preparation lane of the shared
//     transcode semaphore (acquireBackgroundSlot): at most
//     PrepareConcurrency at once, only while no live request is waiting for
//     a slot, and after any queued transcode-ahead (ahead.go). A segment a
//     live request is transcoding is left to it and revisited later;
//   - pins the segments it prepares: evictSegments never removes them, and
//     they don't count against the per-session cache cap, so the LRU can't
//     evict a prepared film as the job goes. An explicit
//     RIVULET_HLS_SEGMENT_CACHE_BYTES is the exception: a session whose
//     estimated prepared size exceeds it is refused (ErrHLSPrepareTooLarge);
//   - retries every segment that failed or came out uncached once, after the
//     first pass, and stops early (state "failed") after
//     prepMaxConsecutiveFailures transcode errors in a row;
//   - holds the session's inFlight count while it runs, like HLSFile, so the
//     reaper never evicts a session that is being prepared. DeleteHLS and
//     CloseHLS stop it.
//
// GET reports progress and the cache status of every segment; DELETE stops
// the job and unpins the segments (they stay cached, under the cap again).
// With persistence on (HLSConfig.Persist) the request is kept in the
// session's prepare.json, a separate file so older versions, which reject
// unknown session.json fields, still restore the session, and the job
// resumes after a restart, skipping the segments already on disk.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

const (
	// Audio selections for PrepareHLS.
	prepAudioDefault = "default" // the default audio rendition only
	prepAudioAll     = "all"     // every audio rendition

	// Job states, as reported in types.HLSPrepareStatus.State.
	prepIdle    = "idle"
	prepRunning = "running"
	prepDone    = "done"
	prepFailed  = "failed"

	// prepFileName holds a persisted session's preparation request.
	prepFileName = "prepare.json"
	// prepMaxFileSize bounds how much of a prepare.json is read.
	prepMaxFileSize = 4 << 10

	// prepMaxConsecutiveFailures stops a job whose transcodes keep failing
	// (a source that is gone or refusing requests), instead of hammering it
	// for every remaining segment.
	prepMaxConsecutiveFailures = 3
	// prepMaxIssues caps the issues listed in a status.
	prepMaxIssues = 100
	// prepMaxBusyRounds is how many times per pass the job revisits
	// segments that live requests were transcoding before it leaves them.
	prepMaxBusyRounds = 30

	// prepResumeWait bounds how long ResumePrepared waits for the server's
	// own HTTP listener before resuming jobs anyway.
	prepResumeWait = 30 * time.Second
)

// prepBusyRetry is how long the job waits before revisiting segments that
// live requests were transcoding. A variable so tests can shorten it.
var prepBusyRetry = 2 * time.Second

// errSegmentBusy is returned by a background transcodeSegment when a live
// request, or another background transcode, holds the segment's lock.
var errSegmentBusy = errors.New("hls: segment busy")

// prepState is one session's preparation job state.
type prepState struct {
	// ctl serializes PrepareHLS/StopPrepareHLS for the session, including
	// the wait for a replaced job to exit.
	ctl sync.Mutex

	mu     sync.Mutex
	audio  string             // requested audio selection; "" when idle
	state  string             // prepRunning, prepDone, prepFailed; "" when idle
	err    string             // why a failed job stopped
	failed map[string]string  // per segment file, this job's last transcode error
	cancel context.CancelFunc // stops the running job; nil when none
	done   chan struct{}      // closed when the running job has exited
}

// cancelJob stops the running job, if any, without waiting for it.
func (p *prepState) cancelJob() {
	p.mu.Lock()
	cancel := p.cancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (p *prepState) setFailed(name, reason string) {
	p.mu.Lock()
	if p.failed == nil {
		p.failed = map[string]string{}
	}
	p.failed[name] = reason
	p.mu.Unlock()
}

func (p *prepState) clearFailed(name string) {
	p.mu.Lock()
	delete(p.failed, name)
	p.mu.Unlock()
}

// pinSet names the segment files a preparation job pins: every video (or
// muxed) segment, plus the a<k>seg<n>.ts segments of the audio renditions in
// audio. A nil *pinSet pins nothing.
type pinSet struct {
	audio map[int]bool
}

func (p *pinSet) has(name string) bool {
	if p == nil {
		return false
	}
	if strings.HasPrefix(name, "seg") {
		return true
	}
	rest, ok := strings.CutPrefix(name, "a")
	if !ok {
		return false
	}
	i := strings.Index(rest, "seg")
	if i <= 0 {
		return false
	}
	k, err := strconv.Atoi(rest[:i])
	return err == nil && p.audio[k]
}

// prepRendition is one media playlist a job prepares.
type prepRendition struct {
	playlist string  // playlist.m3u8, video.m3u8 or audio<k>.m3u8
	prefix   string  // segment file prefix: "" or "a<k>"
	kind     segKind // what transcodeSegment produces for it
	audioIdx int     // audio stream for segAudioOnly
}

func (r prepRendition) file(n int) string { return fmt.Sprintf("%sseg%d.ts", r.prefix, n) }

// prepPlan returns the renditions to prepare for a session and the pins
// that cover them. A single-audio session has one muxed rendition; a
// multi-audio one has the video plus the default audio rendition, or every
// audio rendition for prepAudioAll.
func prepPlan(multiAudio bool, streams []audioStream, audio string) ([]prepRendition, *pinSet) {
	pins := &pinSet{audio: map[int]bool{}}
	if !multiAudio {
		return []prepRendition{{playlist: "playlist.m3u8", kind: segMuxed}}, pins
	}
	rends := []prepRendition{{playlist: "video.m3u8", kind: segVideoOnly}}
	add := func(k int) {
		pins.audio[k] = true
		rends = append(rends, prepRendition{
			playlist: fmt.Sprintf("audio%d.m3u8", k),
			prefix:   fmt.Sprintf("a%d", k),
			kind:     segAudioOnly,
			audioIdx: k,
		})
	}
	if audio == prepAudioAll {
		for k := range streams {
			add(k)
		}
	} else {
		add(defaultAudioIndex(streams))
	}
	return rends, pins
}

// normalizePrepAudio validates a requested audio selection; "" means the
// default.
func normalizePrepAudio(audio string) (string, error) {
	switch audio {
	case "", prepAudioDefault:
		return prepAudioDefault, nil
	case prepAudioAll:
		return prepAudioAll, nil
	}
	return "", fmt.Errorf("hls: audio must be %q or %q, got %q: %w", prepAudioDefault, prepAudioAll, audio, types.ErrInvalidHLSOption)
}

// segmentCount is the number of segments in a session of dur seconds.
func segmentCount(dur float64) int {
	if dur <= 0 {
		return 0
	}
	return int(math.Ceil(dur / segDur))
}

// prepSnapshot returns what a plan and an estimate need from s.
func (s *hlsSession) prepSnapshot() (dur float64, multiAudio bool, streams []audioStream, tc sessionConfig) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.duration, s.multiAudio, s.audioStreams, s.tc
}

// prepEstimate is the expected size of rends' segments for a session of dur
// seconds at its peak video bitrate (the maxrate) plus the audio bitrate.
func (m *hlsManager) prepEstimate(tc sessionConfig, dur float64, hasAudio bool, rends []prepRendition) int64 {
	vbps := tc.maxrateBps
	if vbps <= 0 {
		vbps = legacyMaxrateBps
	}
	var abps int64
	if hasAudio {
		abps, _ = parseDecimalBitrate(m.cfg.AudioBitrate)
	}
	var bps int64
	for _, r := range rends {
		switch r.kind {
		case segMuxed:
			bps += vbps + abps
		case segVideoOnly:
			bps += vbps
		case segAudioOnly:
			bps += abps
		}
	}
	return int64(float64(bps) / 8 * dur)
}

// prepSession validates id and returns its registered session.
func (m *hlsManager) prepSession(id string) (*hlsSession, error) {
	if !validSessionID(id) {
		return nil, fmt.Errorf("hls: %w %q", types.ErrHLSInvalidSessionID, id)
	}
	m.mu.Lock()
	s, ok := m.sessions[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("hls: %w %s", types.ErrHLSSessionNotFound, id)
	}
	return s, nil
}

// PrepareHLS starts the preparation job of session id (see the file
// comment), or leaves it running when a job with the same audio selection
// already is; a job with another selection is replaced. A job that has
// finished is started again, which re-checks every segment.
func (m *hlsManager) PrepareHLS(id, audio string) (types.HLSPrepareStatus, error) {
	audio, err := normalizePrepAudio(audio)
	if err != nil {
		return types.HLSPrepareStatus{}, err
	}
	s, err := m.prepSession(id)
	if err != nil {
		return types.HLSPrepareStatus{}, err
	}
	s.lastAccess.Store(time.Now().UnixNano())
	dur, multiAudio, streams, tc := s.prepSnapshot()
	if dur <= 0 {
		return types.HLSPrepareStatus{}, fmt.Errorf("hls: %w: %s", types.ErrHLSDurationUnknown, id)
	}
	if !multiAudio {
		audio = prepAudioDefault // one muxed rendition either way
	}
	rends, _ := prepPlan(multiAudio, streams, audio)
	if limit := m.cfg.SegmentCacheBytes; limit > 0 {
		if est := m.prepEstimate(tc, dur, len(streams) > 0, rends); est > limit {
			return types.HLSPrepareStatus{}, fmt.Errorf("hls: %w: about %d bytes, RIVULET_HLS_SEGMENT_CACHE_BYTES is %d",
				types.ErrHLSPrepareTooLarge, est, limit)
		}
	}
	if err := m.startPrepare(id, s, audio, true); err != nil {
		return types.HLSPrepareStatus{}, err
	}
	return m.prepStatus(s), nil
}

// startPrepare starts s's job for audio (see PrepareHLS); save writes the
// request to prepare.json when persistence is on.
func (m *hlsManager) startPrepare(id string, s *hlsSession, audio string, save bool) error {
	p := &s.prep
	p.ctl.Lock()
	defer p.ctl.Unlock()
	p.mu.Lock()
	if p.cancel != nil && p.audio == audio {
		p.mu.Unlock()
		return nil
	}
	oldCancel, oldDone := p.cancel, p.done
	p.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
		<-oldDone
	}

	dur, multiAudio, streams, _ := s.prepSnapshot()
	rends, pins := prepPlan(multiAudio, streams, audio)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	// Publish cancel before taking inFlight: a DeleteHLS that runs after
	// the take below finds it (see DeleteHLS), and one that ran before it
	// has unregistered the session, which the check below sees.
	p.mu.Lock()
	p.audio, p.state, p.err, p.failed, p.cancel, p.done = audio, prepRunning, "", nil, cancel, done
	p.mu.Unlock()
	// Hold inFlight for the job's whole life, taken under m.mu while the
	// session is still registered (as HLSFile does), so neither the reaper
	// nor DeleteHLS removes its directory under it.
	m.mu.Lock()
	registered := m.sessions[id] == s
	if registered {
		s.inFlight.Add(1)
	}
	m.mu.Unlock()
	if !registered {
		cancel()
		p.mu.Lock()
		p.audio, p.state, p.cancel, p.done = "", "", nil, nil
		p.mu.Unlock()
		return fmt.Errorf("hls: %w %s", types.ErrHLSSessionNotFound, id)
	}
	s.pins.Store(pins)
	if save && m.persist {
		if err := writePrepFile(s.dir, audio); err != nil {
			logging.For("media").Warn("persist HLS preparation failed", "id", id, "err", err)
		}
	}
	logging.For("media").Info("hls preparation started", "id", id, "audio", audio, "segments", segmentCount(dur)*len(rends))
	go m.runPrepare(ctx, cancel, id, s, rends, segmentCount(dur), done)
	return nil
}

// runPrepare is the job goroutine: it runs the passes, records the final
// state and releases the session's inFlight hold.
func (m *hlsManager) runPrepare(ctx context.Context, cancel context.CancelFunc, id string, s *hlsSession, rends []prepRendition, n int, done chan struct{}) {
	defer close(done)
	defer func() {
		s.lastAccess.Store(time.Now().UnixNano())
		if s.inFlight.Add(-1) == 0 && s.deleted.Load() {
			m.removeDeletedSession(id, s)
		}
	}()
	defer cancel()
	go func() {
		select {
		case <-m.stopCh: // CloseHLS
			cancel()
		case <-ctx.Done():
		}
	}()

	state, reason := m.prepPasses(ctx, s, rends, n)
	p := &s.prep
	p.mu.Lock()
	if p.done == done {
		// "" (stopped by shutdown or deletion) leaves the session idle;
		// StopPrepareHLS and a replacing job reset the state themselves.
		p.state, p.err = state, reason
		p.cancel = nil
	}
	p.mu.Unlock()
	if state != "" {
		logging.For("media").Info("hls preparation finished", "id", id, "state", state, "err", reason)
	}
}

// prepPasses transcodes every segment of rends that isn't cached: a first
// pass, then one more over the segments that failed or came out uncached.
// It returns prepDone or prepFailed with the reason, or "" when the job was
// stopped (DeleteHLS, StopPrepareHLS, a replacing job, or shutdown).
func (m *hlsManager) prepPasses(ctx context.Context, s *hlsSession, rends []prepRendition, n int) (string, string) {
	seg := m.transcode
	if seg == nil {
		seg = m.transcodeSegment
	}
	type item struct {
		r prepRendition
		n int
	}
	stopped := func() bool { return ctx.Err() != nil || m.closing.Load() || s.deleted.Load() }
	todo := make([]item, 0, n*len(rends))
	for i := range n {
		for _, r := range rends {
			todo = append(todo, item{r, i})
		}
	}
	for pass := 0; pass < 2 && len(todo) > 0; pass++ {
		consecutive := 0 // in a row within this pass
		var again, busy []item
		busyRounds := 0
		for len(todo) > 0 {
			it := todo[0]
			todo = todo[1:]
			if stopped() {
				return "", ""
			}
			name := it.r.file(it.n)
			if segCached(s.dir, name) {
				s.prep.clearFailed(name)
			} else {
				_, err := seg(ctx, s, it.n, it.r.kind, it.r.audioIdx, segPrepare)
				switch {
				case stopped():
					return "", ""
				case errors.Is(err, errSegmentBusy):
					busy = append(busy, it)
				case err != nil:
					s.prep.setFailed(name, err.Error())
					again = append(again, it)
					if consecutive++; consecutive >= prepMaxConsecutiveFailures {
						return prepFailed, err.Error()
					}
				default:
					consecutive = 0
					s.prep.clearFailed(name)
					if !segCached(s.dir, name) {
						again = append(again, it) // served but not cached
					}
				}
				// Progress counts as use: with persistence, flushAccess
				// writes it to session.json, so a restart mid-job doesn't
				// find the session idle past its TTL and discard it.
				s.lastAccess.Store(time.Now().UnixNano())
			}
			if len(todo) == 0 && len(busy) > 0 && busyRounds < prepMaxBusyRounds {
				// Live requests were transcoding these; they are usually
				// cached by the time we come back.
				busyRounds++
				select {
				case <-ctx.Done():
					return "", ""
				case <-time.After(prepBusyRetry):
				}
				todo, busy = busy, nil
			}
		}
		todo = append(again, busy...)
	}
	return prepDone, ""
}

// segCached reports whether segment file name is cached in dir.
func segCached(dir, name string) bool {
	fi, err := os.Stat(filepath.Join(dir, name))
	return err == nil && fi.Size() > 0
}

// HLSPrepareStatus reports session id's preparation status (see
// types.HLSPrepareStatus). It does not count as an access to the session.
func (m *hlsManager) HLSPrepareStatus(id string) (types.HLSPrepareStatus, error) {
	s, err := m.prepSession(id)
	if err != nil {
		return types.HLSPrepareStatus{}, err
	}
	return m.prepStatus(s), nil
}

// prepStatus builds s's status. While idle the map covers the default plan,
// so a caller can check what a session already has cached before preparing
// it.
func (m *hlsManager) prepStatus(s *hlsSession) types.HLSPrepareStatus {
	dur, multiAudio, streams, tc := s.prepSnapshot()
	p := &s.prep
	p.mu.Lock()
	st := types.HLSPrepareStatus{State: p.state, Audio: p.audio, Error: p.err, Renditions: []types.HLSPrepareRendition{}}
	failed := maps.Clone(p.failed)
	p.mu.Unlock()
	if st.State == "" {
		st.State = prepIdle
	}
	n := segmentCount(dur)
	if n == 0 {
		return st
	}
	audio := st.Audio
	if audio == "" {
		audio = prepAudioDefault
	}
	rends, _ := prepPlan(multiAudio, streams, audio)
	st.Segments = n
	st.Total = n * len(rends)
	st.EstimatedBytes = m.prepEstimate(tc, dur, len(streams) > 0, rends)
	cached := s.cachedSegments()
	s.mu.RLock()
	uncached := maps.Clone(s.uncached)
	s.mu.RUnlock()
	issue := func(name, reason string) {
		if len(st.Issues) < prepMaxIssues {
			st.Issues = append(st.Issues, types.HLSPrepareIssue{File: name, Reason: reason})
		}
	}
	for _, r := range rends {
		b := make([]byte, n)
		for i := range n {
			name := r.file(i)
			switch {
			case cached[name] > 0:
				b[i] = 'c'
				st.Cached++
				st.CachedBytes += cached[name]
			case failed[name] != "":
				b[i] = 'f'
				st.Failed++
				issue(name, failed[name])
			case uncached[name] != "":
				b[i] = 'u'
				st.Uncached++
				issue(name, uncached[name])
			default:
				b[i] = '-'
			}
		}
		st.Renditions = append(st.Renditions, types.HLSPrepareRendition{Playlist: r.playlist, Map: string(b)})
	}
	return st
}

// StopPrepareHLS stops session id's preparation job, waits for it to exit
// and unpins its segments. The segments already cached stay; the eviction
// after the session's next cached live transcode trims them back under the
// cache cap.
func (m *hlsManager) StopPrepareHLS(id string) error {
	s, err := m.prepSession(id)
	if err != nil {
		return err
	}
	p := &s.prep
	p.ctl.Lock()
	defer p.ctl.Unlock()
	p.mu.Lock()
	cancel, done := p.cancel, p.done
	p.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	p.mu.Lock()
	p.audio, p.state, p.err, p.failed, p.cancel, p.done = "", "", "", nil, nil, nil
	p.mu.Unlock()
	s.pins.Store(nil)
	if m.persist {
		if err := os.Remove(filepath.Join(s.dir, prepFileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
			logging.For("media").Warn("remove HLS preparation file failed", "id", id, "err", err)
		}
	}
	return nil
}

// prepRecord is the on-disk shape of prepare.json.
type prepRecord struct {
	Audio string `json:"audio"`
}

// writePrepFile writes dir/prepare.json the way writeSessionFile writes
// session.json: a 0600 temp file, fsynced, renamed into place.
func writePrepFile(dir, audio string) error {
	data, err := json.Marshal(prepRecord{Audio: audio})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, prepFileName+".tmp*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(dir, prepFileName))
	}
	if err != nil {
		_ = os.Remove(tmp)
	}
	return err
}

// errPrepInvalid marks a prepare.json whose content is bad (as opposed to
// one that could not be read).
var errPrepInvalid = errors.New("invalid prepare file")

// readPrepFile reads dir/prepare.json. ok is false when there is none; an
// unreadable or invalid one is an error, wrapping errPrepInvalid for the
// latter.
func readPrepFile(dir string) (audio string, ok bool, err error) {
	f, err := os.Open(filepath.Join(dir, prepFileName))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, prepMaxFileSize+1))
	if err != nil {
		return "", false, err
	}
	if len(data) > prepMaxFileSize {
		return "", false, fmt.Errorf("%w: %s larger than %d bytes", errPrepInvalid, prepFileName, prepMaxFileSize)
	}
	var rec prepRecord
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return "", false, fmt.Errorf("%w: decode %s: %w", errPrepInvalid, prepFileName, err)
	}
	if rec.Audio != prepAudioDefault && rec.Audio != prepAudioAll {
		return "", false, fmt.Errorf("%w: %s: invalid audio %q", errPrepInvalid, prepFileName, rec.Audio)
	}
	return rec.Audio, true, nil
}

// ResumePrepared restarts, once, the jobs of restored sessions that were
// being prepared (they have a prepare.json), when preparation and
// persistence are both on. It returns at once: the jobs start in the
// background once the server's own HTTP listener (selfBase) accepts
// connections, since a torrent-backed session's segments are read through
// it and would all fail before it is up; after prepResumeWait they start
// anyway. The server calls it after starting its listeners.
func (m *hlsManager) ResumePrepared() {
	if !m.cfg.Prepare || !m.persist || !m.resumeOnce.CompareAndSwap(false, true) {
		return
	}
	go func() {
		if !m.waitSelfListener(prepResumeWait) {
			return // shutting down
		}
		m.resumePrepared()
	}()
}

// waitSelfListener waits until a TCP connection to selfBase's host
// succeeds, for at most limit. It returns false if CloseHLS ran meanwhile.
func (m *hlsManager) waitSelfListener(limit time.Duration) bool {
	u, err := url.Parse(m.selfBase)
	if err != nil || u.Host == "" {
		return true
	}
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", u.Host, time.Second); err == nil {
			_ = c.Close()
			return true
		}
		select {
		case <-m.stopCh:
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	logging.For("media").Warn("server listener not reachable; resuming HLS preparation anyway", "self", m.selfBase)
	return true
}

// resumePrepared restarts the jobs of restored sessions with a
// prepare.json (see ResumePrepared).
func (m *hlsManager) resumePrepared() {
	log := logging.For("media")
	m.mu.Lock()
	sessions := maps.Clone(m.sessions)
	m.mu.Unlock()
	for id, s := range sessions {
		audio, ok, err := readPrepFile(s.dir)
		if err != nil {
			if errors.Is(err, errPrepInvalid) {
				log.Warn("discarding invalid HLS preparation file", "id", id, "err", err)
				_ = os.Remove(filepath.Join(s.dir, prepFileName))
			} else {
				log.Warn("cannot read HLS preparation file; keeping it", "id", id, "err", err)
			}
			continue
		}
		if !ok {
			continue
		}
		if err := m.startPrepare(id, s, audio, false); err != nil {
			log.Warn("resume HLS preparation failed", "id", id, "err", err)
		}
	}
}

// --- types.HLSPreparer (delegates to the manager) ---

// HLSPrepareEnabled reports whether preparation is on (HLSConfig.Prepare).
func (p *prober) HLSPrepareEnabled() bool { return p.hls.cfg.Prepare }

func (p *prober) PrepareHLS(id, audio string) (types.HLSPrepareStatus, error) {
	return p.hls.PrepareHLS(id, audio)
}

func (p *prober) HLSPrepareStatus(id string) (types.HLSPrepareStatus, error) {
	return p.hls.HLSPrepareStatus(id)
}

func (p *prober) StopPrepareHLS(id string) error { return p.hls.StopPrepareHLS(id) }

// ResumeHLSPrepare resumes persisted preparation jobs (see
// hlsManager.ResumePrepared). Not part of types.MediaProber; the server
// calls it after starting its listeners.
func (p *prober) ResumeHLSPrepare() { p.hls.ResumePrepared() }

// restorePins pins the segments of a restored session's persisted
// preparation request from load time, so live playback before the job
// resumes cannot evict the prepared film.
func (m *hlsManager) restorePins(s *hlsSession) {
	if !m.cfg.Prepare {
		return
	}
	audio, ok, err := readPrepFile(s.dir)
	if err != nil || !ok {
		return
	}
	_, multiAudio, streams, _ := s.prepSnapshot()
	_, pins := prepPlan(multiAudio, streams, audio)
	s.pins.Store(pins)
}
