// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Opt-in HLS session persistence (HLSConfig.Persist / RIVULET_HLS_PERSIST).
//
// By default every hlsManager gets a fresh, random, owner-only rivulet-engine-hls-*
// directory (MED-2, see newHLSBaseDir) that CloseHLS deletes, and session
// state lives only in memory, so a restart throws away every transcoded
// segment. With Persist on (and WorkDir set) the manager instead uses the
// stable directory <WorkDir>/rivulet-engine-hls-persist, writes a session.json next
// to each session's segments, keeps the directory on CloseHLS, and on
// startup rehydrates every still-valid session so already-transcoded
// segments are served straight from disk.
//
// Layout:
//
//	<WorkDir>/rivulet-engine-hls-persist/        0700, must be ours; exclusively locked while in use
//	<WorkDir>/rivulet-engine-hls-persist/.lock   the lock file (see lockPersistDir)
//	<WorkDir>/rivulet-engine-hls-persist/<id>/   0700, one directory per session id
//	<WorkDir>/rivulet-engine-hls-persist/<id>/session.json   0600, written atomically
//
// lastAccess durability: session.json is written once at creation (after the
// first successful probe), then rewritten by the reaper on each tick only for
// sessions whose lastAccess moved since the last write, and once more by
// CloseHLS. That bounds the cost to one small write per active session per
// ReaperInterval (<= 30s by default) and never touches the request path. A
// crash loses at most one ReaperInterval of lastAccess, which only makes the
// session look slightly older than it is. The newest file mtime in the
// session dir was rejected as the source of truth because serving an
// already-transcoded segment from cache doesn't touch any file, so a title
// being watched entirely from pre-transcoded segments would look idle.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

const (
	// persistDirName is the fixed directory under HLSConfig.WorkDir that holds
	// persistent sessions. Fixed (not random) by design: the next process has
	// to find it.
	persistDirName = "rivulet-engine-hls-persist"
	// persistLockName is the lock file inside the persist dir.
	persistLockName = ".lock"
	// sessionFileName is the per-session metadata file.
	sessionFileName = "session.json"
	// sessionFormatVersion is bumped on any incompatible session.json change,
	// and whenever transcodeSegment's output-shaping ffmpeg arguments change
	// (it is part of outputFingerprint). A file with any other version is
	// discarded on load.
	sessionFormatVersion = 1
	// maxSessionFileSize bounds how much of a session.json is read on load.
	// Real files are a few KiB even with dozens of tracks.
	maxSessionFileSize = 1 << 20
)

// errPersistLocked reports that another process holds the persist dir lock.
var errPersistLocked = errors.New("persist dir is in use by another process")

// persistedSession is the on-disk shape of session.json.
type persistedSession struct {
	Version  int    `json:"version"`
	ID       string `json:"id"`
	MediaURL string `json:"mediaURL"`
	// Fingerprint is outputFingerprint(Config) as computed by the process
	// that wrote the file. On load it is recomputed from the same Config by
	// the current process: they differ only when a process-wide knob (CRF,
	// VAAPI QP, audio, segment/GOP layout, format version) changed between
	// runs, in which case the session's old segments would not match new
	// ones and it is discarded.
	Fingerprint string          `json:"fingerprint"`
	Config      persistedConfig `json:"config"`
	Probe       persistedProbe  `json:"probe"`
	LastAccess  time.Time       `json:"lastAccess"`
	// TTL is the session's own per-session ?ttl= override in nanoseconds
	// (RIVULET_HLS_SESSION_OVERRIDES; see hlsSession.ttl), 0 when unset (the
	// restored session then falls back to the current process's
	// HLSConfig.SessionTTL, exactly like a freshly created one). An explicit
	// per-session ttl must survive a restart: it is what makes a session
	// created ahead of a scheduled party outlive the process, and — per
	// evictIdle's documented precedence — it is honoured even when
	// DisableIdleEviction (RIVULET_HLS_SESSION_TTL=0) is set.
	TTL time.Duration `json:"ttl,omitempty"`
}

// persistedConfig mirrors sessionConfig (the per-session snapshot, see
// hlsSession.tc). maxrateBps is not stored: it is derived from VideoMaxrate.
type persistedConfig struct {
	VideoBitrate string `json:"videoBitrate"`
	VideoMaxrate string `json:"videoMaxrate"`
	VideoBufsize string `json:"videoBufsize"`
	MaxWidth     int    `json:"maxWidth"`
	MaxHeight    int    `json:"maxHeight"`
	HWEnabled    bool   `json:"hwEnabled"`
	X264Preset   string `json:"x264Preset"`
	NVENCPreset  string `json:"nvencPreset"`
	// QSVPreset is the h264_qsv -preset snapshot (RIVULET_TRANSCODE_QSV_PRESET
	// / transcodeProfile, see qsvPresetForX264). Required: a record without
	// one (e.g. written before this field existed) fails sessionConfig's
	// validation and is discarded, rather than restoring a session whose
	// h264_qsv segments would get an empty -preset argument.
	QSVPreset string `json:"qsvPreset"`
	// Overridden mirrors sessionConfig.overridden: whether a per-session
	// quality override (bitrate/maxrate/bufsize/maxWidth/maxHeight) shaped
	// this session, so a restored session's master.m3u8 keeps deriving
	// BANDWIDTH/CODECS from tc instead of reverting to the legacy strings.
	Overridden bool `json:"overridden,omitempty"`
}

// persistedColor mirrors videoColor (whose fields are unexported: it is the
// probe-result type used throughout internal/media, not a wire shape) so
// HDR/Dolby-Vision detection survives a restart and a restored session keeps
// tone-mapping (or not) the same way as the segments already cached from
// before the restart.
type persistedColor struct {
	Transfer     string `json:"transfer,omitempty"`
	Primaries    string `json:"primaries,omitempty"`
	Matrix       string `json:"matrix,omitempty"`
	ColorRange   string `json:"colorRange,omitempty"`
	HasDOVI      bool   `json:"hasDOVI,omitempty"`
	DOVIProfile  int    `json:"doviProfile,omitempty"`
	DOVIBLCompat int    `json:"doviBLCompat,omitempty"`
}

func toPersistedColor(c videoColor) persistedColor {
	return persistedColor{
		Transfer: c.transfer, Primaries: c.primaries, Matrix: c.matrix, ColorRange: c.colorRange,
		HasDOVI: c.hasDOVI, DOVIProfile: c.doviProfile, DOVIBLCompat: c.doviBLCompat,
	}
}

func (p persistedColor) toVideoColor() videoColor {
	return videoColor{
		transfer: p.Transfer, primaries: p.Primaries, matrix: p.Matrix, colorRange: p.ColorRange,
		hasDOVI: p.HasDOVI, doviProfile: p.DOVIProfile, doviBLCompat: p.DOVIBLCompat,
	}
}

// persistedProbe is the subset of probeMediaResult needed to skip re-probing.
type persistedProbe struct {
	Duration        float64          `json:"duration"`
	AudioStreams    []audioStream    `json:"audioStreams"`
	SubtitleStreams []subtitleStream `json:"subtitleStreams"`
	HighBitDepth    bool             `json:"highBitDepth"`
	Width           int              `json:"width"`
	Height          int              `json:"height"`
	// HasVideo, VideoEnd and IsTS feed segExpectation (segcheck.go) for
	// segments transcoded after a restart, exactly as they do for a session
	// created fresh in this process (see hlsSession.hasVideo/videoEnd/isTS).
	HasVideo bool           `json:"hasVideo,omitempty"`
	VideoEnd float64        `json:"videoEnd,omitempty"`
	IsTS     bool           `json:"isTS,omitempty"`
	Color    persistedColor `json:"color,omitempty"`
	// SeekIndexed selects the input-seek pre-roll (see segPreRollPlan). A
	// record without it restores with the full margin, as before.
	SeekIndexed bool `json:"seekIndexed,omitempty"`
}

func configToPersisted(tc sessionConfig) persistedConfig {
	return persistedConfig{
		VideoBitrate: tc.videoBitrate,
		VideoMaxrate: tc.videoMaxrate,
		VideoBufsize: tc.videoBufsize,
		MaxWidth:     tc.maxWidth,
		MaxHeight:    tc.maxHeight,
		HWEnabled:    tc.hwEnabled,
		X264Preset:   tc.x264Preset,
		NVENCPreset:  tc.nvencPreset,
		QSVPreset:    tc.qsvPreset,
		Overridden:   tc.overridden,
	}
}

// nvencPresets are the preset names h264_nvenc accepts (current p1..p7 plus
// the legacy aliases), for validating a persisted NVENCPreset.
var nvencPresets = map[string]bool{
	"p1": true, "p2": true, "p3": true, "p4": true, "p5": true, "p6": true, "p7": true,
	"default": true, "slow": true, "medium": true, "fast": true, "hp": true, "hq": true,
	"bd": true, "ll": true, "llhq": true, "llhp": true, "lossless": true, "losslesshp": true,
}

// qsvPresets are the preset names h264_qsv accepts (see qsvPresetForX264),
// for validating a persisted QSVPreset.
var qsvPresets = map[string]bool{
	"veryfast": true, "faster": true, "fast": true, "medium": true,
	"slow": true, "slower": true, "veryslow": true,
}

// sessionConfig validates c (session.json is read back as untrusted input and
// its values end up as ffmpeg arguments) and converts it to the in-memory
// snapshot. A preset is accepted if it is a known name or equals the
// current process's configured default (which may be any operator-chosen
// string, as today).
func (m *hlsManager) sessionConfig(c persistedConfig) (sessionConfig, error) {
	for _, r := range []struct{ name, v string }{
		{"videoBitrate", c.VideoBitrate}, {"videoMaxrate", c.VideoMaxrate}, {"videoBufsize", c.VideoBufsize},
	} {
		if _, ok := parseDecimalBitrate(r.v); !ok {
			return sessionConfig{}, fmt.Errorf("invalid %s %q", r.name, r.v)
		}
	}
	maxrateBps, _ := parseDecimalBitrate(c.VideoMaxrate)
	if c.MaxWidth < 0 || c.MaxHeight < 0 {
		return sessionConfig{}, fmt.Errorf("invalid max size %dx%d", c.MaxWidth, c.MaxHeight)
	}
	if !isKnownX264Preset(c.X264Preset) && c.X264Preset != m.cfg.X264Preset {
		return sessionConfig{}, fmt.Errorf("unknown x264 preset %q", c.X264Preset)
	}
	if !nvencPresets[c.NVENCPreset] && c.NVENCPreset != m.cfg.NVENCPreset {
		return sessionConfig{}, fmt.Errorf("unknown nvenc preset %q", c.NVENCPreset)
	}
	if !qsvPresets[c.QSVPreset] && c.QSVPreset != m.cfg.QSVPreset {
		return sessionConfig{}, fmt.Errorf("unknown qsv preset %q", c.QSVPreset)
	}
	return sessionConfig{
		videoBitrate: c.VideoBitrate,
		videoMaxrate: c.VideoMaxrate,
		videoBufsize: c.VideoBufsize,
		maxrateBps:   maxrateBps,
		maxWidth:     c.MaxWidth,
		maxHeight:    c.MaxHeight,
		hwEnabled:    c.HWEnabled,
		x264Preset:   c.X264Preset,
		nvencPreset:  c.NVENCPreset,
		qsvPreset:    c.QSVPreset,
		overridden:   c.Overridden,
	}, nil
}

// outputFingerprint is a readable, canonical description of every setting
// that shapes a transcoded segment: the format version and segment/GOP
// layout constants, the session's own bitrate caps, downscale limits and
// encoder presets (tc), and the process-wide CRF/QP/audio knobs from m.cfg.
//
// tc is the session's own snapshot, so a /settings change between runs does
// not invalidate a restored session: in-process, an existing session keeps
// encoding with its snapshot regardless of /settings, and that stays true
// across a restart. Only the process-wide knobs can make the current
// process's output differ from the session's existing segments.
//
// Deliberately excluded: the encoder actually used (m.enc: hardware vs
// libx264, and the VAAPI device) and tc.hwEnabled. The hardware paths don't
// use byte-identical settings to libx264 (no -keyint_min/-sc_threshold/
// -profile:v, -qp on VAAPI), but transcodeSegment already falls back from
// the hardware encoder to libx264 per segment on any error, so a session can
// contain segments from both today. Including the encoder would throw away
// hours of pre-transcoded work just because, say, a GPU driver failed to
// load after a reboot, while ruling out nothing a live session can't hit.
//
// pre is the session's pre-roll plan (segPreRollPlan). The first attempt's
// pre-roll decides which source frames a segment starts decoding from, so
// a restored session whose pre-roll changed (RIVULET_HLS_SEEK_PREROLL, for
// an indexed container) is re-transcoded rather than mixing segments. It
// is appended only when it differs from the full margin, so the
// fingerprint of every session at the default is unchanged.
func (m *hlsManager) outputFingerprint(tc sessionConfig, pre preRollPlan) string {
	fp := fmt.Sprintf(
		"v%d seg=%s gop=%d b:v=%s maxrate=%s bufsize=%s max=%dx%d x264=%s crf=%d nvenc=%s qsv=%s vaapi_qp=%d ac=%d b:a=%s",
		sessionFormatVersion, ftoa(segDur), videoGOP,
		tc.videoBitrate, tc.videoMaxrate, tc.videoBufsize, tc.maxWidth, tc.maxHeight,
		tc.x264Preset, m.cfg.X264CRF, tc.nvencPreset, tc.qsvPreset, m.cfg.VAAPIQP,
		m.cfg.AudioChannels, m.cfg.AudioBitrate,
	)
	if pre.first != fullPreRoll {
		fp += " preroll=" + ftoa(pre.first)
	}
	return fp
}

// newHLSBase picks the manager's base working directory. persist is true only
// when cfg.Persist is on, cfg.WorkDir is set, and the stable persist
// directory under it is usable and exclusively locked (lock is then non-nil
// and must stay open for the manager's lifetime); in every other case the
// result is exactly newHLSBaseDir(cfg.WorkDir), i.e. today's fresh random
// directory. cfg must already be normalized.
func newHLSBase(cfg HLSConfig) (base string, persist bool, lock io.Closer) {
	log := logging.For("media")
	if cfg.Persist {
		if cfg.WorkDir == "" {
			log.Warn("RIVULET_HLS_PERSIST requires RIVULET_HLS_WORK_DIR; HLS sessions will not persist across restarts")
		} else if dir, lk, err := preparePersistDir(cfg.WorkDir); err != nil {
			log.Warn("HLS persist dir unusable; HLS sessions will not persist across restarts", "work_dir", cfg.WorkDir, "err", err)
		} else {
			log.Info("HLS sessions persist across restarts", "dir", dir)
			if cfg.SessionTTL == defaultSessionTTL && !cfg.DisableIdleEviction {
				log.Warn("RIVULET_HLS_PERSIST is on but RIVULET_HLS_SESSION_TTL is still the default; persisted sessions are evicted a minute after last use and discarded by any restart longer than that — raise it (e.g. 12h)", "session_ttl", cfg.SessionTTL)
			}
			return dir, true, lk
		}
	}
	return newHLSBaseDir(cfg.WorkDir), false, nil
}

// preparePersistDir creates (or reuses) <workDir>/rivulet-engine-hls-persist and
// takes its exclusive lock.
//
// Unlike newHLSBaseDir the name is predictable, which is what MED-2 moved
// away from: on a multi-user host another user could pre-create it to read
// or plant session data. So an existing directory is accepted only when it
// is a real directory (not a symlink) and, on POSIX, owned by this process's
// effective uid; its mode is then tightened to 0700 (securePersistDir).
// Anything else is refused, as is a directory another process has locked,
// and the caller falls back to the non-persistent layout.
func preparePersistDir(workDir string) (string, io.Closer, error) {
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return "", nil, fmt.Errorf("create work dir: %w", err)
	}
	dir := filepath.Join(workDir, persistDirName)
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", nil, fmt.Errorf("create %s: %w", dir, err)
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return "", nil, fmt.Errorf("stat %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return "", nil, fmt.Errorf("%s is not a directory (mode %s)", dir, fi.Mode())
	}
	if err := securePersistDir(dir, fi); err != nil {
		return "", nil, fmt.Errorf("%s: %w", dir, err)
	}
	lock, err := lockPersistDir(filepath.Join(dir, persistLockName))
	if err != nil {
		return "", nil, fmt.Errorf("lock %s: %w", dir, err)
	}
	return dir, lock, nil
}

// persistSession writes s's session.json. Errors are logged, not returned:
// persistence is best effort and must never fail a playback request.
// Sessions without a successful probe (duration 0) are not persisted, and
// neither is a session that is no longer the one registered under id (e.g.
// a slow post-probe write racing an eviction and a new session with the
// same id), so a stale write can never overwrite a newer session's file.
func (m *hlsManager) persistSession(id string, s *hlsSession) {
	m.mu.Lock()
	current := m.sessions[id] == s
	m.mu.Unlock()
	if !current {
		return
	}
	s.mu.RLock()
	rec := persistedSession{
		Version:     sessionFormatVersion,
		ID:          id,
		MediaURL:    s.mediaURL,
		Fingerprint: m.outputFingerprint(s.tc, m.segPreRollPlan(s.isTS, s.seekIndexed)),
		Config:      configToPersisted(s.tc),
		Probe: persistedProbe{
			Duration:        s.duration,
			AudioStreams:    s.audioStreams,
			SubtitleStreams: s.subtitleStreams,
			HighBitDepth:    s.highBitDepth,
			Width:           s.srcWidth,
			Height:          s.srcHeight,
			HasVideo:        s.hasVideo,
			VideoEnd:        s.videoEnd,
			IsTS:            s.isTS,
			Color:           toPersistedColor(s.color),
			SeekIndexed:     s.seekIndexed,
		},
		TTL: time.Duration(s.ttl.Load()),
	}
	s.mu.RUnlock()
	if rec.Probe.Duration <= 0 {
		return
	}
	la := s.lastAccess.Load()
	rec.LastAccess = time.Unix(0, la).UTC()
	if err := writeSessionFile(s.dir, rec); err != nil {
		logging.For("media").Warn("persist HLS session failed", "id", id, "err", err)
		return
	}
	s.persistedAccess.Store(la)
}

// flushAccess rewrites session.json for every session whose lastAccess moved
// since its last write. Called from the reaper after each eviction pass and
// from CloseHLS; never from the request path.
func (m *hlsManager) flushAccess() {
	type entry struct {
		id string
		s  *hlsSession
	}
	var dirty []entry
	m.mu.Lock()
	for id, s := range m.sessions {
		if s.lastAccess.Load() != s.persistedAccess.Load() {
			dirty = append(dirty, entry{id, s})
		}
	}
	m.mu.Unlock()
	// Disk I/O outside m.mu, as everywhere else in the manager.
	for _, e := range dirty {
		m.persistSession(e.id, e.s)
	}
}

// writeSessionFile writes rec to dir/session.json atomically: a 0600
// os.CreateTemp file in the same directory is written, fsynced, and renamed
// over the destination, so a crash leaves either the old or the new file,
// never a torn one. Leftover session.json.tmp* files from a crash mid-write
// match the *.tmp* cleanup in loadPersisted. The directory itself is not
// fsynced: losing the rename in a crash just means the previous session.json
// (or, for a brand new session, none, and the session is re-created on
// demand) — acceptable for a cache, and it keeps the write cheap.
func writeSessionFile(dir string, rec persistedSession) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	f, err := os.CreateTemp(dir, sessionFileName+".tmp*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync temp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, sessionFileName)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// readSessionFile reads and decodes dir/session.json, bounded to
// maxSessionFileSize and rejecting unknown fields and any trailing data.
func readSessionFile(dir string) (persistedSession, error) {
	var rec persistedSession
	f, err := os.Open(filepath.Join(dir, sessionFileName))
	if err != nil {
		return rec, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxSessionFileSize+1))
	if err != nil {
		return rec, err
	}
	if len(data) > maxSessionFileSize {
		return rec, fmt.Errorf("%s larger than %d bytes", sessionFileName, maxSessionFileSize)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return rec, fmt.Errorf("decode %s: %w", sessionFileName, err)
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return rec, fmt.Errorf("decode %s: trailing data", sessionFileName)
	}
	return rec, nil
}

// validatePersistedURL re-checks a persisted mediaURL without any network
// access: startup must not block on (or be fooled by) DNS. It requires an
// http(s) URL with a host, and rejects private/loopback/metadata IP literals
// unless the URL points at this server's own origin (the same carve-out as
// validateRemoteURL, limited to the same media routes), except those
// on allow (HLSConfig.PrivateAllow).
// Hostnames are left to the relay's dial-time guard, which every later
// ffmpeg fetch of a non-self URL goes through (SEC-4).
func validatePersistedURL(raw, selfBase string, allow *netguard.Allow) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme %q", u.Scheme)
	}
	if u.Hostname() == "" {
		return errors.New("URL has no host")
	}
	if isSelfOrigin(raw, selfBase) {
		return validateSelfURL(raw)
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil {
		if err := netguard.ValidateIPAllow(ip, true, allow); err != nil {
			return err
		}
	}
	return nil
}

// loadPersisted rehydrates sessions from m.base (the persist dir) into
// m.sessions. Called once from newHLS, while holding the persist dir lock,
// before the reaper starts. For each entry:
//
//   - non-directories (including symlinks, never followed) and names that
//     fail validSessionID are left alone and logged;
//   - a missing, unreadable, corrupt or other-version session.json, an id
//     mismatch, an invalid duration or config, or a mediaURL that fails
//     validatePersistedURL deletes the session directory;
//   - a stored fingerprint that differs from what this process computes for
//     the session's own config deletes it (old segments would mix outputs);
//   - a lastAccess older than its idle TTL deletes it (the reaper would
//     have): the session's own ttl override if it has one, else SessionTTL,
//     or never when DisableIdleEviction is set and it has no ttl of its own;
//     a stored ttl outside the range an override accepts deletes it too;
//   - otherwise *.tmp* partial files from an interrupted transcode, subtitle
//     extraction, or session.json write are removed and the session is kept.
//
// If more than MaxSessions sessions survive, the most recently accessed ones
// are kept and the rest deleted. Playlists and per-segment locks are rebuilt
// lazily on first request, exactly as for a new session.
func (m *hlsManager) loadPersisted() {
	log := logging.For("media")
	entries, err := os.ReadDir(m.base)
	if err != nil {
		log.Warn("read HLS persist dir failed", "dir", m.base, "err", err)
		return
	}
	now := time.Now()
	cutoff := now.Add(-m.cfg.SessionTTL)

	type candidate struct {
		id string
		s  *hlsSession
	}
	var kept []candidate
	for _, e := range entries {
		name := e.Name()
		if name == persistLockName {
			continue
		}
		// ReadDir reports the entry's own type (Lstat), so a symlink is not
		// a directory here and is never followed or deleted.
		if !e.IsDir() {
			log.Warn("ignoring non-directory entry in HLS persist dir", "name", name)
			continue
		}
		if !validSessionID(name) {
			log.Warn("ignoring HLS persist entry with an invalid session id", "name", name)
			continue
		}
		dir := filepath.Join(m.base, name)
		discard := func(msg string, args ...any) {
			log.Info(msg, append([]any{"id", name}, args...)...)
			if err := os.RemoveAll(dir); err != nil {
				log.Warn("remove persisted HLS session failed", "id", name, "err", err)
			}
		}
		rec, err := readSessionFile(dir)
		switch {
		case err != nil:
			discard("discarding persisted HLS session: unreadable session.json", "err", err)
			continue
		case rec.Version != sessionFormatVersion:
			discard("discarding persisted HLS session: unsupported format version", "version", rec.Version)
			continue
		case rec.ID != name:
			discard("discarding persisted HLS session: id mismatch", "file_id", rec.ID)
			continue
		case rec.Probe.Duration <= 0 || rec.Probe.Duration > maxSegIdx*segDur:
			// Also bounded above: writePlaylist lists every segment, so an
			// absurd duration from a tampered file must not be trusted.
			discard("discarding persisted HLS session: invalid duration", "duration", rec.Probe.Duration)
			continue
		}
		if err := validatePersistedURL(rec.MediaURL, m.selfBase, m.cfg.PrivateAllow); err != nil {
			discard("discarding persisted HLS session: media URL rejected", "err", err)
			continue
		}
		tc, err := m.sessionConfig(rec.Config)
		if err != nil {
			discard("discarding persisted HLS session: invalid config", "err", err)
			continue
		}
		if fp := m.outputFingerprint(tc, m.segPreRollPlan(rec.Probe.IsTS, rec.Probe.SeekIndexed)); rec.Fingerprint != fp {
			discard("discarding persisted HLS session: transcode settings changed", "was", rec.Fingerprint, "now", fp)
			continue
		}
		lastAccess := rec.LastAccess
		if lastAccess.After(now) {
			lastAccess = now // clock went backwards; don't let it live forever
		}
		if rec.TTL != 0 && (rec.TTL < minOverrideTTL || rec.TTL > maxOverrideTTL) {
			discard("discarding persisted HLS session: invalid ttl", "ttl", rec.TTL)
			continue
		}
		// Same rule as evictIdle: the session's own ttl if it has one, else
		// SessionTTL, and no idle cutoff at all when idle eviction is
		// disabled (RIVULET_HLS_SESSION_TTL=0) and it has no ttl of its own.
		switch {
		case rec.TTL > 0:
			if lastAccess.Before(now.Add(-rec.TTL)) {
				discard("discarding persisted HLS session: idle past its ttl", "last_access", rec.LastAccess, "ttl", rec.TTL)
				continue
			}
		case !m.cfg.DisableIdleEviction && lastAccess.Before(cutoff):
			discard("discarding persisted HLS session: idle past session TTL", "last_access", rec.LastAccess)
			continue
		}
		removeTmpFiles(dir)

		s := &hlsSession{
			mediaURL:        rec.MediaURL,
			dir:             dir,
			duration:        rec.Probe.Duration,
			audioStreams:    rec.Probe.AudioStreams,
			subtitleStreams: rec.Probe.SubtitleStreams,
			multiAudio:      len(rec.Probe.AudioStreams) >= 2,
			highBitDepth:    rec.Probe.HighBitDepth,
			srcWidth:        rec.Probe.Width,
			srcHeight:       rec.Probe.Height,
			hasVideo:        rec.Probe.HasVideo,
			videoEnd:        rec.Probe.VideoEnd,
			isTS:            rec.Probe.IsTS,
			seekIndexed:     rec.Probe.SeekIndexed,
			color:           rec.Probe.Color.toVideoColor(),
			segLocks:        map[string]*sync.Mutex{},
			playlistData:    map[string]struct{}{},
			tc:              tc,
		}
		s.ttl.Store(int64(rec.TTL))
		s.lastAccess.Store(lastAccess.UnixNano())
		s.persistedAccess.Store(rec.LastAccess.UnixNano())
		m.restorePins(s)
		kept = append(kept, candidate{name, s})
	}

	if len(kept) > m.cfg.MaxSessions {
		sort.Slice(kept, func(i, j int) bool {
			return kept[i].s.lastAccess.Load() > kept[j].s.lastAccess.Load()
		})
		for _, c := range kept[m.cfg.MaxSessions:] {
			log.Info("discarding persisted HLS session: over RIVULET_HLS_MAX_SESSIONS", "id", c.id, "max_sessions", m.cfg.MaxSessions)
			if err := os.RemoveAll(c.s.dir); err != nil {
				log.Warn("remove persisted HLS session failed", "id", c.id, "err", err)
			}
		}
		kept = kept[:m.cfg.MaxSessions]
	}

	m.mu.Lock()
	for _, c := range kept {
		m.sessions[c.id] = c.s
	}
	m.mu.Unlock()
	if len(kept) > 0 {
		log.Info("restored persisted HLS sessions", "count", len(kept), "dir", m.base)
	}
}

// removeTmpFiles deletes *.tmp* files directly inside dir: partial outputs
// left by an ffmpeg segment transcode (seg<n>.ts.tmp.ts), a subtitle
// extraction (sub<k>.vtt.tmp) or a session.json write that a crash or kill
// interrupted. Completed files only ever appear via rename, so they never
// match. Safe only because the persist dir lock guarantees no other live
// process is writing into dir.
func removeTmpFiles(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if ok, _ := filepath.Match("*.tmp*", e.Name()); ok {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				logging.For("media").Warn("remove partial HLS file failed", "file", e.Name(), "err", err)
			}
		}
	}
}
