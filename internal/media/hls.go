// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// segDur is the HLS segment length in seconds.
const segDur = 4.0

// videoGOP is the fixed keyframe interval for all video encodes.
// Computed as round(30fps × 4s) = 120.  Aligns segment boundaries with IDR
// frames so the player can seek without decoding a preceding GOP.
const videoGOP = 120

// segKind identifies what streams a transcoded segment contains.
type segKind int

const (
	segMuxed     segKind = iota // video + first-audio, muxed (single-audio session)
	segVideoOnly                // video only, no audio (multi-audio session)
	segAudioOnly                // audio only for one track (multi-audio session)
)

// probeCacheMaxSize is the maximum number of entries kept in the negative
// probe cache.  Expired entries are swept on every insert; when the map
// still exceeds this limit the soonest-expiring entry is evicted.
const probeCacheMaxSize = 512

// maxSegIdx is the hard upper bound on any caller-supplied segment index.
// 1<<18 == 262 144 segments × 4 s ≈ 12 days; far beyond any real media.
const maxSegIdx = 1 << 18

// audioStream describes one audio track discovered by ffprobe.
type audioStream struct {
	Index     int    // global ffprobe stream index
	CodecName string // e.g. "ac3", "aac"
	Channels  int
	Language  string // tags.language (BCP-47, may be empty)
	Title     string // tags.title (may be empty)
	IsDefault bool   // disposition.default != 0
}

// subtitleStream describes one text-based subtitle track discovered by ffprobe.
// Image-based subtitle formats (pgssub, dvdsub, xsub, dvb_subtitle,
// hdmv_pgs_subtitle) are excluded: they cannot be converted to WebVTT text
// and require graphical composition — this is a format limitation, not a stub.
type subtitleStream struct {
	Index     int    // global ffprobe stream index
	SubIdx    int    // 0-based index among subtitle streams in the file
	CodecName string // e.g. "subrip", "ass", "webvtt", "mov_text"
	Language  string // tags.language (BCP-47, may be empty)
	Title     string // tags.title (may be empty)
	IsDefault bool   // disposition.default != 0
}

// hlsSession holds the state for one transcoded stream (keyed by client id).
// Segments are transcoded on demand so the client can seek anywhere; a VOD
// playlist computed from the media duration lists every segment up front.
type hlsSession struct {
	mediaURL        string
	dir             string
	mu              sync.RWMutex
	duration        float64
	audioStreams    []audioStream          // probed once in StartHLS; nil until then
	subtitleStreams []subtitleStream       // text-based subtitle tracks (subrip/ass/ssa/mov_text/webvtt)
	multiAudio      bool                   // true when len(audioStreams) >= 2
	highBitDepth    bool                   // true if any video stream is 10/12-bit
	srcWidth        int                    // probed video width; 0 if unknown/no video stream
	srcHeight       int                    // probed video height; 0 if unknown/no video stream
	hasVideo        bool                   // first video stream is real video (not absent / cover art)
	videoEnd        float64                // where that video stream ends, seconds; 0 if unknown
	isTS            bool                   // source container is MPEG-TS
	seekIndexed     bool                   // source container's input seek is frame-accurate (see isSeekIndexed)
	color           videoColor             // probed colour metadata of the first video stream (HDR detection)
	segLocks        map[string]*sync.Mutex // keyed by segment filename; guarded by mu
	segIdx          segIndex               // on-disk segment cache accounting (see segindex.go)
	lastAccess      atomic.Int64           // unix nanoseconds; updated on each StartHLS/HLSFile call
	// inFlight counts calls currently executing HLSFile (which covers
	// transcodeSegment/extractSubtitle/writePlaylist) for this session.
	// evictIdle refuses to remove any session with inFlight > 0, so the
	// reaper can never os.RemoveAll(s.dir) while ffmpeg is still writing
	// seg<n>.ts.tmp.ts into it. Incremented/decremented with defer so it
	// cannot leak on any error path.
	inFlight atomic.Int32
	// provisional lists, per segment filename, copies of suspicious
	// segments that were served but not cached (see installSegment), so the
	// next transcode of that segment can remove them. Guarded by mu; nil
	// until first use.
	provisional map[string][]string
	// deleted is set by DeleteHLS once the session has been unregistered from
	// hlsManager.sessions. HLSFile's exit path checks it after decrementing
	// inFlight so the last in-flight call can finish a deferred deletion (see
	// DeleteHLS). removed makes the directory removal itself run exactly once
	// even when DeleteHLS and that last exit both observe inFlight == 0.
	deleted atomic.Bool
	removed atomic.Bool
	// playlistData records which segPrefix playlists have already been rendered
	// and written to disk; content is immutable once duration is set, so a
	// presence marker is enough to skip the rebuild + write.  Guarded by mu.
	playlistData map[string]struct{}
	// tc is a snapshot of the effective transcode configuration taken once,
	// at session creation (StartHLS), applying any live /settings overrides
	// active at that moment (issue #20). Immutable for the life of the
	// session so every segment is encoded consistently even if /settings
	// changes again mid-playback; a new session picks up the new values.
	tc sessionConfig
	// ttl is this session's own idle-eviction TTL in nanoseconds, from the
	// ?ttl= per-session override (RIVULET_HLS_SESSION_OVERRIDES). 0 means
	// "use HLSConfig.SessionTTL". Unlike tc it is not frozen at creation: a
	// later master.m3u8 request carrying ?ttl= replaces it (see StartHLS), so
	// a caller can extend or shorten a session's lifetime without affecting
	// how its segments are encoded.
	ttl atomic.Int64
	// persistedAccess is the lastAccess value (unix nanoseconds) most recently
	// written to session.json. The reaper rewrites the file only when
	// lastAccess has moved past it, so an idle session costs no writes and a
	// busy one at most one write per ReaperInterval. Unused unless
	// HLSConfig.Persist is on.
	persistedAccess atomic.Int64
}

// idleTTL returns the idle-eviction window that applies to s: its own
// per-session TTL when one was set, else the process-wide default.
func (s *hlsSession) idleTTL(global time.Duration) time.Duration {
	if v := s.ttl.Load(); v > 0 {
		return time.Duration(v)
	}
	return global
}

// hwEncoder holds the selected H.264 encoder identity plus any device path
// required for it.  Software fallback is codec="libx264", isHW=false.
type hwEncoder struct {
	codec     string // ffmpeg codec name: "h264_vaapi", "h264_nvenc", "libx264", …
	isHW      bool   // false only for libx264
	driDevice string // VAAPI renderD* path; empty for all non-VAAPI codecs
}

// probeCacheEntry records a cached ffprobe result (positive or negative).
// Negative (zero-duration/error) results use HLSConfig.NegProbeTTL; positive
// results use HLSConfig.PosProbeTTL so duplicate sessions for the same URL
// skip re-probing.
type probeCacheEntry struct {
	result    probeMediaResult
	expiresAt time.Time
}

// hlsManager owns per-id sessions and the hardware-accel decision.
// enc is set once in newHLS() and is read-only thereafter; cfg is likewise
// resolved once (env-derived) and read-only — per-session /settings
// overrides live on hlsSession.tc instead (see effectiveSessionConfig).
type hlsManager struct {
	base string
	// selfBase is this server's own local base URL; media URLs pointing at it
	// are exempt from the private-address SSRF check (see validateRemoteURL).
	selfBase string
	enc      hwEncoder
	cfg      HLSConfig
	// tonemap is the HDR→SDR tone-mapping algorithm actually in effect:
	// cfg.Tonemap when ffmpeg has the required filters, else "" (off). Set
	// once in newHLS and read-only thereafter, like enc.
	tonemap string
	// settings is consulted at session-creation time (StartHLS) and on every
	// transcode-slot acquisition to apply live /settings overrides (issue
	// #20 "Related"). May be nil (no /settings integration; env defaults
	// only), which every settings* helper treats as "no override".
	settings SettingsSource

	mu         sync.Mutex
	sessions   map[string]*hlsSession
	probeCache map[string]probeCacheEntry // probe cache (positive+negative); keyed by mediaURL
	// draining holds sessions removed by DeleteHLS whose directory has not
	// been deleted from disk yet (an HLSFile call was still in flight, or the
	// RemoveAll is in progress). StartHLS refuses to re-create an id listed
	// here, because the new session would reuse the same <base>/<id>
	// directory and the pending RemoveAll would wipe it. Lazily allocated.
	draining map[string]*hlsSession
	// slots bounds concurrent ffmpeg segment-transcode jobs by
	// currentConcurrency() (see acquireTranscodeSlot). A resizable FIFO
	// semaphore instead of a fixed-capacity channel so the limit can change
	// live via /settings without rebuilding the primitive.
	slots slotSem
	// subs bounds concurrent subtitle extractions (see extractSubtitle).
	subs   slotSem
	stopCh chan struct{} // closed by CloseHLS to stop the reaper
	// reaperWG tracks the reaper goroutine (startReaper). With persistence
	// on, CloseHLS waits on it so its own flush is the last session.json
	// write and a reaper flush that read an older lastAccess can't land
	// after it.
	reaperWG sync.WaitGroup
	// closing is set by BeginShutdown (and CloseHLS) once the process has
	// started shutting down. The streams this process serves to ffmpeg (a
	// torrent file through its own HTTP server) may then go away under it, so transcodeSegment stops starting new
	// attempts (the libx264 fallback, the longer-pre-roll retry) and stops
	// caching what it produces (see transcodeSegment).
	closing atomic.Bool
	// persist is true only when HLSConfig.Persist was requested AND the
	// stable persist directory was usable (see newHLSBase); base is then
	// <WorkDir>/rivulet-engine-hls-persist, sessions write session.json, and
	// CloseHLS keeps base on disk. False reproduces the historical
	// per-process, deleted-on-close behaviour exactly.
	persist bool
	// persistLock holds the persist dir's exclusive lock (see
	// lockPersistDir) for the manager's lifetime; CloseHLS releases it. Nil
	// unless persist.
	persistLock io.Closer
	// probe is the ffprobe seam; nil means probeMedia. Only tests set it, so
	// StartHLS can be exercised end to end without a real ffprobe.
	probe func(ctx context.Context, mediaURL, selfBase string, timeout time.Duration) probeMediaResult
	// guard bounds concurrent probes and dedupes identical in-flight ones.
	// newHLS gives each manager its own; media.New swaps in the prober's so
	// Probe/Tracks/StartHLS share one semaphore.
	guard  probeGuard
	shared *probeGuard
}

// ── encoder detection ─────────────────────────────────────────────────────────

// ffmpegDetectTimeout bounds the startup capability probes (-encoders,
// -filters). A hung ffmpeg (broken driver, stalled mount) fails open to
// software encoding instead of blocking server start.
const ffmpegDetectTimeout = 10 * time.Second

// encListOnce guards the one-time run of `ffmpeg -hide_banner -encoders`.
var (
	encListOnce sync.Once
	encListOut  string
)

// encodersList returns the cached stdout of `ffmpeg -hide_banner -encoders`.
// The command is run exactly once per process; subsequent calls are instant.
func encodersList() string {
	encListOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), ffmpegDetectTimeout)
		defer cancel()
		out, err := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-encoders").Output()
		if err == nil {
			encListOut = string(out)
		}
	})
	return encListOut
}

// verifyEncoder confirms an encoder actually works on this hardware by running
// a 5-second test-transcode of a synthetic 64×64 source into /dev/null.
//
//   - preInput  — args inserted before -i (e.g. -vaapi_device /dev/dri/renderD128)
//   - preEncode — args inserted before -c:v (e.g. -vf format=nv12,hwupload)
//
// Returns true only if ffmpeg exits 0 within the timeout.
func verifyEncoder(codec string, preInput, preEncode []string) bool {
	args := []string{"-hide_banner", "-loglevel", "error"}
	args = append(args, preInput...)
	args = append(args,
		"-f", "lavfi", "-i", "testsrc2=size=64x64:rate=1:duration=1",
		"-frames:v", "1", "-an",
	)
	args = append(args, preEncode...)
	args = append(args, "-c:v", codec, "-f", "null", "-")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "ffmpeg", args...).Run() == nil
}

// selectEncoder probes available H.264 encoders and returns the best one that
// actually works on this machine.  Called once in newHLS(). driDevice is the
// VAAPI render node to use (HLSConfig.VAAPIDevice; RIVULET_HLS_VAAPI_DEVICE,
// default "/dev/dri/renderD128") — configurable for multi-GPU hosts (issue
// #20).
//
// Selection rules:
//   - RIVULET_HWACCEL=0                            → always libx264 (hard switch)
//   - RIVULET_HWACCEL=nvenc|qsv|vaapi|…            → explicit; listed encoder used
//     without the verify step (user accepts responsibility)
//   - RIVULET_HWACCEL="" or "auto" (default)       → auto; only verified encoders
//     chosen; priority: NVENC > QSV > VideoToolbox > VAAPI > V4L2M2M > libx264
func selectEncoder(driDevice string) hwEncoder {
	sw := hwEncoder{codec: "libx264"} // software fallback

	// Hard software-only override.
	if os.Getenv("RIVULET_HWACCEL") == "0" {
		return sw
	}

	out := encodersList()
	listed := func(enc string) bool { return strings.Contains(out, enc) }

	// Explicit profile override: use the requested encoder if listed.
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RIVULET_HWACCEL"))) {
	case "nvenc", "nvidia", "cuda":
		if listed("h264_nvenc") {
			return hwEncoder{codec: "h264_nvenc", isHW: true}
		}
		return sw
	case "qsv", "intel", "quicksync":
		if listed("h264_qsv") {
			return hwEncoder{codec: "h264_qsv", isHW: true}
		}
		return sw
	case "vaapi":
		if listed("h264_vaapi") {
			return hwEncoder{codec: "h264_vaapi", isHW: true, driDevice: driDevice}
		}
		return sw
	case "videotoolbox", "vt":
		if listed("h264_videotoolbox") {
			return hwEncoder{codec: "h264_videotoolbox", isHW: true}
		}
		return sw
	case "v4l2", "v4l2m2m":
		if listed("h264_v4l2m2m") {
			return hwEncoder{codec: "h264_v4l2m2m", isHW: true}
		}
		return sw
		// "" / "auto" / anything else: fall through to auto-detect.
	}

	// Auto-detect: run a real test-transcode for each candidate before selecting.
	// "Listed in ffmpeg -encoders" only means the codec was compiled in; it does
	// not mean the local driver, device, or session limits can actually open it.

	// NVENC (NVIDIA CUDA)
	if listed("h264_nvenc") && verifyEncoder("h264_nvenc", nil, nil) {
		return hwEncoder{codec: "h264_nvenc", isHW: true}
	}
	// Intel Quick Sync Video
	if listed("h264_qsv") && verifyEncoder("h264_qsv", nil, nil) {
		return hwEncoder{codec: "h264_qsv", isHW: true}
	}
	// Apple VideoToolbox (macOS)
	if listed("h264_videotoolbox") && verifyEncoder("h264_videotoolbox", nil, nil) {
		return hwEncoder{codec: "h264_videotoolbox", isHW: true}
	}
	// VAAPI (Linux: Intel Iris Xe, AMD, etc.) — needs the device path for both
	// the verification test-transcode and the real encode.
	if listed("h264_vaapi") {
		if _, statErr := os.Stat(driDevice); statErr == nil {
			preIn := []string{"-vaapi_device", driDevice}
			preEnc := []string{"-vf", "format=nv12,hwupload"}
			if verifyEncoder("h264_vaapi", preIn, preEnc) {
				return hwEncoder{codec: "h264_vaapi", isHW: true, driDevice: driDevice}
			}
		}
	}
	// V4L2 Memory-to-Memory (ARM / Raspberry Pi)
	if listed("h264_v4l2m2m") && verifyEncoder("h264_v4l2m2m", nil, nil) {
		return hwEncoder{codec: "h264_v4l2m2m", isHW: true}
	}

	return sw // no working hardware encoder found
}

// newHLS constructs the manager for one server instance. cfg is normalized
// (zero-value fields resolved to defaults, SegmentConcurrency resolved to
// runtime.NumCPU() when unset) before use; settings is consulted for live
// /settings overrides at session-creation time (may be nil).
func newHLS(selfBase string, cfg HLSConfig, settings SettingsSource) *hlsManager {
	cfg = cfg.normalize(runtime.NumCPU())
	base, persist, persistLock := newHLSBase(cfg)
	enc := selectEncoder(cfg.VAAPIDevice)
	if enc.isHW {
		logging.For("media").Info("HLS transcode using hardware encoder", "encoder", enc.codec, "device", enc.driDevice)
	} else {
		logging.For("media").Warn("HLS transcode using software encoder; expect high CPU", "encoder", enc.codec, "hint", "set RIVULET_HWACCEL=vaapi and ensure /dev/dri access")
	}
	// Only probe ffmpeg's filter list when tone mapping was requested, so
	// the default configuration spawns exactly the processes it always did.
	var tonemap string
	if cfg.Tonemap != "" {
		tonemap = resolveTonemap(cfg.Tonemap, filtersList())
	}
	m := &hlsManager{
		base:       base,
		selfBase:   selfBase,
		enc:        enc,
		cfg:        cfg,
		tonemap:    tonemap,
		settings:   settings,
		sessions:   map[string]*hlsSession{},
		probeCache: map[string]probeCacheEntry{},
		stopCh:     make(chan struct{}),
		persist:    persist,

		persistLock: persistLock,
	}
	if persist {
		// Rehydrate before the reaper starts and before the manager is
		// reachable by any request.
		m.loadPersisted()
	}
	m.startReaper()
	return m
}

// newHLSBaseDir creates and returns a fresh, unpredictable, owner-only
// (0700) working directory for one hlsManager instance (MED-2).
//
// workDir == "" preserves the original behaviour: os.MkdirTemp gives each
// manager instance a fresh, unpredictable, 0700 (owner-only) directory
// instead of a fixed, shared, 0755 path — which a co-resident user on a
// multi-user host could pre-create (or read) before this process ever
// started. workDir != "" (HLSConfig.WorkDir / RIVULET_HLS_WORK_DIR, issue
// #20) creates the same kind of fresh rivulet-engine-hls-* directory, rooted under
// workDir instead of the OS default temp dir — e.g. to place HLS segments on
// a specific fast or large disk. Falls back to the workDir-less behaviour if
// workDir can't be created/used.
func newHLSBaseDir(workDir string) string {
	if workDir != "" {
		if err := os.MkdirAll(workDir, 0o700); err == nil {
			if base, mkErr := os.MkdirTemp(workDir, "rivulet-engine-hls-*"); mkErr == nil {
				return base
			}
		}
		logging.For("media").Warn("RIVULET_HLS_WORK_DIR unusable; falling back to the OS temp dir", "work_dir", workDir)
	}
	base, err := os.MkdirTemp("", "rivulet-engine-hls-*")
	if err != nil {
		// Fall back to a per-process-unique path rather than the old
		// predictable shared name if MkdirTemp itself fails (e.g. a
		// read-only default temp dir).
		base = filepath.Join(os.TempDir(), fmt.Sprintf("rivulet-engine-hls-%d", os.Getpid()))
		_ = os.MkdirAll(base, 0o700)
	}
	return base
}

// localize rewrites the self-signed https loopback URL to plain http so ffmpeg
// (which doesn't ignore TLS errors) can read the stream.
func localize(u string) string {
	return strings.ReplaceAll(u, "https://127.0.0.1:12470", "http://127.0.0.1:11470")
}

// sanitizeM3U8Attr removes characters that are illegal inside a quoted-string
// M3U8 attribute value: double-quote, carriage return, newline, and comma.
// These would break the playlist syntax; stripping them is safe because the
// values are only decorative labels (NAME, LANGUAGE).
func sanitizeM3U8Attr(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '"', '\r', '\n', ',':
			return -1
		}
		return r
	}, s)
}

// ── probeMedia ────────────────────────────────────────────────────────────────

// probeMediaResult carries the output of a combined ffprobe run.
type probeMediaResult struct {
	duration        float64
	audioStreams    []audioStream
	subtitleStreams []subtitleStream
	highBitDepth    bool       // true if any video stream is 10/12-bit
	width           int        // first video stream's width; 0 if unknown/no video stream
	height          int        // first video stream's height; 0 if unknown/no video stream
	color           videoColor // first video stream's colour metadata (HDR detection)
	// hasVideo is true when the first video stream (the one -map 0:v:0
	// selects) is real video rather than attached cover art; videoEnd is
	// that stream's duration (0 if unknown); isTS is true for MPEG-TS input.
	// All three feed segment validation (see segExpectation). seekIndexed
	// selects the input-seek pre-roll (see isSeekIndexed, segPreRollPlan).
	hasVideo    bool
	videoEnd    float64
	isTS        bool
	seekIndexed bool
}

// probeMedia runs a single ffprobe with -show_format -show_streams and returns
// the media duration, every audio stream, whether the video is high-bit-depth,
// and the first video stream's resolution. timeout bounds the ffprobe child
// process (HLSConfig.ProbeTimeout; RIVULET_HLS_PROBE_TIMEOUT, default 30s —
// issue #20: slow-starting torrents/debrid links can exceed the historical
// fixed 30s). mediaURL is routed through the loopback relay (SEC-4) unless it
// matches selfBase. The caller must NOT hold any session lock when calling
// this function.
func probeMedia(ctx context.Context, mediaURL, selfBase string, timeout time.Duration) probeMediaResult {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	inputURL, protoWL, err := relayInput(mediaURL, selfBase)
	if err != nil {
		return probeMediaResult{}
	}
	out, err := runCapped(exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet", "-print_format", "json",
		"-protocol_whitelist", protoWL,
		"-show_format", "-show_streams",
		inputURL), ffprobeOutputLimit) // MED-1: bound ffprobe stdout
	if err != nil {
		return probeMediaResult{}
	}
	return parseProbeOutput(out)
}

// parseProbeOutput decodes the JSON printed by probeMedia's ffprobe run
// (-show_format -show_streams). Undecodable output yields the zero result.
func parseProbeOutput(out []byte) probeMediaResult {
	var r struct {
		Format struct {
			Duration   string `json:"duration"`
			FormatName string `json:"format_name"`
		} `json:"format"`
		Streams []struct {
			Index     int    `json:"index"`
			CodecType string `json:"codec_type"`
			CodecName string `json:"codec_name"`
			PixFmt    string `json:"pix_fmt"` // e.g. "yuv420p", "yuv420p10le"
			Profile   string `json:"profile"` // e.g. "High", "Main 10"
			Channels  int    `json:"channels"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
			Duration  string `json:"duration"`
			// Colour metadata, used to detect HDR (PQ/HLG) sources.
			ColorTransfer  string `json:"color_transfer"`
			ColorPrimaries string `json:"color_primaries"`
			ColorSpace     string `json:"color_space"`
			ColorRange     string `json:"color_range"`
			// Decoded entry by entry (see doviFromSideData) so an unexpected
			// field type in one side-data entry cannot fail the whole probe.
			SideDataList []json.RawMessage `json:"side_data_list"`
			Tags         struct {
				Language string `json:"language"`
				Title    string `json:"title"`
				Duration string `json:"DURATION"` // Matroska per-stream duration
			} `json:"tags"`
			Disposition struct {
				Default     int `json:"default"`
				AttachedPic int `json:"attached_pic"` // cover art, not a real video track
			} `json:"disposition"`
		} `json:"streams"`
	}
	if json.Unmarshal(out, &r) != nil {
		return probeMediaResult{}
	}
	d, _ := strconv.ParseFloat(r.Format.Duration, 64)
	if math.IsNaN(d) || math.IsInf(d, 0) || d <= 0 || d > maxSegIdx*segDur {
		d = 0 // probe failure: bounds the playlist size writePlaylist emits
	}
	var audio []audioStream
	var subs []subtitleStream
	var subCount int // tracks 0-based index among subtitle streams
	var highBit bool
	var width, height int
	var color videoColor
	var sawVideo, hasVideo bool
	var videoEnd float64
	for _, st := range r.Streams {
		switch st.CodecType {
		case "audio":
			audio = append(audio, audioStream{
				Index:     st.Index,
				CodecName: st.CodecName,
				Channels:  st.Channels,
				Language:  st.Tags.Language,
				Title:     st.Tags.Title,
				IsDefault: st.Disposition.Default != 0,
			})
		case "video":
			// Detect 10/12-bit sources:
			//   pix_fmt: "yuv420p10le", "yuv422p12le", "p010le", etc. contain "10"/"12".
			//   profile:  "Main 10" (HEVC/AVC Hi10P) contains "10".
			// Common 8-bit formats ("yuv420p", "nv12", "yuvj420p", …) do not match.
			pf := strings.ToLower(st.PixFmt)
			pr := strings.ToLower(st.Profile)
			if strings.Contains(pf, "10") || strings.Contains(pf, "12") ||
				strings.Contains(pr, "10") {
				highBit = true
			}
			// Use the first video stream's dimensions (embedded thumbnails/
			// attached-pic streams, if any, are additional video streams).
			if width == 0 && height == 0 {
				width, height = st.Width, st.Height
			}
			if !sawVideo {
				sawVideo = true
				hasVideo = st.Disposition.AttachedPic == 0
				if hasVideo {
					videoEnd = parseStreamDuration(st.Duration, st.Tags.Duration)
				}
				// Colour metadata (HDR detection) from the first video stream.
				color = videoColor{
					transfer:   st.ColorTransfer,
					primaries:  st.ColorPrimaries,
					matrix:     st.ColorSpace,
					colorRange: st.ColorRange,
				}
				doviFromSideData(&color, st.SideDataList)
			}
		case "subtitle":
			// Only include text-based subtitle codecs that ffmpeg can convert to WebVTT.
			// Image-based formats (pgssub / dvdsub / xsub / dvb_subtitle /
			// hdmv_pgs_subtitle) are skipped — they cannot be losslessly text-converted.
			switch strings.ToLower(st.CodecName) {
			case "subrip", "srt", "ass", "ssa", "mov_text", "webvtt", "text", "jacosub", "realtext", "sami", "subviewer":
				subs = append(subs, subtitleStream{
					Index:     st.Index,
					SubIdx:    subCount,
					CodecName: st.CodecName,
					Language:  st.Tags.Language,
					Title:     st.Tags.Title,
					IsDefault: st.Disposition.Default != 0,
				})
			}
			subCount++ // always advance so SubIdx matches ffprobe's 0:s:<k> numbering
		}
	}
	return probeMediaResult{
		duration: d, audioStreams: audio, subtitleStreams: subs, highBitDepth: highBit,
		width: width, height: height, hasVideo: hasVideo, videoEnd: videoEnd,
		isTS: strings.Contains(r.Format.FormatName, "mpegts"), color: color,
		seekIndexed: isSeekIndexed(r.Format.FormatName),
	}
}

// doviFromSideData fills c's Dolby Vision fields from a stream's
// side_data_list, if it contains a "DOVI configuration record". Entries are
// decoded individually and malformed ones are skipped.
func doviFromSideData(c *videoColor, list []json.RawMessage) {
	for _, raw := range list {
		var sd struct {
			SideDataType       string `json:"side_data_type"`
			DVProfile          int    `json:"dv_profile"`
			DVBLSignalCompatID int    `json:"dv_bl_signal_compatibility_id"`
		}
		if json.Unmarshal(raw, &sd) != nil || sd.SideDataType != "DOVI configuration record" {
			continue
		}
		c.hasDOVI = true
		c.doviProfile = sd.DVProfile
		c.doviBLCompat = sd.DVBLSignalCompatID
	}
}

// ── StartHLS ──────────────────────────────────────────────────────────────────

// StartHLS registers a session for id (probing duration + audio/subtitle streams
// once) and returns the master playlist text.
//
//   - <=1 audio track, no text subs: single muxed variant pointing at playlist.m3u8.
//   - <=1 audio track, text subs present: muxed variant with SUBTITLES="subs".
//   - >=2 audio tracks: EXT-X-MEDIA TYPE=AUDIO group + single video.m3u8 variant;
//     text subtitle renditions are added alongside when present.
//
// Text subtitle renditions: one EXT-X-MEDIA:TYPE=SUBTITLES entry per text
// subtitle track. The URI points at sub<k>.m3u8 which serves a single-segment
// WebVTT playlist.  sub<k>.vtt is extracted from the container on first request.
//
// The master playlist's BANDWIDTH/CODECS are derived from the effective
// bitrate cap and output resolution (issue #20) — see deriveBandwidthCodecs.
//
// opts carries the per-session query overrides. They are ignored entirely
// unless HLSConfig.SessionOverrides (RIVULET_HLS_SESSION_OVERRIDES) is on;
// when on, they are validated on every request (an invalid value is an error
// wrapping types.ErrInvalidHLSOption) and then:
//   - on the request that creates the session, all of them apply: quality
//     overrides are folded into the session's snapshotted tc (see
//     effectiveSessionConfig) and ttl into s.ttl;
//   - on later requests for an existing session only ttl applies (replacing
//     the session's TTL); quality overrides are ignored, because tc is
//     frozen at creation so every segment of a session is encoded alike.
//
// Overrides never bypass the mediaURL SSRF check or the session-id guard
// below, which both run first regardless of opts.
func (m *hlsManager) StartHLS(id, mediaURL string, opts types.HLSSessionOptions) (string, error) {
	return m.StartHLSContext(context.Background(), id, mediaURL, opts)
}

// StartHLSContext is StartHLS with caller cancellation: a cancelled ctx stops
// this caller waiting on the (shared, deduplicated) initial probe. The probe
// itself keeps running for other waiters and still fills the probe cache.
func (m *hlsManager) StartHLSContext(ctx context.Context, id, mediaURL string, opts types.HLSSessionOptions) (string, error) {
	if mediaURL == "" {
		return "", fmt.Errorf("hls: missing mediaURL: %w", types.ErrInvalidHLSOption)
	}
	// Validated once here, before mediaURL is ever stored on the session, so
	// every later ffprobe/ffmpeg call on s.mediaURL (probeMedia, extractSubtitle,
	// transcodeSegment) is guaranteed to already be an http(s) URL on a
	// non-private, non-metadata host — or a media route on this server's
	// own origin (classifySelfPath), which is how the https UI on :12470 asks for a stream this
	// server serves on :11470 — or a private host on cfg.PrivateAllow
	// (RIVULET_HLS_PRIVATE_ALLOW).
	if err := validateMediaURL(mediaURL, m.selfBase, m.cfg.PrivateAllow); err != nil {
		return "", fmt.Errorf("hls: %w", err)
	}
	// Reject ids that could escape the base directory via path traversal.
	if !validSessionID(id) {
		return "", fmt.Errorf("hls: %w %q", types.ErrHLSInvalidSessionID, id)
	}
	var ov sessionOverrides
	if m.cfg.SessionOverrides {
		var err error
		if ov, err = parseSessionOverrides(opts); err != nil {
			return "", fmt.Errorf("hls: %w", err)
		}
	}
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		if _, busy := m.draining[id]; busy {
			m.mu.Unlock()
			return "", fmt.Errorf("hls: session %q is still being deleted; retry shortly", id)
		}
		if len(m.sessions) >= m.cfg.MaxSessions {
			m.mu.Unlock()
			return "", fmt.Errorf("hls: too many concurrent sessions (limit %d)", m.cfg.MaxSessions)
		}
		s = &hlsSession{
			mediaURL:     mediaURL,
			dir:          filepath.Join(m.base, id),
			segLocks:     map[string]*sync.Mutex{},
			playlistData: map[string]struct{}{},
			// Snapshot the effective transcode config now, at session
			// creation, so a POST /settings change afterward only affects
			// the *next* new session — see hlsSession.tc's doc comment.
			tc: m.effectiveSessionConfig(ov),
		}
		s.ttl.Store(int64(ov.ttl))
		s.lastAccess.Store(time.Now().UnixNano())
		dirPerm := os.FileMode(0o755)
		if m.persist {
			dirPerm = 0o700 // persisted sessions outlive the process; keep them private
		}
		_ = os.MkdirAll(s.dir, dirPerm)
		m.sessions[id] = s
		if ov != (sessionOverrides{}) {
			logging.For("media").Debug("hls session overrides applied", "id", id,
				"ttl", ov.ttl, "max_width", s.tc.maxWidth, "max_height", s.tc.maxHeight,
				"bitrate", s.tc.videoBitrate, "maxrate", s.tc.videoMaxrate, "bufsize", s.tc.videoBufsize)
		}
	} else if ov.ttl > 0 {
		// Existing session: only the TTL may change (see StartHLS doc).
		s.ttl.Store(int64(ov.ttl))
	}
	m.mu.Unlock()

	// Refresh lastAccess for both new and reused sessions.
	s.lastAccess.Store(time.Now().UnixNano())

	// Probe outside the session write lock so a slow ffprobe does not stall
	// concurrent HLSFile calls on the same session.
	s.mu.RLock()
	needProbe := s.duration == 0
	s.mu.RUnlock()

	if needProbe {
		// Negative-probe cache: avoid hammering a broken URL with repeated probes.
		m.mu.Lock()
		cached, hasCached := m.probeCache[mediaURL]
		m.mu.Unlock()

		var res probeMediaResult
		if hasCached && time.Now().Before(cached.expiresAt) {
			res = cached.result
		} else {
			// Identical concurrent probes (same URL, any session id) share one
			// ffprobe and count against the shared probe semaphore.
			v, perr := m.g().do(ctx, "hlsprobe|"+normalizeProbeKey(mediaURL), func(runCtx context.Context) (interface{}, error) {
				m.mu.Lock()
				c, ok := m.probeCache[mediaURL]
				m.mu.Unlock()
				if ok && time.Now().Before(c.expiresAt) {
					return c.result, nil
				}
				if err := m.g().acquire(runCtx); err != nil {
					return nil, err
				}
				defer m.g().release()
				r := m.runProbeCtx(runCtx, mediaURL)
				ttl := m.cfg.PosProbeTTL
				if r.duration == 0 {
					// Negative result: short-circuit future probes.
					ttl = m.cfg.NegProbeTTL
				}
				m.mu.Lock()
				m.probeCache[mediaURL] = probeCacheEntry{result: r, expiresAt: time.Now().Add(ttl)}
				// Prune expired / excess entries to bound cache size.
				m.sweepProbeCache()
				m.mu.Unlock()
				return r, nil
			})
			if perr != nil {
				return "", fmt.Errorf("hls: probe: %w", perr)
			}
			res = v.(probeMediaResult)
		}

		// Store result under the session write lock; another goroutine racing
		// through StartHLS for the same id may have already stored a valid probe.
		s.mu.Lock()
		stored := false
		if s.duration == 0 {
			stored = true
			s.duration = res.duration
			s.audioStreams = res.audioStreams
			s.subtitleStreams = res.subtitleStreams
			s.multiAudio = len(s.audioStreams) >= 2
			s.highBitDepth = res.highBitDepth
			s.srcWidth = res.width
			s.srcHeight = res.height
			s.hasVideo = res.hasVideo
			s.videoEnd = res.videoEnd
			s.isTS = res.isTS
			s.seekIndexed = res.seekIndexed
			s.color = res.color
		}
		s.mu.Unlock()
		if stored && m.tonemap != "" {
			if res.color.doviWithoutCompatibleBase() {
				logging.For("media").Debug("dolby vision source without an HDR10/HLG base layer; not tone mapping",
					"session", id, "dv_profile", res.color.doviProfile)
			} else if planTonemap(m.tonemap, res.color).enabled() {
				logging.For("media").Debug("tone mapping HDR source to SDR",
					"session", id, "transfer", res.color.transfer, "algorithm", m.tonemap)
			}
		}
		// Persist right after the first successful probe, so a crash moments
		// after creation still leaves a recoverable session behind.
		if stored && m.persist && res.duration > 0 {
			m.persistSession(id, s)
		}
	}

	s.mu.RLock()
	multiAudio := s.multiAudio
	audioStreams := s.audioStreams
	subtitleStreams := s.subtitleStreams
	srcWidth, srcHeight := s.srcWidth, s.srcHeight
	tc := s.tc
	s.mu.RUnlock()

	outW, outH, scaled := computeScaledDims(srcWidth, srcHeight, tc.maxWidth, tc.maxHeight)
	audioBps, _ := parseDecimalBitrate(m.cfg.AudioBitrate)
	bandwidth, codecsVideo := deriveBandwidthCodecs(tc, audioBps, outW, outH, scaled)

	return buildMasterPlaylist(masterPlaylistInputs{
		multiAudio:      multiAudio,
		audioStreams:    audioStreams,
		subtitleStreams: subtitleStreams,
		bandwidth:       bandwidth,
		codecsVideo:     codecsVideo,
	}), nil
}

// masterPlaylistInputs bundles everything buildMasterPlaylist needs,
// decoupled from hlsSession/probing so the playlist text can be
// golden-tested without a real ffprobe/ffmpeg session.
type masterPlaylistInputs struct {
	multiAudio      bool
	audioStreams    []audioStream
	subtitleStreams []subtitleStream
	bandwidth       int64
	codecsVideo     string // H.264 profile-level CODECS token, e.g. "avc1.640029"
}

// buildMasterPlaylist renders the HLS master playlist text for one session.
// See StartHLS's doc comment for the produced variant shapes; bandwidth/
// codecsVideo come from deriveBandwidthCodecs.
func buildMasterPlaylist(in masterPlaylistInputs) string {
	hasSubs := len(in.subtitleStreams) > 0
	streamInf := fmt.Sprintf("BANDWIDTH=%d,CODECS=\"%s,mp4a.40.2\"", in.bandwidth, in.codecsVideo)

	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:4\n")

	// ── Subtitle rendition group ──────────────────────────────────────────────
	// One EXT-X-MEDIA:TYPE=SUBTITLES entry per text subtitle stream.
	// The GROUP-ID "subs" is referenced in every EXT-X-STREAM-INF below.
	if hasSubs {
		for k, sub := range in.subtitleStreams {
			// Human-readable NAME: prefer title, then language, then ordinal.
			name := sub.Title
			if name == "" {
				name = sub.Language
			}
			if name == "" {
				name = fmt.Sprintf("Subtitle %d", k+1)
			}
			// LANGUAGE must be a BCP-47 tag; fall back to "und" when absent.
			lang := sub.Language
			if lang == "" {
				lang = "und"
			}
			isDefault := "NO"
			if k == 0 {
				isDefault = "YES"
			}
			fmt.Fprintf(&b,
				"#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",LANGUAGE=\"%s\",NAME=\"%s\",DEFAULT=%s,AUTOSELECT=YES,FORCED=NO,URI=\"sub%d.m3u8\"\n",
				sanitizeM3U8Attr(lang), sanitizeM3U8Attr(name), isDefault, k,
			)
		}
	}

	if !in.multiAudio {
		// Single-audio (or no-audio) path.
		if hasSubs {
			fmt.Fprintf(&b, "#EXT-X-STREAM-INF:%s,SUBTITLES=\"subs\"\nplaylist.m3u8\n", streamInf)
		} else {
			fmt.Fprintf(&b, "#EXT-X-STREAM-INF:%s\nplaylist.m3u8\n", streamInf)
		}
		return b.String()
	}

	// ── Multi-audio master playlist ───────────────────────────────────────────
	// Player audio-menu behaviour: the browser/AVPlayer sees each EXT-X-MEDIA
	// entry as a selectable audio rendition.  It follows the URI to audio<k>.m3u8
	// and fetches the corresponding a<k>seg<n>.ts files.

	// Determine which stream gets DEFAULT=YES (first with disposition.default,
	// or stream 0 if none is marked default).
	defaultIdx := 0
	for k, a := range in.audioStreams {
		if a.IsDefault {
			defaultIdx = k
			break
		}
	}

	for k, a := range in.audioStreams {
		// Human-readable NAME: prefer language tag, then title, then ordinal.
		name := a.Language
		if name == "" {
			name = a.Title
		}
		if name == "" {
			name = fmt.Sprintf("Audio %d", k+1)
		}
		defaultVal, autoVal := "NO", "NO"
		if k == defaultIdx {
			defaultVal, autoVal = "YES", "YES"
		}
		// Optional LANGUAGE attribute (BCP-47 tag forwarded from the container).
		langAttr := ""
		if a.Language != "" {
			langAttr = fmt.Sprintf(",LANGUAGE=\"%s\"", sanitizeM3U8Attr(a.Language))
		}
		fmt.Fprintf(&b,
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\"%s,NAME=\"%s\",DEFAULT=%s,AUTOSELECT=%s,URI=\"audio%d.m3u8\"\n",
			langAttr, sanitizeM3U8Attr(name), defaultVal, autoVal, k,
		)
	}

	// Single video-only variant; AUDIO="aud" links it to the audio rendition group.
	// SUBTITLES="subs" links it to the subtitle rendition group when subs are present.
	if hasSubs {
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:%s,AUDIO=\"aud\",SUBTITLES=\"subs\"\nvideo.m3u8\n", streamInf)
	} else {
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:%s,AUDIO=\"aud\"\nvideo.m3u8\n", streamInf)
	}
	return b.String()
}

// ── HLSFile ───────────────────────────────────────────────────────────────────

// HLSFile serves the VOD playlist or transcodes the requested segment on demand.
//
// filepath.Base(name) is applied first so no sub-path traversal is possible.
// Flat file name dispatch:
//
//	playlist.m3u8   → muxed VOD playlist  (single-audio path; seg<n>.ts)
//	video.m3u8      → video-only VOD playlist (multi-audio; seg<n>.ts, video-only transcode)
//	audio<k>.m3u8   → audio-only VOD playlist for stream k (a<k>seg<n>.ts)
//	sub<k>.m3u8     → single-segment WebVTT subtitle playlist
//	sub<k>.vtt      → full subtitle extracted to WebVTT (cached)
//	seg<n>.ts       → muxed seg (single-audio) or video-only seg (multi-audio)
//	a<k>seg<n>.ts   → audio-only segment for stream k
func (m *hlsManager) HLSFile(ctx context.Context, id, name string) (string, string, error) {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if ok {
		// Touch lastAccess and mark work in flight while still holding m.mu so
		// the reaper cannot evict this session between the map lookup and the
		// update (see evictIdle's inFlight check).
		s.lastAccess.Store(time.Now().UnixNano())
		s.inFlight.Add(1)
	}
	m.mu.Unlock()
	if !ok {
		return "", "", fmt.Errorf("hls: %w %s", types.ErrHLSSessionNotFound, id)
	}
	// Held for the whole call, including transcodeSegment/extractSubtitle,
	// which can run well past SessionTTL on a slow/software-encode host.
	// evictIdle skips any session with inFlight > 0, so a concurrent reaper
	// pass can never os.RemoveAll(s.dir) out from under an in-progress ffmpeg
	// write. lastAccess is refreshed again on the way out (success or error)
	// so the idle clock restarts from completion, not from before the
	// transcode began.
	//
	// If DeleteHLS ran while this call was in progress it left the directory
	// in place (inFlight > 0); the call that brings inFlight back to 0 then
	// performs the removal instead. See DeleteHLS for why this cannot race.
	defer func() {
		s.lastAccess.Store(time.Now().UnixNano())
		if s.inFlight.Add(-1) == 0 && s.deleted.Load() {
			m.removeDeletedSession(id, s)
		}
	}()

	name = filepath.Base(name)
	// Belt-and-suspenders: ensure the joined path stays inside the session directory.
	// filepath.Base already strips separators; this also catches name=="..".
	if p := filepath.Join(s.dir, name); !strings.HasPrefix(p, s.dir+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("hls: path %q not within session directory", name)
	}
	// Snapshot guarded fields before per-file dispatch.
	s.mu.RLock()
	multiAudio := s.multiAudio
	audioStreams := s.audioStreams
	subtitleStreams := s.subtitleStreams
	sessionDur := s.duration
	s.mu.RUnlock()

	// ── playlists ────────────────────────────────────────────────────────────

	if name == "playlist.m3u8" {
		// Muxed playlist: segment names are seg<n>.ts (segPrefix="").
		p := filepath.Join(s.dir, "playlist.m3u8")
		if err := s.writePlaylist(p, ""); err != nil {
			return "", "", err
		}
		return p, "application/vnd.apple.mpegurl", nil
	}

	if name == "video.m3u8" {
		if !multiAudio {
			return "", "", fmt.Errorf("hls: video.m3u8 is only available in multi-audio sessions")
		}
		p := filepath.Join(s.dir, "video.m3u8")
		// Same segment timing as the muxed playlist; seg<n>.ts are video-only.
		if err := s.writePlaylist(p, ""); err != nil {
			return "", "", err
		}
		return p, "application/vnd.apple.mpegurl", nil
	}

	if strings.HasPrefix(name, "audio") && strings.HasSuffix(name, ".m3u8") {
		kStr := strings.TrimSuffix(strings.TrimPrefix(name, "audio"), ".m3u8")
		k, err := strconv.Atoi(kStr)
		if err != nil || k < 0 || k >= len(audioStreams) {
			return "", "", fmt.Errorf("hls: bad audio playlist %q", name)
		}
		p := filepath.Join(s.dir, name)
		// Audio segments are named a<k>seg<n>.ts (segPrefix="a<k>").
		if err := s.writePlaylist(p, fmt.Sprintf("a%d", k)); err != nil {
			return "", "", err
		}
		return p, "application/vnd.apple.mpegurl", nil
	}

	// sub<k>.m3u8 — single-segment WebVTT subtitle playlist for text subtitle k.
	if strings.HasPrefix(name, "sub") && strings.HasSuffix(name, ".m3u8") {
		kStr := strings.TrimSuffix(strings.TrimPrefix(name, "sub"), ".m3u8")
		k, err := strconv.Atoi(kStr)
		if err != nil || k < 0 || k >= len(subtitleStreams) {
			return "", "", fmt.Errorf("hls: bad subtitle playlist %q", name)
		}
		p := filepath.Join(s.dir, name)
		if err := s.writeSubPlaylist(p, k); err != nil {
			return "", "", err
		}
		return p, "application/vnd.apple.mpegurl", nil
	}

	// sub<k>.vtt — full subtitle track extracted to WebVTT on first request.
	if strings.HasPrefix(name, "sub") && strings.HasSuffix(name, ".vtt") {
		kStr := strings.TrimSuffix(strings.TrimPrefix(name, "sub"), ".vtt")
		k, err := strconv.Atoi(kStr)
		if err != nil || k < 0 || k >= len(subtitleStreams) {
			return "", "", fmt.Errorf("hls: bad subtitle track %q", name)
		}
		p, err := m.extractSubtitle(ctx, s, k)
		if err != nil {
			return "", "", err
		}
		return p, "text/vtt; charset=utf-8", nil
	}

	// ── segments ─────────────────────────────────────────────────────────────

	if strings.HasPrefix(name, "seg") && strings.HasSuffix(name, ".ts") {
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "seg"), ".ts"))
		if err != nil || n < 0 || n > maxSegIdx {
			return "", "", fmt.Errorf("hls: bad segment %q", name)
		}
		if sessionDur > 0 {
			if n >= int(math.Ceil(sessionDur/segDur)) {
				return "", "", fmt.Errorf("hls: segment %d beyond media duration (%.0fs)", n, sessionDur)
			}
		}
		kind := segMuxed
		if multiAudio {
			kind = segVideoOnly
		}
		p, err := m.transcodeSegment(ctx, s, n, kind, 0)
		if err != nil {
			return "", "", err
		}
		return p, "video/mp2t", nil
	}

	// a<k>seg<n>.ts — audio-only segment for stream k.
	// Name format: "a" + k + "seg" + n + ".ts"
	if strings.HasPrefix(name, "a") && strings.HasSuffix(name, ".ts") {
		rest := strings.TrimSuffix(strings.TrimPrefix(name, "a"), ".ts")
		// rest == "<k>seg<n>"
		segIdx := strings.Index(rest, "seg")
		if segIdx < 0 {
			return "", "", fmt.Errorf("hls: bad segment name %q", name)
		}
		k, err := strconv.Atoi(rest[:segIdx])
		if err != nil || k < 0 || k >= len(audioStreams) {
			return "", "", fmt.Errorf("hls: bad audio segment %q", name)
		}
		n, err := strconv.Atoi(rest[segIdx+3:])
		if err != nil || n < 0 || n > maxSegIdx {
			return "", "", fmt.Errorf("hls: bad audio segment number in %q", name)
		}
		if sessionDur > 0 {
			if n >= int(math.Ceil(sessionDur/segDur)) {
				return "", "", fmt.Errorf("hls: segment %d beyond media duration (%.0fs)", n, sessionDur)
			}
		}
		p, err := m.transcodeSegment(ctx, s, n, segAudioOnly, k)
		if err != nil {
			return "", "", err
		}
		return p, "video/mp2t", nil
	}

	return "", "", fmt.Errorf("hls: not found %q", name)
}

// writePlaylist builds a VOD playlist listing every segment (enables seeking).
// segPrefix is prepended to the segment number in each URI, e.g.:
//
//	segPrefix=""   → seg0.ts, seg1.ts, …  (muxed or video-only)
//	segPrefix="a1" → a1seg0.ts, a1seg1.ts, …  (audio stream 1)
func (s *hlsSession) writePlaylist(path, segPrefix string) error {
	// Check cache under read-lock; playlist bytes are immutable once duration is set.
	s.mu.RLock()
	_, ok := s.playlistData[path]
	dur := s.duration
	s.mu.RUnlock()
	if ok {
		// Already rendered and written to disk on a prior request — skip rebuild.
		return nil
	}
	// Serialize the render+write per file: two concurrent first requests
	// used to share one ".tmp" path and could interleave their writes.
	l := s.lockFor(filepath.Base(path))
	l.Lock()
	defer l.Unlock()
	s.mu.RLock()
	_, done := s.playlistData[path]
	s.mu.RUnlock()
	if done {
		return nil
	}
	n := 0
	if dur > 0 {
		n = int(math.Ceil(dur / segDur))
	}
	if n == 0 {
		return fmt.Errorf("hls: unknown duration")
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:0\n", int(segDur)+1)
	for i := 0; i < n; i++ {
		d := segDur
		if i == n-1 {
			if rem := dur - float64(i)*segDur; rem > 0 {
				d = rem
			}
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%sseg%d.ts\n", d, segPrefix, i)
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	data := []byte(b.String())
	// Store rendered state so future requests for the same path skip the
	// O(n_segments) format loop and disk write entirely.  Keyed by path, not
	// segPrefix: playlist.m3u8 and video.m3u8 share prefix "" but are
	// distinct files.
	if err := writeFileAtomic(path, data); err != nil {
		return err
	}
	s.markPlaylistWritten(path)
	return nil
}

// writeSubPlaylist writes a single-segment VOD subtitle playlist for subtitle
// track k.  The playlist references sub<k>.vtt which is extracted on demand.
// Using a single segment spanning the full duration is correct: subtitle
// parsers handle the full VTT at once, and the player seeks within it natively.
func (s *hlsSession) writeSubPlaylist(path string, k int) error {
	// Same pattern as writePlaylist: rendered once per path (lock-free
	// check first), then serialized per file so concurrent first requests
	// never interleave writes.
	s.mu.RLock()
	_, done := s.playlistData[path]
	s.mu.RUnlock()
	if done {
		return nil
	}
	l := s.lockFor(filepath.Base(path))
	l.Lock()
	defer l.Unlock()
	s.mu.RLock()
	_, done = s.playlistData[path]
	dur := s.duration
	s.mu.RUnlock()
	if done {
		return nil
	}
	if dur <= 0 {
		dur = 0
	}
	targetDur := int(math.Ceil(dur))
	if targetDur < 1 {
		targetDur = 1
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:0\n", targetDur)
	fmt.Fprintf(&b, "#EXTINF:%.3f,\nsub%d.vtt\n", dur, k)
	b.WriteString("#EXT-X-ENDLIST\n")
	if err := writeFileAtomic(path, []byte(b.String())); err != nil {
		return err
	}
	if dur > 0 { // a degraded (unknown-duration) playlist must be re-rendered later
		s.markPlaylistWritten(path)
	}
	return nil
}

// markPlaylistWritten records that the playlist at path has been rendered.
func (s *hlsSession) markPlaylistWritten(path string) {
	s.mu.Lock()
	if s.playlistData == nil {
		s.playlistData = make(map[string]struct{})
	}
	s.playlistData[path] = struct{}{}
	s.mu.Unlock()
}

// writeFileAtomic writes data via a temp file + rename so a concurrent reader
// never sees a truncated file. Callers serialize writers of the same path.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// extractSubtitle extracts subtitle track k from the session media file to a
// WebVTT file cached in the session directory.  Thread-safe: concurrent calls
// for the same sub<k>.vtt block on the per-filename mutex; the file is written
// atomically (tmp → final rename) so partial writes are never served.
func (m *hlsManager) extractSubtitle(ctx context.Context, s *hlsSession, k int) (string, error) {
	s.mu.RLock()
	sub := s.subtitleStreams[k]
	s.mu.RUnlock()
	filename := fmt.Sprintf("sub%d.vtt", k)
	vttFile := filepath.Join(s.dir, filename)

	l := s.lockFor(filename)
	l.Lock()
	defer l.Unlock()

	if fi, err := os.Stat(vttFile); err == nil && fi.Size() > 0 {
		return vttFile, nil // already extracted
	}

	// Bound concurrent subtitle extractions: each one demuxes the whole
	// input through ffmpeg, so a client asking for every track at once must
	// not fork them all. Waiting honours the request context and happens
	// before the extraction timeout starts, so queueing does not eat it.
	if err := m.subs.acquire(ctx, subtitleLimit); err != nil {
		return "", err
	}
	defer m.subs.release(subtitleLimit)

	tmp := vttFile + ".tmp"
	ctx, cancel := context.WithTimeout(ctx, m.cfg.SubtitleTimeout)
	defer cancel()
	// SEC-4: route through the loopback relay instead of handing ffmpeg the
	// real remote URL directly.
	inputURL, protoWL, rerr := relayInput(s.mediaURL, m.selfBase)
	if rerr != nil {
		return "", fmt.Errorf("hls: subtitle extract %s: %w", filename, rerr)
	}
	// -map 0:s:<SubIdx> selects the k-th subtitle stream by its subtitle-stream
	// index (not global index), matching how ffprobe numbers subtitle streams.
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error", "-y",
		"-protocol_whitelist", protoWL,
		"-i", inputURL,
		"-map", fmt.Sprintf("0:s:%d", sub.SubIdx),
		"-f", "webvtt",
		tmp,
	)
	var stderr cappedBuffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("hls: subtitle extract %s: %w", filename, err)
	}
	// ffmpeg exits 0 after "Stream ends prematurely" or the Matroska demuxer's
	// "File ended prematurely" (the input was cut mid-demux) and leaves a
	// partial track behind. Never promote that to the permanent sub<k>.vtt cache: a later
	// request must re-extract.
	if stderr.containsFold("ends prematurely") || stderr.containsFold("ended prematurely") {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("hls: subtitle extract %s: input truncated: %s", filename, strings.TrimSpace(stderr.String()))
	}
	if fi, err := os.Stat(tmp); err != nil || fi.Size() == 0 {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("hls: subtitle extract %s: ffmpeg produced no output", filename)
	}
	if err := os.Rename(tmp, vttFile); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return vttFile, nil
}

// lockFor returns (creating if necessary) the per-filename mutex.
// Keyed by the segment filename so concurrent requests for different streams
// (e.g. "seg0.ts" and "a1seg0.ts") do not block each other. The common case
// (the mutex already exists, e.g. a cache-hit segment request) only takes the
// read lock; the write lock is needed once per filename.
func (s *hlsSession) lockFor(filename string) *sync.Mutex {
	s.mu.RLock()
	l, ok := s.segLocks[filename]
	s.mu.RUnlock()
	if ok {
		return l
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok = s.segLocks[filename]; !ok {
		l = &sync.Mutex{}
		if s.segLocks == nil {
			s.segLocks = map[string]*sync.Mutex{}
		}
		s.segLocks[filename] = l
	}
	return l
}

// ── transcode concurrency ───────────────────────────────────────────────────

// subtitleConcurrency caps concurrent subtitle extractions per process.
const subtitleConcurrency = 2

// subtitleLimit is the fixed limit for hlsManager.subs.
func subtitleLimit() int { return subtitleConcurrency }

// acquireTranscodeSlot blocks until a concurrent-transcode slot is available
// or ctx is done. Waiters queue FIFO on a semaphore woken by
// releaseTranscodeSlot (no polling); the limit is re-read from
// currentConcurrency() on every acquire/release and by blocked waiters on a
// slow tick, so a live POST /settings transcodeConcurrency change takes effect
// without rebuilding anything — concurrency bounds a single process-wide
// resource shared by every session, so "session creation time" (used by the
// other /settings-overridable knobs) isn't a natural fit here.
func (m *hlsManager) acquireTranscodeSlot(ctx context.Context) error {
	return m.slots.acquire(ctx, m.currentConcurrency)
}

// releaseTranscodeSlot releases a slot acquired by acquireTranscodeSlot and
// wakes the next queued waiter, if any.
func (m *hlsManager) releaseTranscodeSlot() {
	m.slots.release(m.currentConcurrency)
}

// currentConcurrency resolves the live effective SegmentConcurrency: the
// env-configured default (m.cfg.SegmentConcurrency, resolved to
// runtime.NumCPU() at construction when unset) unless the live
// transcodeConcurrency /settings value is set to something other than its
// settings-schema default of 1 (see effectiveSessionConfig's doc comment for
// why the schema default is excluded rather than treated as an active
// override).
func (m *hlsManager) currentConcurrency() int {
	limit := m.cfg.SegmentConcurrency
	if v, ok := settingsInt(m.settings, "transcodeConcurrency"); ok && v > 0 && v != 1 {
		limit = v
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// ── transcodeSegment ──────────────────────────────────────────────────────────

// transcodeSegment transcodes segment n on demand (cached to disk in s.dir).
//
//   - segMuxed:     -map 0:v:0? -map 0:a:0?  h264 + aac  (single-audio sessions)
//   - segVideoOnly: -map 0:v:0  -an           h264 only   (multi-audio sessions)
//   - segAudioOnly: -map 0:a:<audioIdx>  -vn  aac only    (multi-audio sessions)
//
// For video kinds (muxed/video-only) the configured hardware encoder is tried
// first (unless the session's tc.hwEnabled is false — transcodeHardwareAccel
// /settings override, issue #20); libx264 is the automatic fallback on any
// error. Audio is always software AAC.
//
// Robustness flags applied to all paths:
//   - Input:  -ss inputSeek (before -i) + -ss outputSeek (after -i) = hybrid seek
//     -fflags +genpts -analyzeduration 2000000 -probesize 2000000
//   - Video:  GOP alignment (-g/-keyint_min), bitrate caps (-b:v/-maxrate/-bufsize,
//     session-effective — see hlsSession.tc), optional downscale (scale/scale_vaapi,
//     see buildVideoFilter)
//   - Audio:  -af aresample=async=1:first_pts=0,apad  (keeps A/V aligned on seek)
//   - Output: -output_ts_offset -muxdelay 0 -t dur -mpegts_copyts 1
//     (-t as output option terminates apad in audio-only segments)
//
// The output is inspected before it is cached (see segcheck.go). A segment
// that is still served, but not cached, so the next request transcodes it
// again, has one of these reasons:
//   - "no video frames" or "video starts …": video missing or starting late;
//   - "video ends …": video stops early while the padded audio runs on;
//   - "no audio PES": audio missing where it is expected;
//   - "ffmpeg input error: …": ffmpeg logged an input read error (see
//     inputErrMarkers);
//   - "shutdown in progress": BeginShutdown ran before the segment finished.
//
// MPEG-TS input with missing/late video is first retried once with a longer
// input-seek pre-roll. After BeginShutdown, no software fallback is started
// for a failed hardware attempt (the error is returned, nothing is cached),
// and no retry is started (the first output is served, uncached).
//
// High-bit-depth safety: we never add -hwaccel decode flags (SW decode is always
// used, which is safe across all input formats).  For VAAPI the filter chain
// "format=nv12,hwupload" handles 10→8 bit downconversion before GPU upload.
// For other HW encoders, an explicit "format=yuv420p" filter is prepended when
// s.highBitDepth is true.
//
// HDR→SDR tone mapping (RIVULET_TRANSCODE_TONEMAP, off by default): for a
// PQ/HLG source the video filter becomes the software zscale+tonemap chain
// (see tonemapChain/buildVideoFilter) and BT.709 colour tags are added; all
// other sources, and every source with the knob off, are unaffected.
//
// Persistence (HLSConfig.Persist): restored sessions reuse segments written
// by an earlier process, guarded by outputFingerprint. Whenever the ffmpeg
// arguments below change in a way that alters the output, bump
// sessionFormatVersion (persist.go) so old segments are discarded instead of
// being spliced with new ones.
func (m *hlsManager) transcodeSegment(ctx context.Context, s *hlsSession, n int, kind segKind, audioIdx int) (string, error) {
	s.mu.RLock()
	sessionDur := s.duration
	highBitDepth := s.highBitDepth
	srcWidth, srcHeight := s.srcWidth, s.srcHeight
	hasVideo, videoEnd := s.hasVideo, s.videoEnd
	preRolls := m.segPreRollPlan(s.isTS, s.seekIndexed)
	hasAudio := len(s.audioStreams) > 0
	color := s.color
	tc := s.tc
	s.mu.RUnlock()
	var filename string
	if kind == segAudioOnly {
		filename = fmt.Sprintf("a%dseg%d.ts", audioIdx, n)
	} else {
		filename = fmt.Sprintf("seg%d.ts", n)
	}
	segFile := filepath.Join(s.dir, filename)

	l := s.lockFor(filename)
	l.Lock()
	defer l.Unlock()
	if fi, err := os.Stat(segFile); err == nil && fi.Size() > 0 {
		s.noteSegment(filename, fi.Size(), time.Now()) // refresh LRU recency
		return segFile, nil                            // already transcoded; serve from cache
	}

	// Bound concurrent ffmpeg spawns: a client prefetch burst must not fork
	// unbounded processes.  Respect the caller context so a disconnected client
	// releases the slot rather than blocking indefinitely.
	if err := m.acquireTranscodeSlot(ctx); err != nil {
		return "", err
	}
	defer m.releaseTranscodeSlot()

	start := float64(n) * segDur
	dur := segDur
	if sessionDur > 0 {
		if rem := sessionDur - start; rem > 0 && rem < dur {
			dur = rem
		}
	}
	tmp := segFile + ".tmp.ts"

	// Downscale target for this segment's video (no-op unless MaxWidth/
	// MaxHeight is configured and the source exceeds it — see
	// computeScaledDims: aspect-preserving, even dimensions, never upscales).
	outW, outH, scaled := computeScaledDims(srcWidth, srcHeight, tc.maxWidth, tc.maxHeight)

	// HDR→SDR tone mapping (zero plan = off: SDR source or knob disabled).
	// The pre-roll trim keeps the expensive chain off the frames the output
	// -ss discards anyway (see tonemapPlan.trimStart); trimStart is set
	// per-attempt inside run(), since preRoll (and so outputSeek) can differ
	// between the first attempt and a longer-pre-roll retry.
	tm := planTonemap(m.tonemap, color)
	// SEC-4: route through the loopback relay instead of handing ffmpeg the
	// real remote URL directly. Computed once and reused by both the
	// hardware and (on fallback) software encode attempts below.
	inputURL, protoWL, rerr := relayInput(s.mediaURL, m.selfBase)
	if rerr != nil {
		return "", fmt.Errorf("hls: transcode %s: %w", filename, rerr)
	}

	// One overall deadline for every ffmpeg run of this segment (HW attempt,
	// SW fallback, and the optional longer-pre-roll retry), equal to the
	// worst case before validation existed: one SegmentTimeout for a
	// software-only encode, two when a hardware attempt precedes the libx264
	// fallback. The retry only gets whatever time is left; if it runs out,
	// the first attempt's output is served (see transcodeChecked).
	useHW := m.enc.isHW && tc.hwEnabled && kind != segAudioOnly
	deadline := m.cfg.SegmentTimeout
	if useHW {
		deadline *= 2
	}
	dctx, dcancel := context.WithTimeout(ctx, deadline)
	defer dcancel()

	job := segmentJob{
		kind:         kind,
		audioIdx:     audioIdx,
		inputURL:     inputURL,
		protoWL:      protoWL,
		start:        start,
		dur:          dur,
		highBitDepth: highBitDepth,
		outW:         outW,
		outH:         outH,
		scaled:       scaled,
		tc:           tc,
		tm:           tm,
	}
	// run executes one transcode with the given encoder and input-seek
	// pre-roll (argv from buildSegmentArgs). enc is either m.enc (hardware
	// attempt) or the libx264 fallback. It returns the first input-failure
	// marker seen on ffmpeg's stderr (see inputErrMarkers).
	run := func(enc hwEncoder, preRoll float64, out string) (string, error) {
		ctx, cancel := context.WithTimeout(dctx, m.cfg.SegmentTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "ffmpeg", m.buildSegmentArgs(job, enc, preRoll, out)...)
		var watch stderrWatch
		cmd.Stderr = &watch
		err := cmd.Run()
		return watch.hit, err
	}

	// encode runs one transcode attempt into out: hardware encode first,
	// falling back to libx264 on any error. If the caller's context is
	// already done when the HW attempt fails, propagate the cancellation
	// directly — do not start a software re-encode for a segment that is no
	// longer needed.
	sw := hwEncoder{codec: "libx264"}
	var hwCanceled bool
	encode := func(preRoll float64, out string) (string, error) {
		if useHW {
			// VAAPI/NVENC/etc. accelerates h264 video encoding only; audio is always SW.
			hit, err := run(m.enc, preRoll, out)
			if err == nil {
				return hit, nil
			}
			_ = os.Remove(out)
			if ctx.Err() != nil {
				hwCanceled = true
				return "", ctx.Err()
			}
			if m.closing.Load() {
				return "", err // shutting down: don't start a software re-encode
			}
			return run(sw, preRoll, out) // transparent software fallback
		}
		return run(sw, preRoll, out)
	}
	// Once shutdown has begun, the only attempt allowed is the first: the
	// longer-pre-roll retry would re-read a source that is going away (a
	// refused retry counts as a failed one, so the first output is served).
	attempts := 0
	encodeOnce := func(preRoll float64, out string) (string, error) {
		attempts++
		if attempts > 1 && m.closing.Load() {
			return "", errShuttingDown
		}
		return encode(preRoll, out)
	}
	// ffmpeg can exit 0 with a segment missing its video or cut short by an
	// input error, so inspect the output before caching it (see segcheck.go).
	// The optional retry writes to its own tmp file so the first attempt's
	// output survives a failed or timed-out retry.
	began := time.Now()
	want := segExpectation(kind, hasVideo, videoEnd, hasAudio, start, dur)
	retryTmp := segFile + ".retry.tmp.ts"
	out, reason, err := transcodeChecked(dctx, filename, start, preRolls, [2]string{tmp, retryTmp}, encodeOnce,
		func(path string) (segVerdict, error) { return inspectSegment(path, want) })
	if err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(retryTmp)
		if hwCanceled {
			return "", err
		}
		return "", fmt.Errorf("hls: transcode %s: %w", filename, classifySegmentError(ctx, dctx, globalRelay, inputURL, began, err))
	}
	if reason == "" && m.closing.Load() {
		// The input may have been cut off under ffmpeg, which can
		// still exit 0 with a short segment: serve it, don't cache it (with
		// persistence on, a cached segment would outlive this process).
		reason = "shutdown in progress"
	}
	p, err := s.installSegment(filename, out, reason)
	if err == nil && reason == "" {
		// Account the freshly cached segment and trim the session cache
		// back under its cap (never touching this segment, see
		// evictSegments).
		now := time.Now()
		if fi, serr := os.Stat(p); serr == nil {
			s.noteSegment(filename, fi.Size(), now)
		}
		s.evictSegments(m.segmentCacheBytes(s), filename, now)
	}
	return p, err
}

// classifySegmentError marks a failed segment transcode whose cause lies
// with the source, so the HTTP layer can tell it from a missing segment:
// types.ErrHLSSegmentTimeout if the relay saw the origin time out or the
// transcode ran past its overall deadline (dctx), types.ErrHLSSourceFailed
// if the relay saw any other origin failure for inputURL since began.
// Anything else, including a caller that went away (ctx), is returned
// unchanged.
func classifySegmentError(ctx, dctx context.Context, rl *mediaRelay, inputURL string, began time.Time, err error) error {
	if ctx.Err() != nil {
		return err
	}
	if f, ok := rl.failureSince(inputURL, began); ok {
		if f.timeout {
			return fmt.Errorf("%w: %s: %w", types.ErrHLSSegmentTimeout, f.reason, err)
		}
		return fmt.Errorf("%w: %s: %w", types.ErrHLSSourceFailed, f.reason, err)
	}
	if errors.Is(dctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: transcode deadline exceeded: %w", types.ErrHLSSegmentTimeout, err)
	}
	return err
}

// segmentJob carries the per-segment inputs of buildSegmentArgs, resolved
// once by transcodeSegment and shared by every attempt at the segment (the
// hardware encode, the libx264 fallback and the longer-pre-roll retry).
type segmentJob struct {
	kind              segKind
	audioIdx          int
	inputURL, protoWL string
	start, dur        float64
	highBitDepth      bool
	outW, outH        int
	scaled            bool
	tc                sessionConfig
	tm                tonemapPlan // trimStart is set per attempt from preRoll
}

// hybridSeek splits a segment's start into the coarse input seek (-ss
// before -i: a fast demuxer seek) and the precise output seek for the
// residual (-ss after -i: decodes from inputSeek and discards frames until
// start). It is much faster than pure output seeking for mid-file segments
// while still landing on the correct frame, provided decoding starts from a
// keyframe at or before start: preRoll (seconds) is the margin that
// guarantees it where the input seek alone doesn't (see segPreRollPlan).
func hybridSeek(start, preRoll float64) (inputSeek, outputSeek float64) {
	inputSeek = math.Max(0, start-math.Max(0, preRoll))
	return inputSeek, start - inputSeek
}

// buildSegmentArgs builds the full ffmpeg argument list for one attempt at
// a segment: enc is m.enc (hardware attempt) or the libx264 fallback,
// preRoll the input-seek margin for this attempt, out the output file.
// Pure (no I/O), so the exact argv is unit-testable.
func (m *hlsManager) buildSegmentArgs(j segmentJob, enc hwEncoder, preRoll float64, out string) []string {
	kind, audioIdx, start, dur := j.kind, j.audioIdx, j.start, j.dur
	inputSeek, outputSeek := hybridSeek(start, preRoll)
	// Per-attempt tone-map plan: trimStart tracks this attempt's own
	// outputSeek, since preRoll (and so outputSeek) can differ between
	// the first attempt and a longer-pre-roll retry (see transcodeChecked).
	rtm := j.tm
	if rtm.enabled() {
		rtm.trimStart = outputSeek
	}

	a := []string{"-hide_banner", "-loglevel", "error", "-y"}

	// Input robustness: re-generate missing timestamps; cap probe overhead.
	a = append(a, "-fflags", "+genpts",
		"-analyzeduration", "2000000", "-probesize", "2000000")

	// Pre-input device flags (VAAPI: global -vaapi_device must precede -i).
	// We never add -hwaccel / -hwaccel_device flags: SW decode is always used
	// (avoids driver compatibility issues with unusual input codecs/formats).
	if enc.codec == "h264_vaapi" && enc.driDevice != "" {
		a = append(a, "-vaapi_device", enc.driDevice)
	}

	// Hybrid seek: coarse input seek (keyframe-aligned) before -i, then
	// fine output seek for the residual after -i.
	if inputSeek > 0 {
		a = append(a, "-ss", ftoa(inputSeek))
	}
	a = append(a, "-protocol_whitelist", j.protoWL, "-i", j.inputURL)
	// Output seek: precise, decodes from inputSeek and discards until start.
	if outputSeek > 0 {
		a = append(a, "-ss", ftoa(outputSeek))
	}

	// Stream mapping.
	switch kind {
	case segMuxed:
		// ? makes each stream optional so video-only files don't error.
		a = append(a, "-map", "0:v:0?", "-map", "0:a:0?")
	case segVideoOnly:
		a = append(a, "-map", "0:v:0", "-an")
	case segAudioOnly:
		// audioIdx is 0-based among audio streams; 0:a:k selects the k-th audio.
		a = append(a, "-map", fmt.Sprintf("0:a:%d", audioIdx), "-vn")
	}

	// ── Video encoding (segMuxed and segVideoOnly) ────────────────────────
	if kind != segAudioOnly {
		a = append(a, videoEncodeArgs(enc.codec, j.highBitDepth, j.outW, j.outH, j.scaled, j.tc, m.cfg, rtm)...)
	}

	// ── Audio encoding (segMuxed and segAudioOnly) ────────────────────────
	// aresample=async=1:first_pts=0 realigns audio to presentation timestamps
	// after a seek; apad pads short final segments to prevent under-runs.
	switch kind {
	case segMuxed:
		a = append(a,
			"-c:a", "aac", "-ac", strconv.Itoa(m.cfg.AudioChannels), "-b:a", m.cfg.AudioBitrate,
			"-af", "aresample=async=1:first_pts=0,apad",
			"-sn", // drop subtitle streams from the output
		)
	case segAudioOnly:
		a = append(a,
			"-c:a", "aac", "-ac", strconv.Itoa(m.cfg.AudioChannels), "-b:a", m.cfg.AudioBitrate,
			"-af", "aresample=async=1:first_pts=0,apad",
		)
	}

	// ── Output mux ────────────────────────────────────────────────────────
	// -output_ts_offset: set the PTS/DTS of the first packet to its position
	//   in the full timeline so the player does not restart at 0.
	// -muxdelay 0:       suppress mpegts muxer buffering jitter.
	// -mpegts_copyts 1:  preserve codec timestamps verbatim in the container.
	// -t dur (output):   CRITICAL for audio paths — terminates the apad filter
	//   which would otherwise pad indefinitely in audio-only segments (no video
	//   reference to signal EOF). Also acts as a safety ceiling for all paths.
	a = append(a,
		"-output_ts_offset", ftoa(start),
		"-muxdelay", "0",
		"-t", ftoa(dur),
		"-mpegts_copyts", "1",
		"-f", "mpegts", out,
	)
	return a
}

// provSeq numbers provisional (served-but-uncached) segment files so
// concurrent or repeated serves of one segment never share a path.
var provSeq atomic.Uint64

// installSegment moves a freshly transcoded tmp file (src) into place for
// filename and returns the path to serve. Any provisional copies left by
// earlier suspicious transcodes of the same segment are removed first (best
// effort: on Windows a copy still being served cannot be deleted and is
// retried next time; session removal deletes whatever remains).
//
// reason == "" installs the segment as the cached <filename>. Otherwise the
// segment is suspicious: it is renamed to a unique
// "<filename>.prov-<n>.tmp.ts" path that is served for this request only —
// neither the cache fast path in transcodeSegment nor HLSFile's name
// parsing ever resolves to it — so the next request transcodes again. The
// ".tmp" in the name keeps it covered by any sweep of stale *.tmp* files.
// The caller must hold the segment's lockFor(filename) lock.
func (s *hlsSession) installSegment(filename, src, reason string) (string, error) {
	segFile := filepath.Join(s.dir, filename)
	s.mu.Lock()
	old := s.provisional[filename]
	delete(s.provisional, filename)
	s.mu.Unlock()
	var keep []string
	for _, p := range old {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			keep = append(keep, p)
		}
	}
	dst := segFile
	if reason != "" {
		dst = fmt.Sprintf("%s.prov-%d.tmp.ts", segFile, provSeq.Add(1))
		keep = append(keep, dst)
	}
	if len(keep) > 0 {
		s.mu.Lock()
		if s.provisional == nil {
			s.provisional = map[string][]string{}
		}
		s.provisional[filename] = append(s.provisional[filename], keep...)
		s.mu.Unlock()
	}
	if err := os.Rename(src, dst); err != nil {
		_ = os.Remove(src)
		return "", err
	}
	return dst, nil
}

// videoEncodeArgs returns the video filter + encoder arguments for one
// segment transcode with the given encoder backend (see transcodeSegment for
// the surrounding input/audio/mux arguments). Kept free of I/O so the exact
// argument lists can be asserted in tests. tm is the tone-mapping plan; its
// zero value reproduces the pre-tone-mapping arguments byte-for-byte.
func videoEncodeArgs(codec string, highBitDepth bool, outW, outH int, scaled bool, tc sessionConfig, cfg HLSConfig, tm tonemapPlan) []string {
	gopStr := strconv.Itoa(videoGOP)
	var a []string
	switch codec {
	case "h264_vaapi":
		// SW decode + GPU encode: robust across codecs and bit-depths.
		// format=nv12 converts 10/12-bit → 8-bit NV12 before hwupload;
		// this is why we do not need explicit -pix_fmt for high-bit-depth.
		// scale_vaapi (when scaled) runs after hwupload, on the GPU frame.
		a = append(a,
			"-vf", buildVideoFilter(codec, true, outW, outH, scaled, tm),
			"-c:v", "h264_vaapi", "-qp", strconv.Itoa(cfg.VAAPIQP),
			"-g", gopStr,
			"-b:v", tc.videoBitrate, "-maxrate", tc.videoMaxrate, "-bufsize", tc.videoBufsize,
		)

	case "h264_nvenc":
		// 10-bit inputs crash NVENC without explicit format conversion.
		if vf := buildVideoFilter(codec, highBitDepth, outW, outH, scaled, tm); vf != "" {
			a = append(a, "-vf", vf)
		}
		a = append(a,
			"-c:v", "h264_nvenc", "-preset", tc.nvencPreset, "-rc", "vbr",
			"-g", gopStr,
			"-b:v", tc.videoBitrate, "-maxrate", tc.videoMaxrate, "-bufsize", tc.videoBufsize,
		)

	case "h264_qsv":
		if vf := buildVideoFilter(codec, highBitDepth, outW, outH, scaled, tm); vf != "" {
			a = append(a, "-vf", vf)
		}
		a = append(a,
			"-c:v", "h264_qsv", "-preset", tc.qsvPreset,
			"-g", gopStr,
			"-b:v", tc.videoBitrate, "-maxrate", tc.videoMaxrate, "-bufsize", tc.videoBufsize,
		)

	case "h264_videotoolbox":
		if vf := buildVideoFilter(codec, highBitDepth, outW, outH, scaled, tm); vf != "" {
			a = append(a, "-vf", vf)
		}
		a = append(a,
			"-c:v", "h264_videotoolbox", "-realtime", "1",
			"-g", gopStr,
			"-b:v", tc.videoBitrate, "-maxrate", tc.videoMaxrate, "-bufsize", tc.videoBufsize,
		)

	case "h264_v4l2m2m":
		if vf := buildVideoFilter(codec, highBitDepth, outW, outH, scaled, tm); vf != "" {
			a = append(a, "-vf", vf)
		}
		a = append(a,
			"-c:v", "h264_v4l2m2m",
			"-g", gopStr,
			"-b:v", tc.videoBitrate, "-maxrate", tc.videoMaxrate, "-bufsize", tc.videoBufsize,
		)

	default: // libx264 — quality-constrained CRF with bitrate ceiling
		a = append(a,
			"-vf", buildVideoFilter("libx264", true, outW, outH, scaled, tm), // ensure 8-bit even for 10-bit inputs
			"-c:v", "libx264", "-preset", tc.x264Preset, "-crf", strconv.Itoa(cfg.X264CRF),
			"-profile:v", "high",
			// sc_threshold=0 disables scene-cut detection so -g is strictly
			// honoured; -keyint_min enforces IDR at every GOP boundary.
			"-sc_threshold", "0",
			"-g", gopStr, "-keyint_min", gopStr,
			"-b:v", tc.videoBitrate, "-maxrate", tc.videoMaxrate, "-bufsize", tc.videoBufsize,
		)
	}
	if tm.enabled() {
		a = append(a, tonemapOutputTags...)
	}
	return a
}

// ── CloseHLS ──────────────────────────────────────────────────────────────────

// CloseHLS stops the background reaper and removes the manager's entire base
// working directory (created fresh per-manager by newHLSBaseDir in newHLS, so
// removing it cannot affect any other manager or process). Safe to call
// multiple times.
//
// With HLSConfig.Persist on (m.persist) the base directory is the stable
// persist dir instead: CloseHLS still stops the reaper and drops the
// in-memory sessions, but first flushes each session's lastAccess to its
// session.json and then leaves everything on disk for the next process.
func (m *hlsManager) CloseHLS() {
	m.closing.Store(true)
	// Signal the reaper to stop; guard against double-close with a non-blocking
	// drain: if stopCh is already closed the receive arm fires immediately.
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
	if m.persist {
		m.reaperWG.Wait()
		m.flushAccess()
	}
	m.mu.Lock()
	m.sessions = map[string]*hlsSession{}
	m.draining = nil
	lock := m.persistLock
	m.persistLock = nil
	m.mu.Unlock()
	if m.persist {
		if lock != nil {
			_ = lock.Close()
		}
		return
	}
	// RemoveAll outside the lock: disk I/O must not stall concurrent requests.
	_ = os.RemoveAll(m.base)
}

// BeginShutdown marks the manager as shutting down, before the HTTP servers
// (which serve ffmpeg its input for torrent sources) are drained: segment
// transcodes still in flight finish and are served, but none is cached and
// none starts another ffmpeg attempt (see closing). Safe to call more than
// once; CloseHLS implies it.
func (m *hlsManager) BeginShutdown() { m.closing.Store(true) }

// errShuttingDown refuses a further ffmpeg attempt once BeginShutdown ran.
var errShuttingDown = errors.New("hls: shutting down")

// ── DeleteHLS ─────────────────────────────────────────────────────────────────

// DeleteHLS explicitly ends session id, for front ends that manage their own
// session lifecycle instead of relying on the idle reaper (and the only way
// to end a session when idle eviction is disabled, RIVULET_HLS_SESSION_TTL=0).
//
// The session is removed from m.sessions immediately, so every later HLSFile
// call gets "unknown session" and a later StartHLS starts from scratch. Its
// directory, however, may still be in use by an HLSFile call already past
// the map lookup (e.g. ffmpeg writing seg<n>.ts.tmp.ts), so the RemoveAll is
// deferred until inFlight drops to 0:
//
//   - HLSFile only increments inFlight while holding m.mu *and* finding the
//     session in m.sessions. DeleteHLS unregisters it under m.mu, so once
//     DeleteHLS releases m.mu inFlight can only go down.
//   - DeleteHLS sets deleted before loading inFlight; HLSFile's deferred exit
//     decrements inFlight before loading deleted. Both are sequentially
//     consistent atomics, so at least one side sees the other's write: either
//     DeleteHLS reads inFlight == 0 (and removes the directory itself), or
//     the exit that takes inFlight to 0 reads deleted == true (and removes
//     it). Both may see it, hence removeDeletedSession's once-only guard.
//
// Until the directory is gone the id stays in m.draining, which makes
// StartHLS refuse to re-create it: a new session would reuse the same
// <base>/<id> directory and be wiped by the pending RemoveAll.
//
// Returns an error wrapping types.ErrHLSInvalidSessionID for an id StartHLS
// would reject, or types.ErrHLSSessionNotFound when no such session exists.
//
// removeDeletedSession's os.RemoveAll(s.dir) removes the session's entire
// directory, including any session.json written by persistSession (see
// persist.go): a deleted session is therefore never rehydrated by a later
// restart, whether persistence is on or off.
func (m *hlsManager) DeleteHLS(id string) error {
	if !validSessionID(id) {
		return fmt.Errorf("hls: %w %q", types.ErrHLSInvalidSessionID, id)
	}
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("hls: %w %s", types.ErrHLSSessionNotFound, id)
	}
	delete(m.sessions, id)
	if m.draining == nil {
		m.draining = map[string]*hlsSession{}
	}
	m.draining[id] = s
	s.deleted.Store(true)
	idle := s.inFlight.Load() == 0
	m.mu.Unlock()

	if idle {
		m.removeDeletedSession(id, s)
	} else {
		logging.For("media").Debug("hls session deleted; removal deferred until in-flight requests finish", "id", id)
	}
	return nil
}

// removeDeletedSession removes a session's directory after DeleteHLS, exactly
// once (the removed CAS), then drops it from m.draining so StartHLS may reuse
// the id. The RemoveAll happens before the draining entry is cleared, so no
// new session for the same id can create its directory in between.
func (m *hlsManager) removeDeletedSession(id string, s *hlsSession) {
	if !s.removed.CompareAndSwap(false, true) {
		return
	}
	if err := os.RemoveAll(s.dir); err != nil {
		logging.For("media").Warn("hls session dir removal failed", "id", id, "err", err)
	}
	m.mu.Lock()
	if m.draining[id] == s {
		delete(m.draining, id)
	}
	m.mu.Unlock()
}

// startReaper launches the reaper goroutine, tracked by reaperWG.
func (m *hlsManager) startReaper() {
	m.reaperWG.Add(1)
	go func() {
		defer m.reaperWG.Done()
		m.reaper()
	}()
}

// reaper is the single background goroutine that evicts idle HLS sessions.
// It runs until CloseHLS closes stopCh, so it never leaks.
func (m *hlsManager) reaper() {
	ticker := time.NewTicker(m.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.evictIdle()
			if m.persist {
				m.flushAccess()
			}
		case <-m.stopCh:
			return
		}
	}
}

// evictIdle removes sessions whose lastAccess timestamp is older than their
// idle TTL AND that have no work currently in flight. The idle TTL is the
// session's own ?ttl= override when set (hlsSession.ttl), else
// HLSConfig.SessionTTL. Eviction predicate:
//
//	inFlight == 0  &&  lastAccess != 0  &&  lastAccess < now-ttl
//
// Eviction only happens on reaper ticks, so a session is removed somewhere
// between ttl and ttl+ReaperInterval after its last access. The reaper
// interval is derived from the global TTL only (min(30s, SessionTTL/2)), so
// a per-session TTL shorter than the global one (possible only when the
// global TTL is above the 60s override floor) is honoured with up to one
// reaper interval (<= 30s unless RIVULET_HLS_REAPER_INTERVAL raises it) of
// extra slack; a longer one simply survives more ticks.
//
// A session with inFlight > 0 (an HLSFile call — segment transcode, subtitle
// extraction, or playlist write — is still running) is never evicted no
// matter how stale lastAccess looks: deleting it mid-write would RemoveAll
// the directory ffmpeg is actively writing seg<n>.ts.tmp.ts into, breaking
// the in-flight request and orphaning the session id ("unknown session") for
// every subsequent request. Deleting from a map during range is safe and
// defined in Go.
//
// HLSConfig.DisableIdleEviction (RIVULET_HLS_SESSION_TTL=0) turns off idle
// eviction for sessions relying on the global TTL, but a session carrying
// its own per-session ?ttl= override is still evicted on schedule: an
// explicit per-session ttl wins over the global disable flag, consistent
// with the documented override > /settings > env precedence in
// effectiveSessionConfig. The reaper keeps running in this mode (its 30s-ish
// tick doesn't depend on DisableIdleEviction), so a per-session ttl is
// always honoured even when the process-wide TTL is 0.
func (m *hlsManager) evictIdle() {
	now := time.Now()
	type victim struct {
		id string
		s  *hlsSession
	}
	var victims []victim
	m.mu.Lock()
	for id, s := range m.sessions {
		if s.inFlight.Load() > 0 {
			// Work is actively running for this session; skip regardless of
			// how old lastAccess is. HLSFile refreshes lastAccess again on
			// exit, so once the work finishes the session gets a fresh idle
			// window before the next reaper pass can evict it.
			continue
		}
		ts := s.lastAccess.Load()
		if ts == 0 {
			// Session created but not yet accessed (e.g. in the window between
			// map insertion and the first lastAccess.Store); skip to be safe.
			continue
		}
		perSessionTTL := s.ttl.Load() > 0
		if m.cfg.DisableIdleEviction && !perSessionTTL {
			// RIVULET_HLS_SESSION_TTL=0: idle eviction is off for sessions
			// that rely on the global TTL. A session carrying its own
			// ?ttl= override still wins (per-session override > env,
			// matching effectiveSessionConfig's precedence) and is still
			// evicted below.
			continue
		}
		if time.Unix(0, ts).Before(now.Add(-s.idleTTL(m.cfg.SessionTTL))) {
			// Park the id in m.draining (like DeleteHLS) so a same-id
			// StartHLS between here and the RemoveAll below is refused
			// instead of having its fresh directory wiped.
			delete(m.sessions, id)
			if m.draining == nil {
				m.draining = map[string]*hlsSession{}
			}
			m.draining[id] = s
			s.deleted.Store(true)
			victims = append(victims, victim{id: id, s: s})
		}
	}
	m.mu.Unlock()
	// RemoveAll outside the lock: disk I/O must not stall concurrent requests.
	// removeDeletedSession clears the draining entry only after the
	// directory is gone.
	for _, v := range victims {
		m.removeDeletedSession(v.id, v.s)
	}
}

// sweepProbeCache removes expired entries from the negative probe cache and,
// when the size still exceeds probeCacheMaxSize after the TTL sweep, evicts
// the entry with the smallest remaining TTL.  Must be called with m.mu held.
func (m *hlsManager) sweepProbeCache() {
	now := time.Now()
	for k, e := range m.probeCache {
		if now.After(e.expiresAt) {
			delete(m.probeCache, k)
		}
	}
	// Hard size cap: evict soonest-expiring entry until under limit.
	for len(m.probeCache) > probeCacheMaxSize {
		var evict string
		var evictExp time.Time
		for k, e := range m.probeCache {
			if evict == "" || e.expiresAt.Before(evictExp) {
				evict = k
				evictExp = e.expiresAt
			}
		}
		delete(m.probeCache, evict)
	}
}

// g returns the probe guard in effect: the prober's shared one when set.
func (m *hlsManager) g() *probeGuard {
	if m.shared != nil {
		return m.shared
	}
	return &m.guard
}

// runProbeCtx probes mediaURL through the m.probe seam (probeMedia by
// default), bounded by ctx.
func (m *hlsManager) runProbeCtx(ctx context.Context, mediaURL string) probeMediaResult {
	probe := m.probe
	if probe == nil {
		probe = probeMedia
	}
	return probe(ctx, mediaURL, m.selfBase, m.cfg.ProbeTimeout)
}

// validSessionID reports whether id is safe to use as a directory name under
// the manager's base dir: non-empty, a single path element, and free of ".."
// anywhere. Used by StartHLS for client-supplied ids and by loadPersisted for
// directory names found on disk.
func validSessionID(id string) bool {
	return id != "" && id != "." && id != ".." && id == filepath.Base(id) && !strings.Contains(id, "..")
}

func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', 3, 64) }

// --- types.MediaProber HLS methods (delegate to the manager) ---

func (p *prober) StartHLS(id, mediaURL string, opts types.HLSSessionOptions) (string, error) {
	return p.hls.StartHLS(id, mediaURL, opts)
}

// StartHLSContext is StartHLS with caller cancellation (see
// hlsManager.StartHLSContext).
func (p *prober) StartHLSContext(ctx context.Context, id, mediaURL string, opts types.HLSSessionOptions) (string, error) {
	return p.hls.StartHLSContext(ctx, id, mediaURL, opts)
}
func (p *prober) HLSFile(ctx context.Context, id, name string) (string, string, error) {
	return p.hls.HLSFile(ctx, id, name)
}
func (p *prober) DeleteHLS(id string) error { return p.hls.DeleteHLS(id) }

// CloseHLS stops the background session reaper and removes all HLS working
// directories (kept on disk when HLSConfig.Persist is active).  Not part of
// types.MediaProber; call directly on shutdown.
func (p *prober) CloseHLS() { p.hls.CloseHLS() }

// BeginShutdown tells the HLS manager that shutdown has begun (see
// hlsManager.BeginShutdown). Not part of types.MediaProber; call directly
// before draining the HTTP servers.
func (p *prober) BeginShutdown() { p.hls.BeginShutdown() }

// Sessions returns the number of currently active HLS transcode sessions.
func (m *hlsManager) Sessions() int {
	m.mu.Lock()
	n := len(m.sessions)
	m.mu.Unlock()
	return n
}

// HLSSessions returns the number of active HLS transcode sessions.
// Satisfies the interface checked by handleMetrics via structural assertion.
func (p *prober) HLSSessions() int { return p.hls.Sessions() }
