// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// HLSConfig holds every runtime-tunable knob for the HLS transcode manager
// (issue #20). Every field defaults to the value that was previously a
// compile-time constant, so DefaultHLSConfig() reproduces today's behaviour
// exactly. Values are resolved once by internal/app (via Lookup — env vars
// and, in library mode, the envJSON map) and handed to New()/newHLS() as a
// plain struct; internal/media never calls os.Getenv itself.
type HLSConfig struct {
	// --- session lifecycle ---
	SessionTTL     time.Duration // idle-eviction window (RIVULET_HLS_SESSION_TTL)
	ReaperInterval time.Duration // reaper scan period (RIVULET_HLS_REAPER_INTERVAL)
	NegProbeTTL    time.Duration // failed-probe negative cache TTL (RIVULET_HLS_NEG_PROBE_TTL)
	PosProbeTTL    time.Duration // successful-probe cache TTL (RIVULET_HLS_POS_PROBE_TTL)
	MaxSessions    int           // hard cap on concurrent sessions (RIVULET_HLS_MAX_SESSIONS)
	WorkDir        string        // "" = os.MkdirTemp default; else rivulet-engine-hls-* under it (RIVULET_HLS_WORK_DIR)
	// DisableIdleEviction turns idle eviction off entirely: the reaper never
	// removes a session for being idle, so sessions live until DeleteHLS (or
	// CloseHLS). internal/app sets it for RIVULET_HLS_SESSION_TTL=0, matching
	// RIVULET_TORRENT_IDLE_TIMEOUT=0. A separate flag rather than
	// SessionTTL==0 because normalize treats a zero SessionTTL as "unset" (a
	// caller-built HLSConfig literal), which must keep meaning the 60s default.
	// An explicit per-session ttl (see SessionOverrides) still wins over this
	// flag: evictIdle evicts a session that carries its own ttl even while
	// global idle eviction is disabled — see evictIdle.
	DisableIdleEviction bool
	// SessionOverrides opts in to the per-session query overrides (ttl,
	// maxWidth, maxHeight, bitrate, maxRate, bufSize) on GET
	// /hlsv2/{id}/master.m3u8 (RIVULET_HLS_SESSION_OVERRIDES). false (the
	// default) ignores those parameters entirely — see parseSessionOverrides.
	SessionOverrides bool
	// Persist opts in to HLS sessions that survive a restart: sessions live
	// in a stable <WorkDir>/rivulet-engine-hls-persist directory (instead of a fresh
	// random rivulet-engine-hls-* one), each with a session.json that lets the next
	// process rehydrate it without re-probing or re-transcoding. Requires
	// WorkDir; ignored (with a warning) without it. See persist.go.
	Persist bool // (RIVULET_HLS_PERSIST)
	// Prepare opts in to engine-side preparation: POST /hlsv2/{id}/prepare
	// starts a background job that transcodes every segment of a session
	// at low priority and pins them in its cache, with progress and
	// per-segment cache status on GET (RIVULET_HLS_PREPARE). false (the
	// default) leaves those routes answering 404 exactly as before. See
	// prepare.go.
	Prepare bool
	// PrepareConcurrency is the most segment transcodes preparation runs at
	// once across every session (RIVULET_HLS_PREPARE_CONCURRENCY). They share
	// the SegmentConcurrency slots with live requests but only take one
	// while no live request is waiting for it. 0 resolves to 1.
	PrepareConcurrency int

	// --- output quality / bandwidth ---
	VideoBitrate string // -b:v, e.g. "8M" (RIVULET_TRANSCODE_VIDEO_BITRATE)
	VideoMaxrate string // -maxrate (RIVULET_TRANSCODE_MAXRATE)
	VideoBufsize string // -bufsize (RIVULET_TRANSCODE_BUFSIZE)
	MaxWidth     int    // 0 = no downscale (RIVULET_TRANSCODE_MAX_WIDTH)
	MaxHeight    int    // 0 = no downscale (RIVULET_TRANSCODE_MAX_HEIGHT)

	// --- encoder tuning ---
	VAAPIQP     int    // -qp for h264_vaapi (RIVULET_TRANSCODE_VAAPI_QP)
	NVENCPreset string // -preset for h264_nvenc (RIVULET_TRANSCODE_NVENC_PRESET)
	QSVPreset   string // -preset for h264_qsv (RIVULET_TRANSCODE_QSV_PRESET)
	X264Preset  string // -preset for libx264 (RIVULET_TRANSCODE_X264_PRESET)
	X264CRF     int    // -crf for libx264 (RIVULET_TRANSCODE_X264_CRF)

	// Tonemap is the tonemap filter algorithm used to convert HDR (PQ/HLG)
	// sources to SDR BT.709 ("hable", "mobius", …; see TonemapAlgorithms).
	// "" disables tone mapping, which is the default and today's behaviour
	// (RIVULET_TRANSCODE_TONEMAP).
	Tonemap string

	// --- audio ---
	AudioChannels int    // -ac (RIVULET_TRANSCODE_AUDIO_CHANNELS)
	AudioBitrate  string // -b:a (RIVULET_TRANSCODE_AUDIO_BITRATE)

	// --- concurrency / hardware ---
	SegmentConcurrency int    // concurrent ffmpeg segment jobs; 0 resolves to runtime.NumCPU() (RIVULET_TRANSCODE_CONCURRENCY)
	VAAPIDevice        string // renderD* device path (RIVULET_HLS_VAAPI_DEVICE)

	// --- segment cache ---
	// SegmentCacheBytes caps the transcoded-segment bytes kept on disk per
	// session; the least recently used segments are deleted beyond it. 0 (the
	// default) derives the cap from the session's max bitrate (~20 minutes of
	// media, see segmentCacheBytes); a negative value disables the cap
	// (RIVULET_HLS_SEGMENT_CACHE_BYTES).
	SegmentCacheBytes int64

	// --- timeouts ---
	SegmentTimeout  time.Duration // per-segment transcode (RIVULET_HLS_SEGMENT_TIMEOUT)
	SubtitleTimeout time.Duration // subtitle extraction (RIVULET_HLS_SUBTITLE_TIMEOUT)
	ProbeTimeout    time.Duration // ffprobe combined probe (RIVULET_HLS_PROBE_TIMEOUT)

	// --- seeking ---
	// SeekPreroll is the input-seek pre-roll transcodeSegment decodes ahead
	// of each segment's start for sources in an indexed container (Matroska,
	// MP4; see seekIndexed), where ffmpeg's input seek is already
	// frame-accurate when re-encoding (RIVULET_HLS_SEEK_PREROLL). It can
	// only shrink the margin: every other source (MPEG-TS, HLS, FLV, raw
	// streams, anything unrecognised) keeps the full defaultSeekPreroll,
	// because its input seek can start decoding at the keyframe *after* the
	// seek point (see segPreRollPlan).
	//
	// Follows net.Dialer.KeepAlive's convention so a zero-value HLSConfig
	// still means "all defaults": 0 = default (10s, today's margin for every
	// container), negative (SeekPrerollNone) = no pre-roll, i.e. a pure
	// input seek. normalize clamps values above MaxSeekPreroll and truncates
	// the rest to whole milliseconds.
	SeekPreroll time.Duration

	// --- network ---
	// PrivateAllow lists the private hosts (IPs, CIDRs, hostnames) a media
	// URL may point at: /probe, /tracks and /hlsv2 mediaURL, checked before
	// the session starts and again by the relay on every address it dials
	// (RIVULET_HLS_PRIVATE_ALLOW). nil, the default, allows none. Subtitle
	// and /opensubHash fetches never use it. Cloud-metadata is always
	// refused.
	PrivateAllow *netguard.Allow
}

// defaultPrepareConcurrency keeps preparation to one background transcode at
// a time, so live sessions keep the rest of the slots.
const defaultPrepareConcurrency = 1

// SeekPrerollNone is the HLSConfig.SeekPreroll value meaning "no pre-roll"
// (pure input seek). A literal 0 cannot carry that meaning because 0 is the
// zero-value "unset, use the default" marker every other HLSConfig field
// uses.
const SeekPrerollNone time.Duration = -1

// MaxSeekPreroll caps HLSConfig.SeekPreroll at the default margin: the knob
// only ever shrinks the pre-roll for indexed containers.
const MaxSeekPreroll = defaultSeekPreroll

// Historical compile-time defaults, kept as named constants so
// DefaultHLSConfig and the default-compatibility check in
// deriveBandwidthCodecs share a single source of truth.
const (
	defaultSessionTTL     = 60 * time.Second
	defaultReaperInterval = 30 * time.Second
	defaultNegProbeTTL    = 5 * time.Minute
	defaultPosProbeTTL    = 10 * time.Minute
	defaultMaxSessions    = 64
	defaultVideoBitrate   = "8M"
	defaultVideoMaxrate   = "8M"
	defaultVideoBufsize   = "16M"
	defaultVAAPIQP        = 23
	defaultNVENCPreset    = "p4"
	defaultQSVPreset      = "veryfast"
	defaultX264Preset     = "veryfast"
	defaultX264CRF        = 23
	defaultAudioChannels  = 2
	defaultAudioBitrate   = "192k"
	defaultVAAPIDevice    = "/dev/dri/renderD128"
	defaultSegmentTimeout = 120 * time.Second
	defaultSubtitleTTL    = 120 * time.Second
	defaultProbeTimeout   = 30 * time.Second
	defaultSeekPreroll    = 10 * time.Second

	// legacyMaxrateBps is defaultVideoMaxrate ("8M") parsed to bits/second.
	// Compared against verbatim in deriveBandwidthCodecs to detect the fully
	// unconfigured case.
	legacyMaxrateBps int64 = 8_000_000
	// legacyBandwidth/legacyCodecsVideo are the exact master-playlist values
	// this server has always advertised (see hls.go StartHLS history). They
	// must keep being emitted byte-for-byte whenever nothing about the
	// transcode configuration has changed, regardless of the actual source
	// resolution — deriving purely mathematically would change the string
	// for the many real sources that aren't exactly 1920x1080/8Mbps.
	legacyBandwidth   int64  = 4_000_000
	legacyCodecsVideo string = "avc1.640029" // H.264 Level 4.1
)

// DefaultHLSConfig returns the config that reproduces every previously
// hardcoded default byte-for-byte. SegmentConcurrency is left at 0 (meaning
// "resolve to runtime.NumCPU() at construction") since that resolution needs
// the runtime package, which callers already have.
func DefaultHLSConfig() HLSConfig {
	return HLSConfig{
		SessionTTL:         defaultSessionTTL,
		ReaperInterval:     defaultReaperInterval,
		NegProbeTTL:        defaultNegProbeTTL,
		PosProbeTTL:        defaultPosProbeTTL,
		MaxSessions:        defaultMaxSessions,
		WorkDir:            "",
		SessionOverrides:   false,
		Persist:            false,
		VideoBitrate:       defaultVideoBitrate,
		VideoMaxrate:       defaultVideoMaxrate,
		VideoBufsize:       defaultVideoBufsize,
		MaxWidth:           0,
		MaxHeight:          0,
		VAAPIQP:            defaultVAAPIQP,
		NVENCPreset:        defaultNVENCPreset,
		QSVPreset:          defaultQSVPreset,
		X264Preset:         defaultX264Preset,
		X264CRF:            defaultX264CRF,
		AudioChannels:      defaultAudioChannels,
		AudioBitrate:       defaultAudioBitrate,
		SegmentConcurrency: 0,
		PrepareConcurrency: defaultPrepareConcurrency,
		VAAPIDevice:        defaultVAAPIDevice,
		SegmentTimeout:     defaultSegmentTimeout,
		SubtitleTimeout:    defaultSubtitleTTL,
		ProbeTimeout:       defaultProbeTimeout,
		SeekPreroll:        defaultSeekPreroll,
	}
}

// DefaultReaperInterval derives the reaper's default scan interval from the
// session idle-eviction TTL: min(30s, ttl/2). Exported so internal/app can
// compute the default before applying RIVULET_HLS_REAPER_INTERVAL as an
// explicit override (issue #20: "Should probably scale with the TTL above"),
// giving the derivation a single, unit-tested home instead of duplicating it
// in internal/app.
//
// A zero (or negative) ttl yields the historical 30s rather than 0: ttl==0
// is how RIVULET_HLS_SESSION_TTL=0 ("never evict", see
// HLSConfig.DisableIdleEviction) reaches here, and a 0 interval would make
// time.NewTicker panic, while a tiny one would spin the reaper.
func DefaultReaperInterval(ttl time.Duration) time.Duration {
	half := ttl / 2
	if half <= 0 || half > defaultReaperInterval {
		return defaultReaperInterval
	}
	return half
}

// normalize fills in any zero-value fields left unset by a caller-built
// HLSConfig (e.g. a test constructing a literal) with DefaultHLSConfig's
// values, and resolves SegmentConcurrency=0 to numCPU. Called once by
// newHLS(); every hlsManager method reads a fully-resolved m.cfg afterward.
func (c HLSConfig) normalize(numCPU int) HLSConfig {
	d := DefaultHLSConfig()
	if c.SessionTTL <= 0 {
		c.SessionTTL = d.SessionTTL
	}
	if c.ReaperInterval <= 0 {
		c.ReaperInterval = DefaultReaperInterval(c.SessionTTL)
	}
	if c.NegProbeTTL <= 0 {
		c.NegProbeTTL = d.NegProbeTTL
	}
	if c.PosProbeTTL <= 0 {
		c.PosProbeTTL = d.PosProbeTTL
	}
	if c.MaxSessions <= 0 {
		c.MaxSessions = d.MaxSessions
	}
	if c.VideoBitrate == "" {
		c.VideoBitrate = d.VideoBitrate
	}
	if c.VideoMaxrate == "" {
		c.VideoMaxrate = d.VideoMaxrate
	}
	if c.VideoBufsize == "" {
		c.VideoBufsize = d.VideoBufsize
	}
	if c.VAAPIQP <= 0 {
		c.VAAPIQP = d.VAAPIQP
	}
	if c.NVENCPreset == "" {
		c.NVENCPreset = d.NVENCPreset
	}
	if c.QSVPreset == "" {
		c.QSVPreset = d.QSVPreset
	}
	if c.X264Preset == "" {
		c.X264Preset = d.X264Preset
	}
	if c.X264CRF <= 0 {
		c.X264CRF = d.X264CRF
	}
	if c.AudioChannels <= 0 {
		c.AudioChannels = d.AudioChannels
	}
	if c.AudioBitrate == "" {
		c.AudioBitrate = d.AudioBitrate
	}
	if c.SegmentConcurrency <= 0 {
		if numCPU < 1 {
			numCPU = 1
		}
		c.SegmentConcurrency = numCPU
	}
	if c.VAAPIDevice == "" {
		c.VAAPIDevice = d.VAAPIDevice
	}
	if c.PrepareConcurrency <= 0 {
		c.PrepareConcurrency = defaultPrepareConcurrency
	}
	if c.SegmentTimeout <= 0 {
		c.SegmentTimeout = d.SegmentTimeout
	}
	if c.SubtitleTimeout <= 0 {
		c.SubtitleTimeout = d.SubtitleTimeout
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = d.ProbeTimeout
	}
	switch {
	case c.SeekPreroll == 0:
		c.SeekPreroll = d.SeekPreroll
	case c.SeekPreroll < 0:
		c.SeekPreroll = SeekPrerollNone // canonicalize any negative value
	case c.SeekPreroll > MaxSeekPreroll:
		c.SeekPreroll = MaxSeekPreroll
	default:
		// ffmpeg's -ss (and outputFingerprint) carry milliseconds; a
		// positive value below 1ms is no pre-roll.
		if c.SeekPreroll = c.SeekPreroll.Truncate(time.Millisecond); c.SeekPreroll == 0 {
			c.SeekPreroll = SeekPrerollNone
		}
	}
	if c.Tonemap != "" && !IsTonemapAlgorithm(c.Tonemap) {
		c.Tonemap = "" // unknown algorithm from a library caller: stay off
	}
	// MaxWidth/MaxHeight/WorkDir/Persist: 0/""/false is a meaningful value
	// (no downscale / OS-default temp dir / no persistence), not "unset" —
	// left as provided.
	return c
}

// SettingsSource is the minimal surface hlsManager needs from
// types.SettingsStore to apply live /settings overrides at session-creation
// time (issue #20 "Related": transcodeMaxBitRate, transcodeMaxWidth,
// transcodeConcurrency, transcodeHardwareAccel, transcodeProfile).
// types.SettingsStore satisfies this structurally, so callers pass it
// directly without internal/media importing anything beyond what it already
// does.
type SettingsSource interface {
	Get(key string) interface{}
}

// settingsInt reads key as an int, accepting the float64/int/int64 shapes
// Get may return depending on whether the value came from json.Unmarshal (a
// loaded server-settings.json) or a fresh in-memory default.
func settingsInt(ss SettingsSource, key string) (int, bool) {
	if ss == nil {
		return 0, false
	}
	switch v := ss.Get(key).(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case int64:
		return int(v), true
	}
	return 0, false
}

func settingsBool(ss SettingsSource, key string) (bool, bool) {
	if ss == nil {
		return false, false
	}
	if v, ok := ss.Get(key).(bool); ok {
		return v, true
	}
	return false, false
}

func settingsString(ss SettingsSource, key string) (string, bool) {
	if ss == nil {
		return "", false
	}
	if v, ok := ss.Get(key).(string); ok && v != "" {
		return v, true
	}
	return "", false
}

// sessionConfig is the subset of HLSConfig that can vary per session because
// a live /settings value overrides it. Snapshotted once in StartHLS (session
// creation) so an already-open session keeps behaving consistently even if
// /settings changes again while it's playing, matching how transcodeSegment
// already treats every other per-session field as immutable after creation.
type sessionConfig struct {
	videoBitrate string
	videoMaxrate string
	videoBufsize string
	maxrateBps   int64 // parsed bits/second, used for BANDWIDTH derivation
	maxWidth     int
	maxHeight    int
	hwEnabled    bool
	x264Preset   string
	nvencPreset  string
	qsvPreset    string
	// overridden is true when a per-session quality override (bitrate,
	// maxrate, bufsize, maxWidth or maxHeight — not ttl) was applied. It
	// takes the session off deriveBandwidthCodecs's legacy-compatibility
	// path: a caller who explicitly asked for a quality gets BANDWIDTH/CODECS
	// derived from what it asked for, even if that happens to equal the
	// historical defaults.
	overridden bool
}

// x264PresetOrder lists every libx264 preset name from fastest/lowest-quality
// to slowest/highest-quality. transcodeProfile is validated in api.go only as
// "a string or null" (no fixed enum), so these are the values this server
// treats as recognized profiles — matching libx264's own preset names keeps
// the setting portable/self-documenting instead of inventing new vocabulary.
var x264PresetOrder = []string{
	"ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow",
}

// nvencPresetForX264 maps a libx264 preset name to the closest NVENC preset,
// so a single transcodeProfile value drives both software and NVENC hardware
// encodes consistently. "veryfast" (the default profile) maps to "p4" — the
// existing NVENC default — so leaving transcodeProfile unset changes nothing.
func nvencPresetForX264(x264Preset string) string {
	switch x264Preset {
	case "ultrafast", "superfast":
		return "p1"
	case "veryfast":
		return "p4"
	case "faster":
		return "p3"
	case "fast":
		return "p4"
	case "medium":
		return "p5"
	case "slow":
		return "p6"
	case "slower", "veryslow":
		return "p7"
	default:
		return defaultNVENCPreset
	}
}

// qsvPresetForX264 maps a libx264 preset name to the closest h264_qsv
// preset, mirroring nvencPresetForX264 so one transcodeProfile value drives
// every encoder consistently. h264_qsv accepts the x264 names from
// veryfast down to veryslow, so those pass through unchanged;
// ultrafast/superfast (no QSV equivalent) map to veryfast, the existing QSV
// default, so leaving transcodeProfile unset (or at "veryfast") changes
// nothing.
func qsvPresetForX264(x264Preset string) string {
	switch x264Preset {
	case "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow":
		return x264Preset
	default: // ultrafast, superfast, or anything unrecognized
		return defaultQSVPreset
	}
}

// isKnownX264Preset reports whether profile is one of x264PresetOrder.
func isKnownX264Preset(profile string) bool {
	for _, p := range x264PresetOrder {
		if p == profile {
			return true
		}
	}
	return false
}

// effectiveSessionConfig computes the per-session transcode configuration at
// session-creation time (StartHLS), applying live /settings overrides on top
// of m.cfg's env-resolved defaults, then the session's own query overrides
// (ov; zero value when RIVULET_HLS_SESSION_OVERRIDES is off or none were
// given) on top of both. Overall precedence: per-session override >
// /settings > env. Per key:
//
//   - transcodeMaxBitRate (settings default 0 = "unset"): overrides
//     videoBitrate/videoMaxrate/videoBufsize (bufsize = 2x) when > 0.
//   - transcodeMaxWidth (settings default 1920 — already the schema's
//     baked-in fresh-install value, NOT a sentinel): overrides MaxWidth only
//     when > 0 AND != 1920, so a server whose /settings was never touched
//     keeps today's uncapped-resolution output byte-for-byte. An operator who
//     wants exactly 1920 as an intentional cap should set
//     RIVULET_TRANSCODE_MAX_WIDTH=1920 instead — indistinguishable from
//     "untouched" is the unavoidable cost of the byte-identical-default
//     requirement, since the settings store cannot tell "user re-saved 1920"
//     from "user never saved anything".
//   - transcodeHardwareAccel (settings default true): false forces software
//     encoding for this session; true (default) preserves the existing
//     auto-detected m.enc behaviour unchanged.
//   - transcodeProfile (settings default nil): a recognized libx264 preset
//     name overrides X264Preset and, mapped to each encoder's closest
//     preset, NVENCPreset and QSVPreset (see nvencPresetForX264,
//     qsvPresetForX264). An unrecognized name is ignored for all three.
//   - per-session bitrate (?bitrate=): overrides videoBitrate and, unless
//     also given explicitly, videoMaxrate (= bitrate) and videoBufsize
//     (= 2x bitrate) — mirroring transcodeMaxBitRate's derivation — beating
//     both transcodeMaxBitRate and the RIVULET_TRANSCODE_* env values.
//   - per-session maxRate/bufSize (?maxRate=, ?bufSize=): override just that
//     value (maxrate also drives BANDWIDTH); a lone maxRate/bufSize leaves
//     the /settings- or env-resolved bitrate alone.
//   - per-session maxWidth/maxHeight (?maxWidth=, ?maxHeight=): override
//     transcodeMaxWidth and RIVULET_TRANSCODE_MAX_WIDTH/HEIGHT for that
//     axis; the other axis keeps its /settings/env value.
//   - per-session ttl is not a transcode knob; StartHLS stores it on the
//     session itself (hlsSession.ttl) for evictIdle.
func (m *hlsManager) effectiveSessionConfig(ov sessionOverrides) sessionConfig {
	cfg := m.cfg
	sc := sessionConfig{
		videoBitrate: cfg.VideoBitrate,
		videoMaxrate: cfg.VideoMaxrate,
		videoBufsize: cfg.VideoBufsize,
		maxWidth:     cfg.MaxWidth,
		maxHeight:    cfg.MaxHeight,
		hwEnabled:    true,
		x264Preset:   cfg.X264Preset,
		nvencPreset:  cfg.NVENCPreset,
		qsvPreset:    cfg.QSVPreset,
	}
	sc.maxrateBps, _ = parseDecimalBitrate(cfg.VideoMaxrate)

	if bps, ok := settingsInt(m.settings, "transcodeMaxBitRate"); ok && bps > 0 {
		v := int64(bps)
		sc.maxrateBps = v
		bpsStr := strconv.FormatInt(v, 10)
		sc.videoBitrate = bpsStr
		sc.videoMaxrate = bpsStr
		sc.videoBufsize = strconv.FormatInt(v*2, 10)
	}

	if w, ok := settingsInt(m.settings, "transcodeMaxWidth"); ok && w > 0 && w != 1920 {
		sc.maxWidth = w
	}

	if hw, ok := settingsBool(m.settings, "transcodeHardwareAccel"); ok {
		sc.hwEnabled = hw
	}

	if profile, ok := settingsString(m.settings, "transcodeProfile"); ok {
		profile = strings.ToLower(strings.TrimSpace(profile))
		if isKnownX264Preset(profile) {
			sc.x264Preset = profile
			sc.nvencPreset = nvencPresetForX264(profile)
			sc.qsvPreset = qsvPresetForX264(profile)
		}
	}

	// Per-session overrides last, so they win over /settings and env.
	if ov.videoBitrate > 0 {
		sc.videoBitrate = strconv.FormatInt(ov.videoBitrate, 10)
		sc.videoMaxrate = sc.videoBitrate
		sc.videoBufsize = strconv.FormatInt(ov.videoBitrate*2, 10)
		sc.maxrateBps = ov.videoBitrate
	}
	if ov.videoMaxrate > 0 {
		sc.videoMaxrate = strconv.FormatInt(ov.videoMaxrate, 10)
		sc.maxrateBps = ov.videoMaxrate
	}
	if ov.videoBufsize > 0 {
		sc.videoBufsize = strconv.FormatInt(ov.videoBufsize, 10)
	}
	if ov.maxWidth > 0 {
		sc.maxWidth = ov.maxWidth
	}
	if ov.maxHeight > 0 {
		sc.maxHeight = ov.maxHeight
	}
	sc.overridden = ov.hasQuality()

	return sc
}

// ── per-session overrides (RIVULET_HLS_SESSION_OVERRIDES) ──────────────────

// Bounds for the per-session query overrides. Deliberately generous (they
// exist to reject nonsense and typos, not to second-guess the caller) but
// finite, so one request can't pin a session slot for a year or ask ffmpeg
// for a 100k-pixel-wide frame. The TTL floor is the historical default
// session TTL: a player with a full buffer can go well over a few seconds
// between fetches, so a shorter TTL would let the reaper evict a session
// mid-playback. An override can therefore never make a session less durable
// than stock behaviour.
const (
	minOverrideTTL     = defaultSessionTTL
	maxOverrideTTL     = 30 * 24 * time.Hour
	minOverrideDim     = 16
	maxOverrideDim     = 7680
	minOverrideBitrate = 100_000     // 100k
	maxOverrideBitrate = 200_000_000 // 200M
	maxOverrideBufsize = 2 * maxOverrideBitrate
)

// sessionOverrides is the parsed, validated form of types.HLSSessionOptions.
// Every zero field means "not given" (the corresponding value then comes from
// /settings or env as usual).
type sessionOverrides struct {
	ttl          time.Duration
	maxWidth     int
	maxHeight    int
	videoBitrate int64 // bits/second
	videoMaxrate int64 // bits/second
	videoBufsize int64 // bits
}

// hasQuality reports whether any override that changes the encoded output
// (everything except ttl) is set.
func (o sessionOverrides) hasQuality() bool {
	return o.maxWidth > 0 || o.maxHeight > 0 ||
		o.videoBitrate > 0 || o.videoMaxrate > 0 || o.videoBufsize > 0
}

// parseSessionOverrides validates the raw master.m3u8 query overrides.
// Empty fields are "not given". Any malformed or out-of-range value fails
// the whole request with an error wrapping types.ErrInvalidHLSOption (the
// API maps it to 400) rather than being silently dropped, so a caller never
// gets a session that quietly ignored the quality it asked for.
//
//   - ttl: whole seconds or a Go duration string (the same syntax as the
//     RIVULET_HLS_* duration env knobs), >= 60s (minOverrideTTL) and <= 30
//     days.
//   - maxWidth/maxHeight: integers in [16, 7680], rounded down to even (the
//     yuv420p/nv12 encoders need even dimensions; computeScaledDims keeps
//     the actual output even as well).
//   - bitrate/maxRate: ffmpeg bitrate syntax, decimals allowed
//     (parseDecimalBitrate), in [100k, 200M]; bufSize: same syntax in
//     [100k, 400M] (2x the bitrate cap). When bitrate and maxRate are both
//     given, maxRate must be >= bitrate.
func parseSessionOverrides(opts types.HLSSessionOptions) (sessionOverrides, error) {
	var o sessionOverrides
	if v := strings.TrimSpace(opts.TTL); v != "" {
		d, ok := parseSecondsOrDuration(v)
		if !ok || d < minOverrideTTL || d > maxOverrideTTL {
			return sessionOverrides{}, fmt.Errorf("%w: ttl %q: want whole seconds or a Go duration (e.g. 3600, 90m) between %s and %s",
				types.ErrInvalidHLSOption, v, minOverrideTTL, maxOverrideTTL)
		}
		o.ttl = d
	}
	var err error
	if o.maxWidth, err = parseOverrideDim("maxWidth", opts.MaxWidth); err != nil {
		return sessionOverrides{}, err
	}
	if o.maxHeight, err = parseOverrideDim("maxHeight", opts.MaxHeight); err != nil {
		return sessionOverrides{}, err
	}
	if o.videoBitrate, err = parseOverrideBitrate("bitrate", opts.Bitrate, maxOverrideBitrate); err != nil {
		return sessionOverrides{}, err
	}
	if o.videoMaxrate, err = parseOverrideBitrate("maxRate", opts.MaxRate, maxOverrideBitrate); err != nil {
		return sessionOverrides{}, err
	}
	if o.videoBufsize, err = parseOverrideBitrate("bufSize", opts.BufSize, maxOverrideBufsize); err != nil {
		return sessionOverrides{}, err
	}
	if o.videoBitrate > 0 && o.videoMaxrate > 0 && o.videoMaxrate < o.videoBitrate {
		return sessionOverrides{}, fmt.Errorf("%w: maxRate %q is below bitrate %q; maxRate must be at least the bitrate",
			types.ErrInvalidHLSOption, strings.TrimSpace(opts.MaxRate), strings.TrimSpace(opts.Bitrate))
	}
	return o, nil
}

func parseOverrideDim(name, raw string) (int, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < minOverrideDim || n > maxOverrideDim {
		return 0, fmt.Errorf("%w: %s %q: want an integer between %d and %d",
			types.ErrInvalidHLSOption, name, v, minOverrideDim, maxOverrideDim)
	}
	return n - n%2, nil
}

func parseOverrideBitrate(name, raw string, maxBps int64) (int64, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, nil
	}
	n, ok := parseDecimalBitrate(v)
	if !ok || n < minOverrideBitrate || n > maxBps {
		return 0, fmt.Errorf("%w: %s %q: want an ffmpeg bitrate (a number with an optional k/M/G suffix, e.g. 6M or 1.5M) between %s and %s",
			types.ErrInvalidHLSOption, name, v, formatBitrate(minOverrideBitrate), formatBitrate(maxBps))
	}
	return n, nil
}

// decimalBitrateRe is the bitrate syntax internal/app's envBitrate accepts
// for the RIVULET_TRANSCODE_* rate knobs (its ffmpegBitrateRe, duplicated
// because internal/app imports this package): an integer or decimal number
// with an optional k/K/m/M/g/G (SI, decimal) suffix. Keeping the two in step
// means a value accepted in the env is accepted as a per-session override.
var decimalBitrateRe = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)([kKmMgG]?)$`)

// parseDecimalBitrate converts a decimalBitrateRe value ("1.5M", "800k",
// "6000000") to whole bits/second, rounding to the nearest integer. ok is
// false for anything outside that syntax or too large to represent. Used
// for the per-session overrides, whose result is canonicalised to an
// integer string before it reaches ffmpeg, and for the audio term of
// BANDWIDTH, since ffmpeg accepts a decimal -b:a such as "0.128M".
// It is the only bitrate parser: the env maxrate, the persisted session
// fields and the overrides all go through it, so "8M" and "8.0M" both reach
// the legacy-BANDWIDTH test (isDefaultMaxrate) as exactly 8_000_000.
func parseDecimalBitrate(s string) (int64, bool) {
	m := decimalBitrateRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}
	switch strings.ToLower(m[2]) {
	case "k":
		f *= 1e3
	case "m":
		f *= 1e6
	case "g":
		f *= 1e9
	}
	f = math.Round(f)
	if f >= math.MaxInt64 { // float64(2^63): out of int64 range, same as envBitrate's values that cannot be represented
		return 0, false
	}
	return int64(f), true
}

// formatBitrate renders bps in the shortest exact ffmpeg suffix form, for
// error messages ("100k", "200M").
func formatBitrate(bps int64) string {
	switch {
	case bps%1_000_000 == 0:
		return strconv.FormatInt(bps/1_000_000, 10) + "M"
	case bps%1_000 == 0:
		return strconv.FormatInt(bps/1_000, 10) + "k"
	}
	return strconv.FormatInt(bps, 10)
}

// parseSecondsOrDuration accepts the same syntax as internal/app's
// envDuration (used by every RIVULET_HLS_* duration knob): a Go duration
// string ("90s", "2h30m") or a bare non-negative integer of whole seconds.
// A local copy rather than a shared helper because internal/app imports
// this package (the reverse import would cycle) and envDuration's
// warn-and-keep-default semantics differ from the reject-with-400 needed
// here.
func parseSecondsOrDuration(v string) (time.Duration, bool) {
	if d, err := time.ParseDuration(v); err == nil {
		return d, true
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 || n > math.MaxInt64/int64(time.Second) {
		return 0, false
	}
	return time.Duration(n) * time.Second, true
}

// isDefault reports whether sc matches the fully-unconfigured (no env
// override, no /settings override) state used by deriveBandwidthCodecs to
// decide whether to reproduce the legacy playlist strings verbatim.
func (sc sessionConfig) isDefaultMaxrate() bool {
	return sc.maxrateBps == legacyMaxrateBps
}

// ── bitrate / resolution helpers ────────────────────────────────────────────

// computeScaledDims applies the MaxWidth/MaxHeight downscale caps to a
// source resolution: preserves aspect ratio (uniform scale factor), never
// upscales, and forces both output dimensions even (required by the
// yuv420p/nv12 pixel formats every encoder path in this package uses).
// scaled is false whenever no cap applies or the source already fits within
// both caps, in which case (w, h) == (srcW, srcH).
func computeScaledDims(srcW, srcH, maxW, maxH int) (w, h int, scaled bool) {
	if srcW <= 0 || srcH <= 0 || (maxW <= 0 && maxH <= 0) {
		return srcW, srcH, false
	}
	scale := 1.0
	if maxW > 0 {
		if s := float64(maxW) / float64(srcW); s < scale {
			scale = s
		}
	}
	if maxH > 0 {
		if s := float64(maxH) / float64(srcH); s < scale {
			scale = s
		}
	}
	if scale >= 1.0 {
		return srcW, srcH, false // cap(s) already satisfied; never upscale
	}
	w = int(math.Floor(float64(srcW) * scale))
	h = int(math.Floor(float64(srcH) * scale))
	w -= w % 2
	h -= h % 2
	if w < 2 {
		w = 2
	}
	if h < 2 {
		h = 2
	}
	return w, h, true
}

// h264Level maps an output resolution to the AVC profile-level CODECS string
// used in the HLS master playlist, per issue #20's table. Unknown (<=0)
// dimensions keep the historical Level 4.1 string rather than guessing.
func h264Level(width, height int) string {
	if width <= 0 || height <= 0 {
		return legacyCodecsVideo
	}
	area := width * height
	switch {
	case area <= 1280*720:
		return "avc1.64001f" // Level 3.1
	case area <= 1920*1080:
		return "avc1.640029" // Level 4.1
	case area <= 2560*1440:
		return "avc1.640032" // Level 5.0
	default:
		return "avc1.640033" // Level 5.1
	}
}

// deriveBandwidthCodecs computes the master-playlist BANDWIDTH and video
// CODECS entry from the effective bitrate cap (maxrateBps, bits/second) and
// output resolution (post-downscale). When the configuration is exactly the
// historical default — maxrateBps still the original 8M constant and no
// downscale actually applied to this stream — it returns the legacy fixed
// values verbatim (see legacyBandwidth/legacyCodecsVideo) instead of the
// generally-derived ones, so unconfigured output never changes regardless of
// actual source resolution. Otherwise BANDWIDTH is the variant's peak, as
// RFC 8216 section 4.3.4.2 defines it: the video maxrate (the VBV cap passed
// to every encoder; h264_vaapi in CQP mode may not honour it) plus the AAC
// audio bitrate (audioBps, bits/second; 0 when it can't be parsed),
// saturating at MaxInt64, and CODECS is the resolution-derived level. An
// unset or unparsable maxrate falls back to legacyBandwidth.
//
// A per-session quality override (sc.overridden) always counts as
// "configured", so it takes the derived path even when its values happen to
// equal the defaults.
func deriveBandwidthCodecs(sc sessionConfig, audioBps int64, outW, outH int, downscaled bool) (bandwidth int64, codecsVideo string) {
	if sc.isDefaultMaxrate() && !sc.overridden && !downscaled {
		return legacyBandwidth, legacyCodecsVideo
	}
	if sc.maxrateBps <= 0 {
		return legacyBandwidth, h264Level(outW, outH)
	}
	audioBps = max(audioBps, 0)
	if sc.maxrateBps > math.MaxInt64-audioBps {
		return math.MaxInt64, h264Level(outW, outH)
	}
	return sc.maxrateBps + audioBps, h264Level(outW, outH)
}

// buildVideoFilter assembles the -vf filter chain for one encoder backend,
// combining the existing high-bit-depth pixel-format conversion with an
// optional downscale filter using the correct filter name per backend:
//   - h264_vaapi already builds a hw frame (format=nv12,hwupload in the
//     caller); scale_vaapi operates on that hw frame and must come after it.
//   - every other backend (libx264, h264_nvenc, h264_qsv,
//     h264_videotoolbox, h264_v4l2m2m) in this codebase decodes and filters
//     entirely in system memory (no -hwaccel/hwupload is ever added — see
//     transcodeSegment's high-bit-depth-safety note), so a plain CPU "scale"
//     filter matches their existing chain style; scale_cuda/scale_npp would
//     need a CUDA/NPP hw-frames context this codebase never establishes.
//
// tm (zero value = off) switches to the software HDR→SDR tone-mapping chain
// (see tonemapChain), which also performs the downscale in its first zscale
// stage so the expensive float tone-map runs at output resolution. Its
// 8-bit BT.709 output then feeds each backend's normal input: format=yuv420p
// for the system-memory encoders, format=nv12,hwupload for VAAPI (no
// scale_vaapi, since the frame is already scaled). With tm off the result
// is byte-identical to the chain built before tone mapping existed.
func buildVideoFilter(codec string, needsFormatConv bool, scaledW, scaledH int, scaled bool, tm tonemapPlan) string {
	if tm.enabled() {
		chain := tonemapChain(tm, scaledW, scaledH, scaled)
		if codec == "h264_vaapi" {
			return chain + ",format=nv12,hwupload"
		}
		return chain + ",format=yuv420p"
	}
	var filters []string
	switch codec {
	case "h264_vaapi":
		filters = append(filters, "format=nv12", "hwupload")
		if scaled {
			filters = append(filters, fmt.Sprintf("scale_vaapi=w=%d:h=%d", scaledW, scaledH))
		}
	default:
		if needsFormatConv {
			filters = append(filters, "format=yuv420p")
		}
		if scaled {
			filters = append(filters, fmt.Sprintf("scale=%d:%d", scaledW, scaledH))
		}
	}
	return strings.Join(filters, ",")
}
