// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package app

// Tests for the issue #20 config-parsing helpers added to lookup.go/app.go:
// envDuration (duration string + plain seconds, invalid -> default),
// envBitrate, and the hlsConfig() env -> media.HLSConfig builder.

import (
	"net"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/media"
)

func TestEnvDurationParsesDurationString(t *testing.T) {
	lookup := MapLookup(map[string]string{"K": "5m30s"})
	got := envDuration(lookup, "K", time.Second)
	want := 5*time.Minute + 30*time.Second
	if got != want {
		t.Errorf("envDuration(%q) = %s, want %s", "5m30s", got, want)
	}
}

func TestEnvDurationParsesPlainSeconds(t *testing.T) {
	lookup := MapLookup(map[string]string{"K": "90"})
	if got := envDuration(lookup, "K", time.Second); got != 90*time.Second {
		t.Errorf("envDuration(%q) = %s, want 90s", "90", got)
	}
}

func TestEnvDurationUnsetReturnsDefault(t *testing.T) {
	lookup := MapLookup(map[string]string{})
	def := 42 * time.Second
	if got := envDuration(lookup, "K", def); got != def {
		t.Errorf("envDuration(unset) = %s, want default %s", got, def)
	}
}

func TestEnvDurationInvalidReturnsDefault(t *testing.T) {
	def := 7 * time.Second
	cases := []string{"not-a-duration", "1.5.3s", "abc123", ""}
	for _, v := range cases {
		lookup := MapLookup(map[string]string{"K": v})
		if got := envDuration(lookup, "K", def); got != def {
			t.Errorf("envDuration(%q) = %s, want default %s", v, got, def)
		}
	}
}

func TestEnvDurationNegativeReturnsDefault(t *testing.T) {
	def := 7 * time.Second
	for _, v := range []string{"-5s", "-5"} {
		lookup := MapLookup(map[string]string{"K": v})
		if got := envDuration(lookup, "K", def); got != def {
			t.Errorf("envDuration(%q) = %s, want default %s (negative rejected)", v, got, def)
		}
	}
}

func TestEnvBitrateValidValues(t *testing.T) {
	for _, v := range []string{"8M", "800k", "8000000", "1.5M"} {
		lookup := MapLookup(map[string]string{"K": v})
		if got := envBitrate(lookup, "K", "8M"); got != v {
			t.Errorf("envBitrate(%q) = %q, want %q unchanged", v, got, v)
		}
	}
}

func TestEnvBitrateInvalidReturnsDefault(t *testing.T) {
	def := "8M"
	for _, v := range []string{"eight megabits", "8Mi", "-8M", "8 M"} {
		lookup := MapLookup(map[string]string{"K": v})
		if got := envBitrate(lookup, "K", def); got != def {
			t.Errorf("envBitrate(%q) = %q, want default %q", v, got, def)
		}
	}
}

func TestEnvBitrateUnsetReturnsDefault(t *testing.T) {
	lookup := MapLookup(map[string]string{})
	if got := envBitrate(lookup, "K", "16M"); got != "16M" {
		t.Errorf("envBitrate(unset) = %q, want default", got)
	}
}

// ── hlsConfig() ──────────────────────────────────────────────────────────────

func TestHLSConfigDefaultsWhenNothingSet(t *testing.T) {
	got := hlsConfig(MapLookup(map[string]string{}))
	want := media.DefaultHLSConfig()
	if got != want {
		t.Errorf("hlsConfig(empty env) = %+v, want DefaultHLSConfig() %+v", got, want)
	}
}

func TestHLSConfigPrivateAllow(t *testing.T) {
	if got := hlsConfig(MapLookup(map[string]string{})).PrivateAllow; got != nil {
		t.Errorf("PrivateAllow with RIVULET_HLS_PRIVATE_ALLOW unset = %v, want nil", got)
	}
	// RIVULET_PROXY_PRIVATE_ALLOW is /proxy's list and must not leak into media.
	if got := hlsConfig(MapLookup(map[string]string{"RIVULET_PROXY_PRIVATE_ALLOW": "192.168.1.10"})).PrivateAllow; got != nil {
		t.Errorf("PrivateAllow from RIVULET_PROXY_PRIVATE_ALLOW = %v, want nil", got)
	}
	got := hlsConfig(MapLookup(map[string]string{"RIVULET_HLS_PRIVATE_ALLOW": "192.168.1.0/24, bad_host!"})).PrivateAllow
	if got == nil || !got.Contains(net.ParseIP("192.168.1.10")) || got.Contains(net.ParseIP("10.0.0.1")) {
		t.Errorf("PrivateAllow from RIVULET_HLS_PRIVATE_ALLOW = %v, want 192.168.1.0/24 only", got)
	}
}

func TestHLSConfigSessionTTLOverrideDerivesReaperInterval(t *testing.T) {
	// RIVULET_HLS_REAPER_INTERVAL left unset: its default must scale with the
	// overridden SessionTTL (issue #20: "Should probably scale with the TTL
	// above"), not stay pinned at the historical 30s.
	got := hlsConfig(MapLookup(map[string]string{"RIVULET_HLS_SESSION_TTL": "10s"}))
	if got.SessionTTL != 10*time.Second {
		t.Fatalf("SessionTTL = %s, want 10s", got.SessionTTL)
	}
	want := media.DefaultReaperInterval(10 * time.Second)
	if got.ReaperInterval != want {
		t.Errorf("ReaperInterval = %s, want derived %s", got.ReaperInterval, want)
	}
}

// RIVULET_HLS_SESSION_TTL=0 disables idle eviction (like
// RIVULET_TORRENT_IDLE_TIMEOUT=0); unset keeps the 60s default and negative or
// invalid values keep falling back to it. In every case the derived reaper
// interval stays a sane positive value (never 0, which would panic
// time.NewTicker, or a tiny busy-loop interval).
func TestHLSConfigSessionTTLZeroDisablesEviction(t *testing.T) {
	cases := []struct {
		name        string
		env         map[string]string
		wantTTL     time.Duration
		wantDisable bool
	}{
		{"unset", map[string]string{}, 60 * time.Second, false},
		{"empty", map[string]string{"RIVULET_HLS_SESSION_TTL": ""}, 60 * time.Second, false},
		{"zero", map[string]string{"RIVULET_HLS_SESSION_TTL": "0"}, 0, true},
		{"zero duration string", map[string]string{"RIVULET_HLS_SESSION_TTL": "0s"}, 0, true},
		{"negative seconds", map[string]string{"RIVULET_HLS_SESSION_TTL": "-5"}, 60 * time.Second, false},
		{"negative duration", map[string]string{"RIVULET_HLS_SESSION_TTL": "-1m"}, 60 * time.Second, false},
		{"invalid", map[string]string{"RIVULET_HLS_SESSION_TTL": "forever"}, 60 * time.Second, false},
		{"positive", map[string]string{"RIVULET_HLS_SESSION_TTL": "90"}, 90 * time.Second, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := hlsConfig(MapLookup(c.env))
			if got.SessionTTL != c.wantTTL {
				t.Errorf("SessionTTL = %s, want %s", got.SessionTTL, c.wantTTL)
			}
			if got.DisableIdleEviction != c.wantDisable {
				t.Errorf("DisableIdleEviction = %v, want %v", got.DisableIdleEviction, c.wantDisable)
			}
			if got.ReaperInterval != 30*time.Second {
				t.Errorf("ReaperInterval = %s, want 30s", got.ReaperInterval)
			}
		})
	}
}

func TestHLSConfigReaperIntervalExplicitOverride(t *testing.T) {
	got := hlsConfig(MapLookup(map[string]string{
		"RIVULET_HLS_SESSION_TTL":     "10s",
		"RIVULET_HLS_REAPER_INTERVAL": "3s",
	}))
	if got.ReaperInterval != 3*time.Second {
		t.Errorf("explicit RIVULET_HLS_REAPER_INTERVAL not honoured: got %s, want 3s", got.ReaperInterval)
	}
}

func TestHLSConfigEveryKnobOverridable(t *testing.T) {
	env := map[string]string{
		"RIVULET_HLS_SESSION_TTL":                 "45s",
		"RIVULET_HLS_NEG_PROBE_TTL":               "2m",
		"RIVULET_HLS_POS_PROBE_TTL":               "3m",
		"RIVULET_HLS_MAX_SESSIONS":                "10",
		"RIVULET_HLS_WORK_DIR":                    "/data/hls",
		"RIVULET_HLS_SESSION_OVERRIDES":           "1",
		"RIVULET_HLS_PERSIST":                     "1",
		"RIVULET_HLS_PREPARE":                     "1",
		"RIVULET_HLS_PREPARE_CONCURRENCY":         "2",
		"RIVULET_HLS_TRANSCODE_AHEAD":             "4",
		"RIVULET_HLS_TRANSCODE_AHEAD_CONCURRENCY": "3",
		"RIVULET_TRANSCODE_VIDEO_BITRATE":         "4M",
		"RIVULET_TRANSCODE_MAXRATE":               "5M",
		"RIVULET_TRANSCODE_BUFSIZE":               "10M",
		"RIVULET_TRANSCODE_MAX_WIDTH":             "1280",
		"RIVULET_TRANSCODE_MAX_HEIGHT":            "720",
		"RIVULET_TRANSCODE_VAAPI_QP":              "18",
		"RIVULET_TRANSCODE_NVENC_PRESET":          "p1",
		"RIVULET_TRANSCODE_QSV_PRESET":            "medium",
		"RIVULET_TRANSCODE_X264_PRESET":           "fast",
		"RIVULET_TRANSCODE_X264_CRF":              "20",
		"RIVULET_TRANSCODE_TONEMAP":               "mobius",
		"RIVULET_TRANSCODE_AUDIO_CHANNELS":        "6",
		"RIVULET_TRANSCODE_AUDIO_BITRATE":         "256k",
		"RIVULET_TRANSCODE_CONCURRENCY":           "2",
		"RIVULET_HLS_VAAPI_DEVICE":                "/dev/dri/renderD129",
		"RIVULET_HLS_SEGMENT_TIMEOUT":             "60s",
		"RIVULET_HLS_SUBTITLE_TIMEOUT":            "45s",
		"RIVULET_HLS_PROBE_TIMEOUT":               "15s",
		"RIVULET_HLS_SEEK_PREROLL":                "3",
		"RIVULET_HLS_SEGMENT_CACHE_BYTES":         "536870912",
		"RIVULET_HLS_RELAY_CACHE_BYTES":           "67108864",
	}
	got := hlsConfig(MapLookup(env))
	want := media.HLSConfig{
		SessionTTL:         45 * time.Second,
		ReaperInterval:     media.DefaultReaperInterval(45 * time.Second),
		NegProbeTTL:        2 * time.Minute,
		PosProbeTTL:        3 * time.Minute,
		MaxSessions:        10,
		WorkDir:            "/data/hls",
		SessionOverrides:   true,
		Persist:            true,
		Prepare:            true,
		PrepareConcurrency: 2,
		TranscodeAhead:     4,
		AheadConcurrency:   3,
		VideoBitrate:       "4M",
		VideoMaxrate:       "5M",
		VideoBufsize:       "10M",
		MaxWidth:           1280,
		MaxHeight:          720,
		VAAPIQP:            18,
		NVENCPreset:        "p1",
		QSVPreset:          "medium",
		X264Preset:         "fast",
		X264CRF:            20,
		Tonemap:            "mobius",
		AudioChannels:      6,
		AudioBitrate:       "256k",
		SegmentConcurrency: 2,
		VAAPIDevice:        "/dev/dri/renderD129",
		SegmentTimeout:     60 * time.Second,
		SubtitleTimeout:    45 * time.Second,
		ProbeTimeout:       15 * time.Second,
		SeekPreroll:        3 * time.Second,
		SegmentCacheBytes:  512 << 20,
		RelayCacheBytes:    64 << 20,
	}
	if got != want {
		t.Errorf("hlsConfig(full env) = %+v,\nwant %+v", got, want)
	}
}

func TestEnvTonemap(t *testing.T) {
	cases := []struct {
		name  string
		value string
		set   bool
		want  string
	}{
		{"unset", "", false, ""},
		{"empty", "", true, ""},
		{"off", "off", true, ""},
		{"0", "0", true, ""},
		{"false", "false", true, ""},
		{"none", "none", true, ""},
		{"on", "on", true, "mobius"},
		{"true", "TRUE", true, "mobius"},
		{"1", "1", true, "mobius"},
		{"yes", "yes", true, "mobius"},
		{"hable", "hable", true, "hable"},
		{"mobius", "mobius", true, "mobius"},
		{"reinhard mixed case, padded", " Reinhard ", true, "reinhard"},
		{"clip", "clip", true, "clip"},
		{"linear", "linear", true, "linear"},
		{"gamma", "gamma", true, "gamma"},
		{"invalid", "bt2390", true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := map[string]string{}
			if c.set {
				env["RIVULET_TRANSCODE_TONEMAP"] = c.value
			}
			if got := envTonemap(MapLookup(env), "RIVULET_TRANSCODE_TONEMAP"); got != c.want {
				t.Errorf("envTonemap(%q) = %q, want %q", c.value, got, c.want)
			}
			if got := hlsConfig(MapLookup(env)).Tonemap; got != c.want {
				t.Errorf("hlsConfig(...).Tonemap for %q = %q, want %q", c.value, got, c.want)
			}
		})
	}
}

func TestHLSConfigRelayCacheBytes(t *testing.T) {
	cases := map[string]int64{"": 0, "0": 0, "33554432": 32 << 20, "-1": 0, "junk": 0}
	for v, want := range cases {
		env := map[string]string{}
		if v != "" {
			env["RIVULET_HLS_RELAY_CACHE_BYTES"] = v
		}
		if got := hlsConfig(MapLookup(env)).RelayCacheBytes; got != want {
			t.Errorf("RIVULET_HLS_RELAY_CACHE_BYTES=%q: RelayCacheBytes = %d, want %d", v, got, want)
		}
	}
}

func TestHLSConfigSessionOverridesGate(t *testing.T) {
	cases := map[string]bool{"": false, "0": false, "off": false, "false": false, "1": true, "true": true, "on": true}
	for v, want := range cases {
		env := map[string]string{}
		if v != "" {
			env["RIVULET_HLS_SESSION_OVERRIDES"] = v
		}
		if got := hlsConfig(MapLookup(env)).SessionOverrides; got != want {
			t.Errorf("RIVULET_HLS_SESSION_OVERRIDES=%q → SessionOverrides=%v, want %v", v, got, want)
		}
	}
}
func TestHLSConfigNegativeMaxWidthHeightRejected(t *testing.T) {
	got := hlsConfig(MapLookup(map[string]string{
		"RIVULET_TRANSCODE_MAX_WIDTH":  "-1",
		"RIVULET_TRANSCODE_MAX_HEIGHT": "-1",
	}))
	if got.MaxWidth != 0 || got.MaxHeight != 0 {
		t.Errorf("negative MaxWidth/MaxHeight must fall back to default 0, got %d/%d", got.MaxWidth, got.MaxHeight)
	}
}

func TestHLSConfigCreateMetadataWaitViaTypesConfig(t *testing.T) {
	// CreateMetadataWait lives on types.Config (built in Run), not
	// media.HLSConfig; covered by envDuration directly plus the run_test.go
	// integration coverage. This is a smoke check that the same helper used
	// there behaves for the exact env name Run wires it to.
	lookup := MapLookup(map[string]string{"RIVULET_CREATE_METADATA_TIMEOUT": "45"})
	if got := envDuration(lookup, "RIVULET_CREATE_METADATA_TIMEOUT", 90*time.Second); got != 45*time.Second {
		t.Errorf("RIVULET_CREATE_METADATA_TIMEOUT = %s, want 45s", got)
	}
}

func TestHLSConfigSeekPreroll(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want time.Duration
	}{
		{"unset", map[string]string{}, 10 * time.Second},
		{"empty", map[string]string{"RIVULET_HLS_SEEK_PREROLL": ""}, 10 * time.Second},
		{"zero seconds disables", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "0"}, media.SeekPrerollNone},
		{"zero duration disables", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "0s"}, media.SeekPrerollNone},
		{"plain seconds", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "2"}, 2 * time.Second},
		{"duration string", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "2500ms"}, 2500 * time.Millisecond},
		{"negative seconds", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "-3"}, 10 * time.Second},
		{"negative duration", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "-3s"}, 10 * time.Second},
		{"invalid", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "soon"}, 10 * time.Second},
		{"at the default", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "10"}, media.MaxSeekPreroll},
		{"above the default clamped", map[string]string{"RIVULET_HLS_SEEK_PREROLL": "30"}, media.MaxSeekPreroll},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hlsConfig(MapLookup(c.env)).SeekPreroll; got != c.want {
				t.Errorf("SeekPreroll = %s, want %s", got, c.want)
			}
		})
	}
}

func TestHLSConfigTranscodeAheadBounds(t *testing.T) {
	for _, c := range []struct {
		env  string
		want int
	}{{"-1", 0}, {"0", 0}, {"6", 6}, {"99", media.MaxTranscodeAhead}} {
		got := hlsConfig(MapLookup(map[string]string{"RIVULET_HLS_TRANSCODE_AHEAD": c.env}))
		if got.TranscodeAhead != c.want {
			t.Errorf("RIVULET_HLS_TRANSCODE_AHEAD=%s: TranscodeAhead = %d, want %d", c.env, got.TranscodeAhead, c.want)
		}
	}
}

func TestIPTVURLList(t *testing.T) {
	cases := map[string][]string{
		"":                        nil,
		"off":                     nil,
		"https://a.example/p.m3u": {"https://a.example/p.m3u"},
		" https://a.example/x.xml.gz, ftp://b.example/y,not a url ,http://c.example/z.xml,": {"https://a.example/x.xml.gz", "http://c.example/z.xml"},
	}
	for in, want := range cases {
		got := iptvURLList(MapLookup(map[string]string{"K": in}), "K", "iptv guide")
		if len(got) != len(want) {
			t.Errorf("iptvURLList(%q) = %v; want %v", in, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("iptvURLList(%q) = %v; want %v", in, got, want)
			}
		}
	}
	if got := iptvURLList(MapLookup(nil), "K", "iptv guide"); got != nil {
		t.Errorf("unset = %v; want nil", got)
	}
}
