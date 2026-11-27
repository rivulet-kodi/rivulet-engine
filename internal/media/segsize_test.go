// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// Size check (HLSConfig.SmallSegmentPercent): a segment far smaller than the
// session's other full-length video segments is served but not cached.

func TestSmallSegmentReason(t *testing.T) {
	four := []int64{4000, 4100, 3900, 4200} // lower median 4000
	cases := []struct {
		name     string
		size     int64
		siblings []int64
		percent  int
		flagged  bool
	}{
		{"off", 10, four, 0, false},
		{"negative percent is off", 10, four, -25, false},
		{"too few siblings", 10, four[:3], 25, false},
		{"no siblings", 10, nil, 25, false},
		{"far smaller", 80, four, 25, true},
		{"just under the threshold", 999, four, 25, true},
		{"exactly at the threshold", 1000, four, 25, false},
		{"normal spread", 2500, four, 25, false},
		{"larger than every sibling", 9000, four, 25, false},
		// Sorted 1000 4000 4100 4200 9000 30000: the lower middle (4100)
		// puts the threshold at 1025, the upper (4200) would at 1050.
		{"even count takes the lower middle", 1040, []int64{1000, 4200, 9000, 4000, 4100, 30000}, 25, false},
		{"zero-sized siblings never flag", 0, []int64{0, 0, 0, 0}, 25, false},
		{"the incident: 93,624 bytes against 4,536,440", 93624, []int64{4536440, 4536440, 4536440, 4536440, 4536440}, 25, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := slices.Clone(tc.siblings)
			got := smallSegmentReason(tc.size, tc.siblings, tc.percent)
			if (got != "") != tc.flagged {
				t.Errorf("smallSegmentReason(%d, %v, %d) = %q, flagged want %v", tc.size, tc.siblings, tc.percent, got, tc.flagged)
			}
			if !slices.Equal(before, tc.siblings) {
				t.Errorf("siblings modified: %v, was %v", tc.siblings, before)
			}
		})
	}
}

// TestFullVideoSegmentSizes checks which cached files count as siblings:
// video segments before the last one, restored from disk or noted since,
// never audio renditions, work or provisional files, the last segment or
// the segment being judged.
func TestFullVideoSegmentSizes(t *testing.T) {
	dir := t.TempDir()
	files := map[string]int{
		"seg0.ts":               10,
		"seg1.ts":               20,
		"seg3.ts":               30, // the segment being judged (skip)
		"seg9.ts":               90, // the last segment
		"a0seg2.ts":             40, // an audio rendition
		"seg2.ts.prov-1.tmp.ts": 50, // served but not cached
		"seg5.ts.tmp.ts":        60, // a transcode in progress
		"seg6.ts.retry.tmp.ts":  70,
		"stream.m3u8":           80,
		"sub0.vtt":              90,
		"segx.ts":               11, // not a segment number
		"seg-1.ts":              12,
	}
	for name, n := range files {
		if err := os.WriteFile(filepath.Join(dir, name), bytes.Repeat([]byte{1}, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s := &hlsSession{dir: dir}
	s.noteSegment("seg4.ts", 44, time.Now()) // loads the directory first, then adds
	got := s.fullVideoSegmentSizes("seg3.ts", 9)
	slices.Sort(got)
	want := []int64{10, 20, 44}
	if !slices.Equal(got, want) {
		t.Errorf("fullVideoSegmentSizes = %v, want %v", got, want)
	}
}

// sizeCheckSession returns a manager with SmallSegmentPercent set to percent
// and a session of dur seconds whose cache holds siblings video segments
// seg0.ts… of sibSize bytes each.
func sizeCheckSession(t *testing.T, percent int, dur float64, siblings int, sibSize int) (*hlsManager, *hlsSession) {
	t.Helper()
	m := newTestHLSManager(t)
	m.cfg.SmallSegmentPercent = percent
	s := injectSession(t, m, "size")
	s.duration = dur
	s.hasVideo = true
	for i := range siblings {
		p := filepath.Join(s.dir, fmt.Sprintf("seg%d.ts", i))
		if err := os.WriteFile(p, make([]byte, sibSize), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return m, s
}

// TestSegmentInspectorSize checks when the size check judges a segment. The
// candidate is a valid four-second segment of about 38 kB; its siblings are
// 200 kB, so at 25% (50 kB) it is too small.
func TestSegmentInspectorSize(t *testing.T) {
	const sib = 200_000
	cases := []struct {
		name       string
		percent    int
		dur        float64 // session duration (0 = unknown)
		siblings   int
		n          int
		kind       segKind
		noVideo    bool    // write a segment with no video frames
		videoEnd   float64 // where the source's video ends (0 = unknown)
		wantReason string
	}{
		{"flagged", 25, 40, 4, 5, segMuxed, false, 0, "under 25% of the median 200000 of 4 earlier segments"},
		{"video-only rendition", 25, 40, 4, 5, segVideoOnly, false, 0, "under 25%"},
		{"off by default", 0, 40, 4, 5, segMuxed, false, 0, ""},
		{"too few siblings", 25, 40, 3, 5, segMuxed, false, 0, ""},
		{"last segment", 25, 40, 4, 9, segMuxed, false, 0, ""},
		{"last segment, short", 25, 38, 4, 9, segMuxed, false, 0, ""},
		{"unknown duration", 25, 0, 4, 5, segMuxed, false, 0, ""},
		{"audio-only rendition", 25, 40, 4, 5, segAudioOnly, false, 0, ""},
		{"the frame check comes first", 25, 40, 4, 5, segMuxed, true, 0, "no video frames"},
		{"normal spread passes", 10, 40, 4, 5, segMuxed, false, 0, ""},
		{"the segment before the last", 25, 40, 4, 8, segMuxed, false, 0, "under 25%"},
		{"video ends later", 25, 40, 4, 5, segMuxed, false, 30, "under 25%"},
		{"video ends at the segment end", 25, 40, 4, 5, segMuxed, false, 24, "under 25%"},
		{"video ends inside the segment", 25, 40, 4, 5, segMuxed, false, 23.5, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, s := sizeCheckSession(t, tc.percent, tc.dur, tc.siblings, sib)
			start := float64(tc.n) * segDur
			nVideo := 96
			if tc.noVideo {
				nVideo = 0
			}
			data := segTS(nVideo, start, 4)
			if !tc.noVideo && (len(data)*100 >= sib*25 || len(data)*100 < sib*10) {
				t.Fatalf("fixture is %d bytes; the cases assume 10%% to 25%% of %d", len(data), sib)
			}
			path := filepath.Join(t.TempDir(), "cand.ts")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			want := segExpectation(tc.kind, true, tc.videoEnd, true, start, segDur)
			name := segName(tc.n, tc.kind, 0)
			v, err := m.segmentInspector(s, tc.n, name, tc.dur, want)(path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantReason == "" && v.reason != "" || !strings.Contains(v.reason, tc.wantReason) {
				t.Errorf("reason = %q, want %q", v.reason, tc.wantReason)
			}
		})
	}
}

// TestTranscodeSegmentSmallNotCached runs the whole transcodeSegment path
// with a PATH-shim ffmpeg that writes a valid but small segment: with the
// size check on it is served from a provisional path, not cached, and its
// reason recorded; with the check off (the default) the same output is
// cached as before.
func TestTranscodeSegmentSmallNotCached(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffmpeg stub is a /bin/sh script")
	}
	const n = 5
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture.ts")
	if err := os.WriteFile(fixture, segTS(96, n*segDur, 4), 0o600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nfor a; do out=$a; done\ncp \"" + fixture + "\" \"$out\"\n"
	if err := os.WriteFile(filepath.Join(dir, "ffmpeg"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	for _, tc := range []struct {
		name    string
		percent int
		cached  bool
	}{
		{"check on: served, not cached", 25, false},
		{"check off: cached as before", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, s := sizeCheckSession(t, tc.percent, 40, 4, 200_000)
			m.selfBase = "http://127.0.0.1:11470"
			s.mediaURL = m.selfBase + testSelfPath
			seg := filepath.Join(s.dir, fmt.Sprintf("seg%d.ts", n))
			p, err := m.transcodeSegment(context.Background(), s, n, segMuxed, 0, segLive)
			if err != nil {
				t.Fatalf("transcode: %v", err)
			}
			if !dirExists(p) {
				t.Errorf("served path %q does not exist", p)
			}
			if got := p == seg && dirExists(seg); got != tc.cached {
				t.Errorf("served %q; cached = %v, want %v", p, got, tc.cached)
			}
			s.mu.Lock()
			reason := s.uncached[filepath.Base(seg)]
			s.mu.Unlock()
			if tc.cached != (reason == "") || !tc.cached && !strings.Contains(reason, "under 25%") {
				t.Errorf("uncached reason = %q", reason)
			}
			if !tc.cached && slices.Contains(s.fullVideoSegmentSizes("", 9), int64(len(segTS(96, n*segDur, 4)))) {
				t.Error("the uncached segment counts as a sibling")
			}
		})
	}
}

func TestFullVideoEnd(t *testing.T) {
	cases := []struct {
		dur, videoEnd float64
		want          int
	}{
		{40, 0, 9},            // segments 0-9, the last one excluded
		{38, 0, 9},            // a short last segment
		{40.1, 0, 10},         // an 11th segment of 0.1 s
		{40, 30, 7},           // segment 7 (28-32) reaches past the video
		{40, 32, 8},           // segment 7 ends exactly with the video
		{40, 100, 9},          // a video end past the media's
		{4, 0, 0},             // a single segment is the last one
		{40, -1, 9},           // unknown video end
		{40, 3, 0},            // video shorter than one segment
		{40.000000001, 0, 10}, // as many segments as the playlist counts
	}
	for _, tc := range cases {
		if got := fullVideoEnd(tc.dur, tc.videoEnd); got != tc.want {
			t.Errorf("fullVideoEnd(%v, %v) = %d, want %d", tc.dur, tc.videoEnd, got, tc.want)
		}
	}
}

// TestSegmentInspectorSiblingsStopAtVideoEnd checks that segments reaching
// past the end of the source's video don't count as siblings: with the
// video ending at 24 s only seg0 and seg1 qualify, too few to judge seg2,
// although seg6 to seg9 are cached and as large.
func TestSegmentInspectorSiblingsStopAtVideoEnd(t *testing.T) {
	const sib = 200_000
	m, s := sizeCheckSession(t, 25, 40, 2, sib)
	for i := 6; i <= 9; i++ {
		if err := os.WriteFile(filepath.Join(s.dir, fmt.Sprintf("seg%d.ts", i)), make([]byte, sib), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const n = 2
	start := float64(n) * segDur
	path := filepath.Join(t.TempDir(), "cand.ts")
	if err := os.WriteFile(path, segTS(96, start, 4), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		videoEnd float64
		flagged  bool
	}{
		{24, false}, // 2 siblings: not judged
		{0, true},   // control: video end unknown, 6 siblings
	} {
		want := segExpectation(segMuxed, true, tc.videoEnd, true, start, segDur)
		v, err := m.segmentInspector(s, n, segName(n, segMuxed, 0), 40, want)(path)
		if err != nil {
			t.Fatal(err)
		}
		if (v.reason != "") != tc.flagged {
			t.Errorf("videoEnd %v: reason = %q, flagged want %v", tc.videoEnd, v.reason, tc.flagged)
		}
	}
}
