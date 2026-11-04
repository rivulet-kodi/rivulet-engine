// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── synthetic MPEG-TS builder ────────────────────────────────────────────────

const (
	testPMTPID   = 0x1000
	testVideoPID = 0x100
	testAudioPID = 0x101
)

// tsPacket builds one 188-byte TS packet. adapt adds a minimal adaptation
// field before the payload; payload == nil yields an adaptation-only packet.
func tsPacket(pid uint16, pusi bool, adapt bool, payload []byte) []byte {
	p := make([]byte, tsPacketSize)
	p[0] = 0x47
	p[1] = byte(pid>>8) & 0x1f
	if pusi {
		p[1] |= 0x40
	}
	p[2] = byte(pid)
	off := 4
	switch {
	case payload == nil:
		p[3] = 0x20 // adaptation field only
		p[4] = tsPacketSize - 5
		return p
	case adapt:
		p[3] = 0x30 // adaptation field + payload
		p[4] = 1    // adaptation_field_length
		p[5] = 0    // flags
		off = 6
	default:
		p[3] = 0x10 // payload only
	}
	n := copy(p[off:], payload)
	for i := off + n; i < tsPacketSize; i++ {
		p[i] = 0xff
	}
	return p
}

// psi wraps a long-form section body (everything after the 8-byte header,
// before the CRC) into a PSI payload with pointer_field 0 and a dummy CRC
// (countTSPackets does not verify it).
func psi(tableID byte, body []byte) []byte {
	secLen := 5 + len(body) + 4
	b := []byte{0, tableID, 0xb0 | byte(secLen>>8), byte(secLen), 0, 1, 0xc1, 0, 0}
	b = append(b, body...)
	return append(b, 0xde, 0xad, 0xbe, 0xef)
}

func patPacket() []byte {
	return tsPacket(0, true, false, psi(0x00, []byte{
		0, 0, 0xe0, 0x10, // program 0 → network PID 0x10 (must be ignored)
		0, 1, 0xe0 | testPMTPID>>8, testPMTPID & 0xff,
	}))
}

// pmtPacket declares one elementary stream per (streamType, pid) pair and a
// 3-byte program-level descriptor to exercise program_info_length.
func pmtPacket(streams ...[2]uint16) []byte {
	body := []byte{0xe0 | testVideoPID>>8, testVideoPID & 0xff, 0xf0, 3, 0x05, 1, 0x00}
	for _, s := range streams {
		body = append(body, byte(s[0]), 0xe0|byte(s[1]>>8), byte(s[1]), 0xf0, 0)
	}
	return tsPacket(testPMTPID, true, false, psi(0x02, body))
}

// pesStart is the first packet of a PES carrying pts (90 kHz), or no PTS
// when pts < 0.
func pesStart(pid uint16, adapt bool, pts int64) []byte {
	if pts < 0 {
		return tsPacket(pid, true, adapt, []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0x00, 0})
	}
	return tsPacket(pid, true, adapt, []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0x80, 5,
		0x21 | byte(pts>>29)&0x0e, byte(pts >> 22), byte(pts>>14) | 1, byte(pts >> 7), byte(pts<<1) | 1})
}

func pesCont(pid uint16) []byte { return tsPacket(pid, false, false, []byte{1, 2, 3}) }

var (
	h264Stream = [2]uint16{0x1b, testVideoPID}
	aacStream  = [2]uint16{0x0f, testAudioPID}
)

func buildTS(pkts ...[]byte) []byte { return bytes.Join(pkts, nil) }

// segTS returns a PAT/PMT (H.264 + AAC) followed by nVideo video PES whose
// PTS start at firstVideo seconds (24 fps), then nAudio audio PES, each
// followed by a continuation packet.
func segTS(nVideo int, firstVideo float64, nAudio int) []byte {
	pkts := [][]byte{patPacket(), pmtPacket(h264Stream, aacStream)}
	for i := 0; i < nVideo; i++ {
		pts := int64(firstVideo*ptsHz) + int64(i)*ptsHz/24
		pkts = append(pkts, pesStart(testVideoPID, i%2 == 0, pts), pesCont(testVideoPID))
	}
	for i := 0; i < nAudio; i++ {
		pkts = append(pkts, pesStart(testAudioPID, false, -1), pesCont(testAudioPID))
	}
	return buildTS(pkts...)
}

// ── countTSPackets ───────────────────────────────────────────────────────────

func TestCountTSPackets(t *testing.T) {
	sec := func(s float64) int64 { return int64(s * ptsHz) }
	cases := []struct {
		name    string
		data    []byte
		want    tsCounts
		wantErr bool
	}{
		{name: "video and audio", data: segTS(5, 12, 3), want: tsCounts{video: 5, audio: 3, videoPTS: sec(12), videoSpan: 4 * ptsHz / 24, hasVideoPTS: true}},
		{name: "audio only output", data: segTS(0, 0, 4), want: tsCounts{audio: 4}},
		{name: "earliest PTS despite B-frame order", data: buildTS(patPacket(), pmtPacket(h264Stream),
			pesStart(testVideoPID, false, sec(56.2)), pesStart(testVideoPID, false, sec(56.04)), pesStart(testVideoPID, false, sec(56.12))),
			want: tsCounts{video: 3, videoPTS: sec(56.04), videoSpan: sec(56.2) - sec(56.04), hasVideoPTS: true}},
		{name: "earliest PTS across 33-bit wrap", data: buildTS(patPacket(), pmtPacket(h264Stream),
			pesStart(testVideoPID, false, 100), pesStart(testVideoPID, false, ptsWrap-50)),
			want: tsCounts{video: 2, videoPTS: ptsWrap - 50, videoSpan: 150, hasVideoPTS: true}},
		{name: "video PES without PTS", data: buildTS(patPacket(), pmtPacket(h264Stream), pesStart(testVideoPID, false, -1)),
			want: tsCounts{video: 1}},
		{name: "PMT without video stream", data: buildTS(patPacket(), pmtPacket(aacStream),
			pesStart(testAudioPID, false, -1), pesStart(testVideoPID, false, 0)), want: tsCounts{audio: 1}},
		{name: "unknown stream type not counted", data: buildTS(patPacket(),
			pmtPacket([2]uint16{0x06, testVideoPID}), pesStart(testVideoPID, false, 0)), want: tsCounts{}},
		{name: "adaptation-only packets ignored", data: buildTS(patPacket(), pmtPacket(h264Stream),
			tsPacket(testVideoPID, true, false, nil), pesStart(testVideoPID, true, 0)), want: tsCounts{video: 1, hasVideoPTS: true}},
		{name: "PES before PMT still classified", data: buildTS(patPacket(),
			pesStart(testVideoPID, false, sec(1)), pmtPacket(h264Stream), pesStart(testVideoPID, false, sec(2))),
			want: tsCounts{video: 2, videoPTS: sec(1), videoSpan: sec(1), hasVideoPTS: true}},
		{name: "trailing partial packet ignored", data: append(segTS(2, 0, 1), 0x47, 0x41, 0x00),
			want: tsCounts{video: 2, audio: 1, videoSpan: ptsHz / 24, hasVideoPTS: true}},
		{name: "empty file", data: nil, wantErr: true},
		{name: "no PMT", data: buildTS(patPacket(), pesStart(testVideoPID, false, 0)), wantErr: true},
		{name: "lost sync", data: append(segTS(1, 0, 1), make([]byte, tsPacketSize)...), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := countTSPackets(bytes.NewReader(tc.data))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("counts = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPTSDiff(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{10, 3, 7},
		{3, 10, -7},
		{5, ptsWrap - 5, 10},
		{ptsWrap - 5, 5, -10},
	}
	for _, tc := range cases {
		if got := ptsDiff(tc.a, tc.b); got != tc.want {
			t.Errorf("ptsDiff(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// ── segExpectation / judgeSegment ────────────────────────────────────────────

func TestSegExpectation(t *testing.T) {
	cases := []struct {
		name      string
		kind      segKind
		hasVideo  bool
		videoEnd  float64
		hasAudio  bool
		start     float64
		wantVideo bool
		wantAudio bool
	}{
		{"muxed video+audio", segMuxed, true, 90, true, 56, true, true},
		{"muxed, video end unknown", segMuxed, true, 0, true, 56, true, true},
		{"muxed past the end of a short video", segMuxed, true, 60, true, 64, false, true},
		{"muxed within the slack of the video end", segMuxed, true, 88.3, true, 88, false, true},
		{"muxed video without audio", segMuxed, true, 90, false, 0, true, false},
		{"audio-only source (or cover art)", segMuxed, false, 0, true, 8, false, true},
		{"video-only segment", segVideoOnly, true, 90, true, 8, true, false},
		{"audio-only segment", segAudioOnly, true, 90, true, 8, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := segExpectation(tc.kind, tc.hasVideo, tc.videoEnd, tc.hasAudio, tc.start, 4)
			if got.video != tc.wantVideo || got.audio != tc.wantAudio || got.start != tc.start || got.dur != 4 {
				t.Errorf("segExpectation = %+v, want video=%v audio=%v", got, tc.wantVideo, tc.wantAudio)
			}
		})
	}
}

func TestJudgeSegment(t *testing.T) {
	scan := func(t *testing.T, data []byte) tsCounts {
		t.Helper()
		c, err := countTSPackets(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	av := segExpect{start: 56, dur: 4, video: true, audio: true}
	cases := []struct {
		name        string
		data        []byte
		want        segExpect
		wantOK      bool
		wantMissing bool
	}{
		{name: "healthy segment", data: segTS(96, 56.04, 10), want: av, wantOK: true},
		{name: "VFR sparse: few frames spread over the segment", data: buildTS(patPacket(), pmtPacket(h264Stream, aacStream),
			pesStart(testVideoPID, false, 56*ptsHz), pesStart(testVideoPID, false, 57.5*ptsHz), pesStart(testVideoPID, false, 59*ptsHz),
			pesStart(testAudioPID, false, -1)), want: av, wantOK: true},
		{name: "video ends early (input cut short, audio padded)", data: segTS(24, 56.0, 10), want: av},
		{name: "video ends just inside the limit", data: segTS(25, 57.0, 10), want: av, wantOK: true},
		{name: "video ends early but at the end of the source's video", data: segTS(24, 56.0, 10),
			want: segExpect{start: 56, dur: 4, videoEnd: 57, video: true, audio: true}, wantOK: true},
		{name: "video ends before the end of the source's video", data: segTS(12, 56.0, 10),
			want: segExpect{start: 56, dur: 4, videoEnd: 59, video: true, audio: true}},
		{name: "first frame just inside the late limit", data: segTS(40, 57.9, 10), want: av, wantOK: true},
		{name: "no video where none is expected (past video end)", data: segTS(0, 0, 10),
			want: segExpect{start: 64, dur: 4, audio: true}, wantOK: true},
		{name: "no video where video is expected", data: segTS(0, 0, 10), want: av, wantMissing: true},
		{name: "late video (TS seek after the keyframe)", data: segTS(23, 15.04, 10),
			want: segExpect{start: 12, dur: 4, video: true, audio: true}, wantMissing: true},
		{name: "short final segment: late limit is at least 1s", data: segTS(5, 88.9, 4),
			want: segExpect{start: 88, dur: 0.5, video: true, audio: true}, wantOK: true},
		{name: "short final segment: beyond 1s is late", data: segTS(5, 89.2, 4),
			want: segExpect{start: 88, dur: 0.5, video: true, audio: true}, wantMissing: true},
		{name: "missing audio", data: segTS(96, 56, 0), want: av},
		{name: "audio-only segment", data: segTS(0, 0, 10), want: segExpect{start: 8, dur: 4, audio: true}, wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := judgeSegment(scan(t, tc.data), tc.want)
			if (v.reason == "") != tc.wantOK || v.videoMissing != tc.wantMissing {
				t.Errorf("verdict = %+v, want ok=%v videoMissing=%v", v, tc.wantOK, tc.wantMissing)
			}
		})
	}
}

func TestInspectSegment(t *testing.T) {
	write := func(t *testing.T, data []byte) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "seg.ts")
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	want := segExpect{start: 0, dur: 4, video: true, audio: true}
	if v, err := inspectSegment(write(t, segTS(96, 0.08, 10)), want); err != nil || v.reason != "" {
		t.Errorf("healthy segment: verdict %+v, err %v", v, err)
	}
	if v, err := inspectSegment(write(t, segTS(0, 0, 10)), want); err != nil || !v.videoMissing {
		t.Errorf("videoless segment: verdict %+v, err %v; want served-uncached verdict, no error", v, err)
	}
	for name, data := range map[string][]byte{"empty": nil, "not MPEG-TS": []byte("garbage, not a transport stream")} {
		if _, err := inspectSegment(write(t, data), want); !errors.Is(err, errBadSegment) {
			t.Errorf("%s: err = %v, want errBadSegment", name, err)
		}
	}
	if _, err := inspectSegment(filepath.Join(t.TempDir(), "absent.ts"), want); err == nil || errors.Is(err, errBadSegment) {
		t.Errorf("missing file: err = %v, want a plain I/O error", err)
	}
}

// ── transcodeChecked ─────────────────────────────────────────────────────────

func TestTranscodeChecked(t *testing.T) {
	ok := segVerdict{}
	noVideo := segVerdict{reason: "no video frames", videoMissing: true}
	noAudio := segVerdict{reason: "no audio PES"}
	ffErr := errors.New("exit status 1")
	killed := errors.New("signal: killed")
	type step struct {
		encodeErr error
		inputErr  string // stderr marker reported by this encode
		verdict   segVerdict
	}
	ts := preRollPlan{first: fullPreRoll, retry: tsRetryPreRoll}
	full := preRollPlan{first: fullPreRoll}
	shrunk := preRollPlan{first: 2, retry: fullPreRoll}
	none := preRollPlan{first: 0, retry: fullPreRoll}
	cases := []struct {
		name       string
		start      float64
		pre        preRollPlan
		steps      []step // one per attempt
		inspectErr error
		cancel     bool // cancel ctx after the first inspect
		wantRolls  []float64
		wantServed int    // index of the attempt whose output is served; -1 = none
		wantReason string // "" = cache; else substring of the reason
		wantErr    error
	}{
		{name: "clean", start: 56, pre: ts, steps: []step{{verdict: ok}},
			wantRolls: []float64{10}},
		{name: "VFR/no video in MKV: served uncached, no retry", start: 56, pre: full, steps: []step{{verdict: noVideo}},
			wantRolls: []float64{10}, wantReason: "no video"},
		{name: "TS late video: retried, retry output cached", start: 56, pre: ts,
			steps:     []step{{verdict: noVideo}, {verdict: ok}},
			wantRolls: []float64{10, 30}, wantServed: 1},
		{name: "TS late video twice: first output served uncached", start: 56, pre: ts,
			steps:     []step{{verdict: noVideo}, {verdict: noVideo}},
			wantRolls: []float64{10, 30}, wantReason: "no video"},
		{name: "retry fails (timeout): first output served uncached, no error", start: 56, pre: ts,
			steps:     []step{{verdict: noVideo}, {encodeErr: killed}},
			wantRolls: []float64{10, 30}, wantReason: "no video"},
		{name: "retry logs an input error: first output served uncached", start: 56, pre: ts,
			steps:     []step{{verdict: noVideo}, {inputErr: "Stream ends prematurely", verdict: ok}},
			wantRolls: []float64{10, 30}, wantReason: "no video"},
		{name: "TS but the seek cannot move", start: 8, pre: ts, steps: []step{{verdict: noVideo}},
			wantRolls: []float64{10}, wantReason: "no video"},
		{name: "TS at exactly the pre-roll", start: 10, pre: ts, steps: []step{{verdict: noVideo}},
			wantRolls: []float64{10}, wantReason: "no video"},
		{name: "truncation marker: served uncached, no retry", start: 56, pre: ts,
			steps:     []step{{inputErr: "Stream ends prematurely", verdict: noVideo}},
			wantRolls: []float64{10}, wantReason: "Stream ends prematurely"},
		{name: "truncation marker on a full-looking segment", start: 56, pre: full,
			steps:     []step{{inputErr: "Error during demuxing", verdict: ok}},
			wantRolls: []float64{10}, wantReason: "Error during demuxing"},
		{name: "missing audio is not retried", start: 56, pre: ts, steps: []step{{verdict: noAudio}},
			wantRolls: []float64{10}, wantReason: "no audio"},
		{name: "no retry after cancellation", start: 56, pre: ts, steps: []step{{verdict: noVideo}}, cancel: true,
			wantRolls: []float64{10}, wantReason: "no video"},
		{name: "ffmpeg error returned, not inspected", start: 56, pre: ts, steps: []step{{encodeErr: ffErr}},
			wantRolls: []float64{10}, wantServed: -1, wantErr: ffErr},
		// Indexed containers with a shrunk pre-roll: the full margin is the
		// backstop for a broken or missing index.
		{name: "indexed 2s late video: retried at 10s, retry cached", start: 56, pre: shrunk,
			steps:     []step{{verdict: noVideo}, {verdict: ok}},
			wantRolls: []float64{2, 10}, wantServed: 1},
		{name: "indexed 2s still late: first output served uncached", start: 56, pre: shrunk,
			steps:     []step{{verdict: noVideo}, {verdict: noVideo}},
			wantRolls: []float64{2, 10}, wantReason: "no video"},
		{name: "indexed no pre-roll: retried at 10s", start: 4, pre: none,
			steps:     []step{{verdict: noVideo}, {verdict: ok}},
			wantRolls: []float64{0, 10}, wantServed: 1},
		{name: "indexed no pre-roll at seg0: the seek cannot move", start: 0, pre: none,
			steps: []step{{verdict: noVideo}}, wantRolls: []float64{0}, wantReason: "no video"},
		{name: "indexed 2s at exactly the pre-roll", start: 2, pre: shrunk,
			steps: []step{{verdict: noVideo}}, wantRolls: []float64{2}, wantReason: "no video"},
		{name: "indexed 2s clean", start: 56, pre: shrunk, steps: []step{{verdict: ok}},
			wantRolls: []float64{2}},
		{name: "indexed 2s missing audio is not retried", start: 56, pre: shrunk,
			steps: []step{{verdict: noAudio}}, wantRolls: []float64{2}, wantReason: "no audio"},
		{name: "unusable output is an error", start: 56, pre: ts, steps: []step{{}}, inspectErr: errBadSegment,
			wantRolls: []float64{10}, wantServed: -1, wantErr: errBadSegment},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dir := t.TempDir()
			paths := [2]string{filepath.Join(dir, "seg14.ts.tmp.ts"), filepath.Join(dir, "seg14.ts.retry.tmp.ts")}
			var rolls []float64
			attempt := map[string]int{paths[0]: 0, paths[1]: 1}
			encode := func(preRoll float64, out string) (string, error) {
				n := len(rolls)
				rolls = append(rolls, preRoll)
				if out != paths[n] {
					t.Errorf("attempt %d wrote to %q, want %q", n, out, paths[n])
				}
				st := tc.steps[n]
				if st.encodeErr != nil {
					return "", st.encodeErr // killed ffmpeg: no (complete) output
				}
				if err := os.WriteFile(out, []byte{byte(n)}, 0o600); err != nil {
					t.Fatal(err)
				}
				return st.inputErr, nil
			}
			inspect := func(path string) (segVerdict, error) {
				if tc.cancel {
					cancel()
				}
				if tc.inspectErr != nil {
					return segVerdict{}, tc.inspectErr
				}
				return tc.steps[attempt[path]].verdict, nil
			}
			served, reason, err := transcodeChecked(ctx, "seg14.ts", tc.start, tc.pre, paths, encode, inspect)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Errorf("unexpected err %v", err)
			}
			if (reason == "") != (tc.wantReason == "") || !strings.Contains(reason, tc.wantReason) {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
			if !reflect.DeepEqual(rolls, tc.wantRolls) {
				t.Errorf("pre-rolls = %v, want %v", rolls, tc.wantRolls)
			}
			if tc.wantServed < 0 {
				if served != "" {
					t.Errorf("served %q, want nothing", served)
				}
				return
			}
			if served != paths[tc.wantServed] {
				t.Errorf("served %q, want %q", served, paths[tc.wantServed])
			}
			if b, err := os.ReadFile(served); err != nil || len(b) != 1 || int(b[0]) != tc.wantServed {
				t.Errorf("served file %q content %v (err %v), want attempt %d's output", served, b, err, tc.wantServed)
			}
			other := paths[1-tc.wantServed]
			if _, err := os.Stat(other); !os.IsNotExist(err) {
				t.Errorf("unserved output %q left behind (err %v)", other, err)
			}
		})
	}
}

// ── stderrWatch ──────────────────────────────────────────────────────────────

func TestStderrWatch(t *testing.T) {
	cases := []struct {
		name   string
		writes []string
		want   string
	}{
		{"clean run", []string{"[h264 @ 0x1] co located POCs unavailable\n"}, ""},
		{"premature EOF", []string{"[http @ 0x1] Stream ends prematurely at 300000, should be 10224380\n"}, "Stream ends prematurely"},
		{"demux error", []string{"[in#0/mpegts @ 0x1] Error during demuxing: Input/output error\n"}, "Error during demuxing"},
		{"marker split across writes", []string{"noise noise [in#0 @ 0x1] Error during de", "muxing: I/O error\n"}, "Error during demuxing"},
		{"Matroska premature EOF (matroskadec)", []string{"[matroska,webm @ 0x1] File ended prematurely at pos. 300000 (10224380)\n"}, "File ended prematurely"},
		{"marker split byte by byte", strings.Split("x Stream ends prematurely y", ""), "Stream ends prematurely"},
		{"first marker wins", []string{"Stream ends prematurely\n", "Error during demuxing\n"}, "Stream ends prematurely"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var w stderrWatch
			for _, s := range tc.writes {
				if n, err := w.Write([]byte(s)); n != len(s) || err != nil {
					t.Fatalf("Write = %d, %v", n, err)
				}
			}
			if w.hit != tc.want {
				t.Errorf("hit = %q, want %q", w.hit, tc.want)
			}
		})
	}
	t.Run("bounded", func(t *testing.T) {
		var w stderrWatch
		big := bytes.Repeat([]byte("decode error spam\n"), 1<<14)
		_, _ = w.Write(big)
		if len(w.tail) >= len("Stream ends prematurely") {
			t.Errorf("tail kept %d bytes, want fewer than the longest marker", len(w.tail))
		}
	})
}

// ── installSegment ───────────────────────────────────────────────────────────

// TestInstallSegmentProvisional checks the serve-but-don't-cache path: a
// suspicious segment is served from a unique provisional path that is never
// the cached seg<n>.ts, older provisional copies are removed on the next
// transcode of that segment, and a clean transcode installs the real file.
func TestInstallSegmentProvisional(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, segLocks: map[string]*sync.Mutex{}}
	segFile := filepath.Join(dir, "seg14.ts")
	put := func(t *testing.T, reason string) string {
		t.Helper()
		tmp := segFile + ".tmp.ts"
		if err := os.WriteFile(tmp, []byte(reason+"x"), 0o600); err != nil {
			t.Fatal(err)
		}
		p, err := s.installSegment("seg14.ts", tmp, reason)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(tmp); !os.IsNotExist(err) {
			t.Errorf("tmp file still present after install (err %v)", err)
		}
		return p
	}
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }

	p1 := put(t, "no video frames")
	if p1 == segFile || !strings.HasPrefix(filepath.Base(p1), "seg14.ts.prov-") || !strings.HasSuffix(p1, ".tmp.ts") || !exists(p1) {
		t.Fatalf("suspicious segment served from %q, want an existing seg14.ts.prov-*.tmp.ts path", p1)
	}
	if exists(segFile) {
		t.Fatal("suspicious segment was installed as the cached seg14.ts")
	}
	p2 := put(t, "no video frames")
	if p2 == p1 || exists(p1) || !exists(p2) {
		t.Errorf("second provisional %q (first %q, still exists=%v); want a new path and the old one removed", p2, p1, exists(p1))
	}
	if exists(segFile) {
		t.Fatal("suspicious segment was installed as the cached seg14.ts")
	}
	p3 := put(t, "")
	if p3 != segFile || !exists(segFile) {
		t.Errorf("clean segment served from %q, want the cached %q", p3, segFile)
	}
	if exists(p2) {
		t.Errorf("provisional %q not removed once the segment was cached", p2)
	}
	if len(s.provisional["seg14.ts"]) != 0 {
		t.Errorf("provisional bookkeeping not cleared: %v", s.provisional)
	}

	// A provisional name can never be requested (or cached) as a segment.
	m := newTestHLSManager(t)
	m.sessions["s1"] = s
	s.audioStreams = []audioStream{{Index: 1}}
	for _, name := range []string{filepath.Base(p1), "seg14.ts.prov-7.tmp.ts", "a0seg14.ts.prov-7.tmp.ts", "seg14.ts.retry.tmp.ts"} {
		if _, _, err := m.HLSFile(context.Background(), "s1", name); err == nil || !strings.Contains(err.Error(), "bad") {
			t.Errorf("HLSFile(%q) err = %v, want a bad-segment error", name, err)
		}
	}
}

// ── probe ────────────────────────────────────────────────────────────────────

func TestParseStreamDuration(t *testing.T) {
	cases := []struct {
		duration, tag string
		want          float64
	}{
		{"57.049688", "", 57.049688},
		{"N/A", "00:01:28.334000000", 88.334},
		{"", "01:00:00.5", 3600.5},
		{"N/A", "", 0},
		{"0", "", 0},
		{"N/A", "garbage", 0},
		{"N/A", "00:xx:01.0", 0},
		{"N/A", "00:00:NaN", 0},
	}
	for _, tc := range cases {
		if got := parseStreamDuration(tc.duration, tc.tag); got < tc.want-1e-9 || got > tc.want+1e-9 {
			t.Errorf("parseStreamDuration(%q, %q) = %v, want %v", tc.duration, tc.tag, got, tc.want)
		}
	}
}

// TestProbeMediaVideoFields checks that probeMedia derives hasVideo and
// videoEnd from the first video stream only, treats attached cover art as
// "no video" (so audio files with embedded artwork are never expected to
// produce video), and flags MPEG-TS input and indexed containers. A PATH-shim stands in for
// ffprobe (a /bin/sh script, hence skipped on Windows).
func TestProbeMediaVideoFields(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffprobe stub is a /bin/sh script")
	}
	cases := []struct {
		name      string
		format    string
		streams   string
		wantVideo bool
		wantEnd   float64
		wantTS    bool
		wantIndex bool
	}{
		{"mpegts video", "mpegts", `{"index":0,"codec_type":"video","duration":"90.000000"},{"index":1,"codec_type":"audio"}`, true, 90, true, false},
		{"matroska DURATION tag", "matroska,webm", `{"index":0,"codec_type":"video","duration":"N/A","tags":{"DURATION":"00:01:00.000000000"}},{"index":1,"codec_type":"audio"}`, true, 60, false, true},
		{"unknown video duration", "mov,mp4,m4a,3gp,3g2,mj2", `{"index":0,"codec_type":"video"}`, true, 0, false, true},
		{"hls input", "hls", `{"index":0,"codec_type":"video"},{"index":1,"codec_type":"audio"}`, true, 0, false, false},
		{"audio only", "mp3", `{"index":0,"codec_type":"audio"}`, false, 0, false, false},
		{"mp3 with cover art", "mp3", `{"index":0,"codec_type":"audio"},{"index":1,"codec_type":"video","duration":"30.5","disposition":{"attached_pic":1}}`, false, 0, false, false},
		{"second video stream ignored", "matroska,webm", `{"index":0,"codec_type":"video","duration":"50"},{"index":1,"codec_type":"video","duration":"1","disposition":{"attached_pic":1}}`, true, 50, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stub := filepath.Join(dir, "ffprobe")
			script := "#!/bin/sh\ncat <<'EOF'\n{\"format\":{\"duration\":\"90\",\"format_name\":\"" + tc.format + "\"},\"streams\":[" + tc.streams + "]}\nEOF\n"
			if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			const self = "http://127.0.0.1:11470"
			res := probeMedia(context.Background(), self+testSelfPath, self, 5*time.Second)
			if res.duration != 90 {
				t.Fatalf("duration = %v, want 90 (stub not used?)", res.duration)
			}
			if res.hasVideo != tc.wantVideo || res.videoEnd != tc.wantEnd || res.isTS != tc.wantTS {
				t.Errorf("hasVideo, videoEnd, isTS = %v, %v, %v; want %v, %v, %v",
					res.hasVideo, res.videoEnd, res.isTS, tc.wantVideo, tc.wantEnd, tc.wantTS)
			}
			if res.seekIndexed != tc.wantIndex {
				t.Errorf("seekIndexed = %v, want %v", res.seekIndexed, tc.wantIndex)
			}
		})
	}
}

// TestTranscodeSegmentNotCachedAfterShutdown checks that once BeginShutdown
// ran, a segment that ffmpeg produced cleanly is still served but is not
// installed as seg<n>.ts: the relay feeding ffmpeg is being drained, so the
// output may be short in ways the scan cannot see.
func TestTranscodeSegmentNotCachedAfterShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffmpeg stub is a /bin/sh script")
	}
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture.ts")
	if err := os.WriteFile(fixture, segTS(0, 0, 4), 0o600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nfor a; do out=$a; done\ncp \"" + fixture + "\" \"$out\"\n"
	if err := os.WriteFile(filepath.Join(dir, "ffmpeg"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	m := newTestHLSManager(t)
	m.selfBase = "http://127.0.0.1:11470"
	s := injectSession(t, m, "shut")
	s.mediaURL = m.selfBase + testSelfPath

	p, err := m.transcodeSegment(context.Background(), s, 0, segMuxed, 0, segLive)
	if err != nil {
		t.Fatalf("transcode before shutdown: %v", err)
	}
	if p != filepath.Join(s.dir, "seg0.ts") {
		t.Errorf("before shutdown: served %q, want the cached seg0.ts", p)
	}

	m.BeginShutdown()
	p, err = m.transcodeSegment(context.Background(), s, 1, segMuxed, 0, segLive)
	if err != nil {
		t.Fatalf("transcode during shutdown: %v", err)
	}
	if p == filepath.Join(s.dir, "seg1.ts") || dirExists(filepath.Join(s.dir, "seg1.ts")) {
		t.Errorf("during shutdown: segment was cached (served %q)", p)
	}
	if !dirExists(p) {
		t.Errorf("during shutdown: served path %q does not exist", p)
	}
}

// shutdownStub is a PATH-shim ffmpeg: every call appends its argv to calls,
// a call whose argv names h264_vaapi fails, and any other call copies
// fixture to its output (the last argument) and then sleeps for delay.
func shutdownStub(calls, fixture, delay string) string {
	return "#!/bin/sh\necho \"$@\" >> \"" + calls + "\"\n" +
		"case \"$*\" in *h264_vaapi*) exit 1;; esac\n" +
		"for a; do out=$a; done\n" +
		"cp \"" + fixture + "\" \"$out\"\n" +
		"sleep " + delay + "\n"
}

// TestTranscodeSegmentShutdownRefusesFallback checks that after BeginShutdown
// a failed hardware attempt is not followed by a libx264 re-encode: the error
// is returned and nothing is cached. The control case shows that the same
// failure does fall back when the server is not shutting down.
func TestTranscodeSegmentShutdownRefusesFallback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffmpeg stub is a /bin/sh script")
	}
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture.ts")
	if err := os.WriteFile(fixture, segTS(0, 0, 0), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(dir, "calls.log")
	if err := os.WriteFile(filepath.Join(dir, "ffmpeg"), []byte(shutdownStub(calls, fixture, "0")), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	newSession := func(t *testing.T) (*hlsManager, *hlsSession) {
		t.Helper()
		_ = os.Remove(calls)
		m := newTestHLSManager(t)
		m.selfBase = "http://127.0.0.1:11470"
		m.enc = hwEncoder{codec: "h264_vaapi", isHW: true}
		s := injectSession(t, m, "fallback")
		s.mediaURL = m.selfBase + testSelfPath
		s.tc = sessionConfig{hwEnabled: true}
		return m, s
	}
	callLog := func(t *testing.T) string {
		t.Helper()
		b, err := os.ReadFile(calls)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return string(b)
	}

	t.Run("control: a failed hardware attempt falls back to libx264", func(t *testing.T) {
		m, s := newSession(t)
		if _, err := m.transcodeSegment(context.Background(), s, 0, segMuxed, 0, segLive); err != nil {
			t.Fatalf("transcode: %v", err)
		}
		if !strings.Contains(callLog(t), "libx264") {
			t.Errorf("no software fallback ran; calls:\n%s", callLog(t))
		}
		if !dirExists(filepath.Join(s.dir, "seg0.ts")) {
			t.Error("fallback output was not cached")
		}
	})

	t.Run("after BeginShutdown: no libx264 re-encode, error returned, nothing cached", func(t *testing.T) {
		m, s := newSession(t)
		m.BeginShutdown()
		if _, err := m.transcodeSegment(context.Background(), s, 1, segMuxed, 0, segLive); err == nil {
			t.Fatal("transcode succeeded after a failed hardware attempt during shutdown")
		}
		if strings.Contains(callLog(t), "libx264") {
			t.Errorf("software re-encode started during shutdown; calls:\n%s", callLog(t))
		}
		if dirExists(filepath.Join(s.dir, "seg1.ts")) {
			t.Error("segment cached during shutdown")
		}
	})
}

// TestTranscodeSegmentShutdownRefusesRetry checks that a shutdown which begins
// while the first attempt is still running refuses the longer-pre-roll retry:
// the first output is served, uncached, and ffmpeg runs only once. The
// control case runs the same segment without shutdown, so the retry path is
// known to be live.
func TestTranscodeSegmentShutdownRefusesRetry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffmpeg stub is a /bin/sh script")
	}
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture.ts")
	if err := os.WriteFile(fixture, segTS(0, 0, 4), 0o600); err != nil { // audio, no video: late-video verdict
		t.Fatal(err)
	}
	calls := filepath.Join(dir, "calls.log")
	if err := os.WriteFile(filepath.Join(dir, "ffmpeg"), []byte(shutdownStub(calls, fixture, "2")), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	newSession := func(t *testing.T, id string) (*hlsManager, *hlsSession) {
		t.Helper()
		_ = os.Remove(calls)
		m := newTestHLSManager(t)
		m.selfBase = "http://127.0.0.1:11470"
		s := injectSession(t, m, id)
		s.mediaURL = m.selfBase + testSelfPath
		s.isTS = true // MPEG-TS input: the retry plan applies
		s.hasVideo = true
		return m, s
	}
	callCount := func(t *testing.T) int {
		t.Helper()
		b, err := os.ReadFile(calls)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Count(string(b), "\n")
	}

	t.Run("control: without shutdown the retry runs", func(t *testing.T) {
		m, s := newSession(t, "retry-control")
		if _, err := m.transcodeSegment(context.Background(), s, 3, segMuxed, 0, segLive); err != nil {
			t.Fatalf("transcode: %v", err)
		}
		if n := callCount(t); n != 2 {
			t.Errorf("ffmpeg ran %d times, want 2 (first attempt and the longer-pre-roll retry)", n)
		}
	})

	t.Run("shutdown during the first attempt refuses the retry", func(t *testing.T) {
		m, s := newSession(t, "retry")
		type result struct {
			p   string
			err error
		}
		done := make(chan result, 1)
		go func() {
			p, err := m.transcodeSegment(context.Background(), s, 3, segMuxed, 0, segLive) // start 12s > the 10s pre-roll
			done <- result{p, err}
		}()

		// Begin shutdown once the first attempt has written its output, while
		// ffmpeg is still running (the stub sleeps after the copy).
		tmp := filepath.Join(s.dir, "seg3.ts.tmp.ts")
		deadline := time.Now().Add(15 * time.Second)
		for !dirExists(tmp) {
			if time.Now().After(deadline) {
				t.Fatal("first attempt never wrote its output")
			}
			time.Sleep(5 * time.Millisecond)
		}
		m.BeginShutdown()

		r := <-done
		if r.err != nil {
			t.Fatalf("transcode: %v", r.err)
		}
		if !dirExists(r.p) {
			t.Errorf("served path %q does not exist", r.p)
		}
		if dirExists(filepath.Join(s.dir, "seg3.ts")) {
			t.Error("segment cached although its first output had no video")
		}
		if n := callCount(t); n != 1 {
			t.Errorf("ffmpeg ran %d times, want 1 (the retry must be refused)", n)
		}
	})
}
