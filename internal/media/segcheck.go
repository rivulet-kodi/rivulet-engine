// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

// Transcoded-segment validation.
//
// ffmpeg can exit 0 and still write a segment with no (or late) video: an
// input read error mid-segment (a stalled or truncated HTTP source) ends
// demuxing early without failing the run, and an input seek into an MPEG-TS
// source whose keyframe interval exceeds the seek pre-roll starts decoding
// at the keyframe *after* the seek point, which can fall late in, or after,
// the segment. transcodeSegment used to cache whatever landed on disk, so
// such a segment was served broken for the rest of the session.
//
// A suspicious segment is still served (it is what upstream served, and a
// frameless stretch can be legitimate, e.g. variable-frame-rate content that
// holds one frame for many seconds) but is not installed as seg<n>.ts, so
// the next request transcodes it again. Only an unusable output — empty or
// not MPEG-TS at all — is an error.

// errBadSegment marks a transcoded segment whose output is unusable.
var errBadSegment = errors.New("unusable segment output")

// segExpect describes what one transcoded segment should contain.
type segExpect struct {
	start, dur float64 // segment position on the media timeline, seconds
	videoEnd   float64 // where the source's video ends, seconds (<= 0 if unknown)
	video      bool    // video frames expected (source has real video that has not ended yet)
	audio      bool    // audio expected
}

// segExpectation derives segExpect for a segment of the given kind covering
// [start, start+dur). hasVideo is false for sources without real video
// (including audio files whose only video stream is attached cover art);
// videoEnd is where the source's video stream ends (seconds, <= 0 if
// unknown), so the tail of a file whose video is shorter than its audio is
// not expected to carry video.
func segExpectation(kind segKind, hasVideo bool, videoEnd float64, hasAudio bool, start, dur float64) segExpect {
	e := segExpect{start: start, dur: dur, videoEnd: videoEnd}
	e.video = hasVideo && kind != segAudioOnly && (videoEnd <= 0 || start+videoEndSlack < videoEnd)
	e.audio = kind == segAudioOnly || (kind == segMuxed && hasAudio)
	return e
}

// videoEndSlack: a segment starting within this many seconds of the end of
// the video stream is not required to carry video (the probed end is only
// approximate).
const videoEndSlack = 1.0

// lateVideo reports how far into a segment its first video frame may start
// before the segment counts as missing its video: half the segment, but at
// least a second. Healthy segments start within a frame or two of the
// segment start; the MPEG-TS seek failure starts at the next keyframe.
func lateVideo(dur float64) float64 { return math.Max(1, dur/2) }

// segVerdict is the result of inspecting a segment.
type segVerdict struct {
	reason       string // "" when the segment may be cached
	videoMissing bool   // no or late video: a longer pre-roll may help (MPEG-TS input)
}

// inspectSegment reads the MPEG-TS segment at path and judges it against
// want. It returns an error wrapping errBadSegment only when the file is
// unusable (empty, or not a transport stream with a PMT).
func inspectSegment(path string, want segExpect) (segVerdict, error) {
	f, err := os.Open(path)
	if err != nil {
		return segVerdict{}, err
	}
	defer func() { _ = f.Close() }()
	got, err := countTSPackets(bufio.NewReaderSize(f, 64*1024))
	if err != nil {
		return segVerdict{}, fmt.Errorf("%w: %w", errBadSegment, err)
	}
	return judgeSegment(got, want), nil
}

// judgeSegment applies the validation rules to scanned counts.
func judgeSegment(got tsCounts, want segExpect) segVerdict {
	if want.video {
		if got.video == 0 {
			return segVerdict{reason: "no video frames", videoMissing: true}
		}
		if got.hasVideoPTS {
			startTicks := int64(math.Round(want.start * ptsHz))
			late := float64(ptsDiff(got.videoPTS, startTicks)) / ptsHz
			if late > lateVideo(want.dur) {
				return segVerdict{reason: fmt.Sprintf("video starts %.2fs into the segment", late), videoMissing: true}
			}
			// The audio track is padded (apad) to the full segment length,
			// so a segment whose input stopped short is still full length on
			// the container level: check that the video itself reaches the
			// segment end (or the end of the source's video, if sooner).
			// With the video end unknown, the tail of a file whose video is
			// shorter than its audio is flagged too: served, not cached.
			end := want.start + want.dur
			if want.videoEnd > 0 && want.videoEnd < end {
				end = want.videoEnd
			}
			lastTicks := (got.videoPTS + got.videoSpan) % ptsWrap
			short := float64(ptsDiff(int64(math.Round(end*ptsHz)), lastTicks)) / ptsHz
			if short > lateVideo(want.dur) {
				return segVerdict{reason: fmt.Sprintf("video ends %.2fs before the segment end", short)}
			}
		}
	}
	if want.audio && got.audio == 0 {
		return segVerdict{reason: "no audio PES"}
	}
	return segVerdict{}
}

// smallSegMinSiblings is how many of the session's cached full-length video
// segments the size check needs before it judges one (HLSConfig.
// SmallSegmentPercent): 16 s of video, so the median is not one scene's.
const smallSegMinSiblings = 4

// smallSegmentReason returns why a full-length video segment of size bytes
// must not be cached next to siblings (the sizes of the session's other
// cached full-length video segments), or "" if it may be: it is smaller than
// percent% of their median, and there are at least smallSegMinSiblings of
// them. percent <= 0 turns the check off. For an even count the lower of
// the two middle sizes is the median, the more lenient choice.
//
// ffmpeg writes such a segment, with full-length video and no input error,
// when the source briefly answers with another, much smaller object (a
// debrid host's placeholder clip): seen at 2% of its siblings' size.
func smallSegmentReason(size int64, siblings []int64, percent int) string {
	if percent <= 0 || len(siblings) < smallSegMinSiblings {
		return ""
	}
	sorted := slices.Clone(siblings)
	slices.Sort(sorted)
	median := sorted[(len(sorted)-1)/2]
	if median <= 0 || size*100 >= median*int64(percent) {
		return ""
	}
	return fmt.Sprintf("segment is %d bytes, under %d%% of the median %d of %d earlier segments",
		size, percent, median, len(sorted))
}

// Input-seek pre-rolls, in seconds before the segment start.
const (
	// fullPreRoll is the historical margin, used for every container
	// without a frame-accurate input seek, and for indexed ones unless
	// HLSConfig.SeekPreroll shrinks it.
	fullPreRoll = 10.0
	// tsRetryPreRoll is the retry for MPEG-TS input whose segment came out
	// with no or late video (see transcodeChecked).
	tsRetryPreRoll = 30.0
)

// seekIndexedFormats are the ffprobe format_name families whose input seek
// lands on the keyframe at or before the target (they carry a seek index),
// so with ffmpeg's default -accurate_seek the decode is frame-accurate
// without a pre-roll margin. This is an allowlist: MPEG-TS (including
// .m2ts), HLS (unindexed MPEG-TS underneath), FLV, raw elementary streams
// and anything unknown keep the full margin.
var seekIndexedFormats = []string{"matroska", "webm", "mov", "mp4"}

// isSeekIndexed reports whether formatName (ffprobe's format_name) names
// one of seekIndexedFormats. ffprobe reports a demuxer's comma-joined
// family ("matroska,webm", "mov,mp4,m4a,3gp,3g2,mj2"), so each element is
// compared whole: a plain substring match would also accept unindexed
// demuxers such as "ipmovie" and "wc3movie".
func isSeekIndexed(formatName string) bool {
	for _, name := range strings.Split(formatName, ",") {
		if slices.Contains(seekIndexedFormats, strings.TrimSpace(name)) {
			return true
		}
	}
	return false
}

// preRollPlan is the pre-roll of a segment's first attempt and of its
// optional retry (0: no retry; see transcodeChecked).
type preRollPlan struct {
	first, retry float64
}

// segPreRollPlan picks a session's pre-rolls from its container:
//
//   - indexed (isSeekIndexed) with HLSConfig.SeekPreroll below the full
//     margin: that pre-roll first, and the full margin as the retry, the
//     backstop for a broken or missing index (e.g. a partial download). A
//     segment whose video is legitimately late (variable-frame-rate
//     content holding one frame for seconds) is retried too, and, like any
//     suspicious segment, on every request: that is the cost of the
//     backstop.
//   - MPEG-TS: the full margin, then tsRetryPreRoll.
//   - everything else, including indexed at the default: the full margin,
//     no retry. This and the MPEG-TS plan are exactly today's behaviour.
func (m *hlsManager) segPreRollPlan(isTS, seekIndexed bool) preRollPlan {
	if seekIndexed {
		if p := m.indexedPreRoll(); p < fullPreRoll {
			return preRollPlan{first: p, retry: fullPreRoll}
		}
	}
	if isTS {
		return preRollPlan{first: fullPreRoll, retry: tsRetryPreRoll}
	}
	return preRollPlan{first: fullPreRoll}
}

// indexedPreRoll is HLSConfig.SeekPreroll in seconds (SeekPrerollNone → 0).
func (m *hlsManager) indexedPreRoll() float64 {
	if m.cfg.SeekPreroll <= 0 {
		return 0
	}
	return m.cfg.SeekPreroll.Seconds()
}

// transcodeChecked runs encode (one ffmpeg transcode into the given path,
// returning the input-failure marker seen on its stderr, if any) and
// inspect (inspectSegment on that path) and decides the segment's fate. It
// returns the path to serve and, for a suspicious segment, the reason it
// must not be cached:
//
//   - encode or inspect error on the first attempt: returned as-is (ffmpeg
//     failed, or the output is unusable).
//   - clean segment: (path, "", nil) — cache it.
//   - suspicious segment: (path, reason, nil) — serve it, don't cache it.
//
// A suspicious first attempt (written to paths[0], with pre.first) is
// re-encoded once into paths[1] with pre.retry only when its video is
// missing or late, the plan has a longer retry pre-roll (MPEG-TS, where a
// seek can land after the keyframe the segment needs, or an indexed
// container with a shrunk pre-roll, whose index may be broken; see
// segPreRollPlan), no input failure was logged (re-reading a truncated
// source does not help), the longer pre-roll moves the input seek (start >
// pre.first) and ctx is still live. The retry's output replaces the
// first only if it is clean; if the retry fails, times out or is still
// suspicious, the first attempt's output is served, so a retry never turns
// a servable segment into an error. The file not served is removed.
func transcodeChecked(ctx context.Context, name string, start float64, pre preRollPlan, paths [2]string,
	encode func(preRoll float64, out string) (inputErr string, err error),
	inspect func(path string) (segVerdict, error),
) (string, string, error) {
	attempt := func(preRoll float64, out string) (segVerdict, error) {
		inputErr, err := encode(preRoll, out)
		if err != nil {
			return segVerdict{}, err
		}
		v, err := inspect(out)
		if err != nil {
			return segVerdict{}, err
		}
		if inputErr != "" {
			v = segVerdict{reason: "ffmpeg input error: " + inputErr}
		}
		return v, nil
	}
	v, err := attempt(pre.first, paths[0])
	if err != nil {
		return "", "", err
	}
	if v.reason == "" {
		return paths[0], "", nil
	}
	retry := "not applicable"
	if v.videoMissing && pre.retry > pre.first && start > pre.first && ctx.Err() == nil {
		v2, err := attempt(pre.retry, paths[1])
		switch {
		case err == nil && v2.reason == "":
			_ = os.Remove(paths[0])
			logging.For("media").Info("hls segment repaired with a longer pre-roll",
				"segment", name, "first_attempt", v.reason, "pre_roll", pre.retry)
			return paths[1], "", nil
		case err != nil:
			retry = "failed: " + err.Error()
		default:
			retry = "still suspicious: " + v2.reason
		}
		_ = os.Remove(paths[1])
	}
	logging.For("media").Warn("hls segment served but not cached",
		"segment", name, "reason", v.reason, "retry", retry)
	return paths[0], v.reason, nil
}

// ── ffmpeg stderr ─────────────────────────────────────────────────────────────

// inputErrMarkers are ffmpeg log lines that mean the input stopped short:
// the HTTP protocol's premature-EOF message, the Matroska demuxer's
// truncation message ("File ended prematurely at pos. N", logged by
// matroskadec, the Matroska/WebM demuxer) and the demuxer's
// I/O failure. ffmpeg still exits 0 after them. A false positive only costs a
// re-transcode, since it merely keeps the segment out of the cache.
var inputErrMarkers = []string{
	"Stream ends prematurely",
	"File ended prematurely",
	"Error during demuxing",
}

// stderrWatch is an io.Writer for ffmpeg's stderr that records the first
// inputErrMarkers entry it sees without buffering the stream: it keeps only
// enough trailing bytes to match a marker split across writes.
type stderrWatch struct {
	tail []byte
	hit  string
}

func (w *stderrWatch) Write(p []byte) (int, error) {
	if w.hit != "" {
		return len(p), nil
	}
	buf := append(w.tail, p...)
	for _, m := range inputErrMarkers {
		if bytes.Contains(buf, []byte(m)) {
			w.hit = m
			w.tail = nil
			return len(p), nil
		}
	}
	keep := 0
	for _, m := range inputErrMarkers {
		keep = max(keep, len(m)-1)
	}
	if len(buf) > keep {
		buf = buf[len(buf)-keep:]
	}
	w.tail = append([]byte(nil), buf...)
	return len(p), nil
}

// ── MPEG-TS scan ──────────────────────────────────────────────────────────────

const (
	tsPacketSize = 188
	ptsHz        = 90000
	ptsWrap      = int64(1) << 33
)

// ptsDiff returns a-b for 33-bit PTS values, wrapped into [-2^32, 2^32).
func ptsDiff(a, b int64) int64 {
	d := (a - b) % ptsWrap
	if d < 0 {
		d += ptsWrap
	}
	if d >= ptsWrap/2 {
		d -= ptsWrap
	}
	return d
}

// tsCounts is what countTSPackets found in a transport stream.
type tsCounts struct {
	video, audio int   // PES packets per stream class (one per frame for ffmpeg's video)
	videoPTS     int64 // earliest video PTS (90 kHz), valid if hasVideoPTS
	videoSpan    int64 // latest minus earliest video PTS (90 kHz ticks)
	hasVideoPTS  bool
}

// countTSPackets scans an MPEG-TS stream and counts the PES packets on the
// video and audio elementary streams declared in its PMT, tracking the
// earliest video PTS. A trailing partial packet is ignored. It fails if
// the data is not a transport stream carrying a PMT.
func countTSPackets(r io.Reader) (tsCounts, error) {
	type pidStats struct {
		starts         int
		firstPTS, minD int64 // earliest PTS = firstPTS + minD (wrap-safe)
		maxD           int64 // latest PTS = firstPTS + maxD
		hasPTS         bool
	}
	var (
		pkt     [tsPacketSize]byte
		pmtPIDs = map[uint16]bool{}
		kinds   = map[uint16]byte{} // elementary PID → 'v' / 'a'
		stats   = map[uint16]*pidStats{}
		sawPMT  bool
	)
	for {
		if _, err := io.ReadFull(r, pkt[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return tsCounts{}, err
		}
		if pkt[0] != 0x47 {
			return tsCounts{}, errors.New("lost MPEG-TS sync")
		}
		pusi := pkt[1]&0x40 != 0
		pid := uint16(pkt[1]&0x1f)<<8 | uint16(pkt[2])
		afc := (pkt[3] >> 4) & 0x3
		if afc&0x1 == 0 {
			continue // adaptation field only, no payload
		}
		off := 4
		if afc == 0x3 {
			off += 1 + int(pkt[4])
		}
		if off >= tsPacketSize || !pusi {
			continue
		}
		payload := pkt[off:]
		switch {
		case pid == 0:
			for _, p := range parsePAT(payload) {
				pmtPIDs[p] = true
			}
		case pmtPIDs[pid]:
			if parsePMT(payload, kinds) {
				sawPMT = true
			}
		case len(payload) >= 3 && payload[0] == 0 && payload[1] == 0 && payload[2] == 1:
			st := stats[pid]
			if st == nil {
				st = &pidStats{}
				stats[pid] = st
			}
			st.starts++
			if pts, ok := pesPTS(payload); ok {
				if !st.hasPTS {
					st.firstPTS, st.hasPTS = pts, true
				} else {
					d := ptsDiff(pts, st.firstPTS)
					st.minD = min(st.minD, d)
					st.maxD = max(st.maxD, d)
				}
			}
		}
	}
	if !sawPMT {
		return tsCounts{}, errors.New("no PMT in segment")
	}
	var c tsCounts
	for pid, st := range stats {
		switch kinds[pid] {
		case 'v':
			c.video += st.starts
			if st.hasPTS && !c.hasVideoPTS { // ffmpeg writes a single video stream
				c.videoPTS = (st.firstPTS + st.minD + ptsWrap) % ptsWrap
				c.videoSpan = st.maxD - st.minD
				c.hasVideoPTS = true
			}
		case 'a':
			c.audio += st.starts
		}
	}
	return c, nil
}

// pesPTS extracts the PTS from a PES header at the start of payload.
func pesPTS(p []byte) (int64, bool) {
	if len(p) < 14 || p[6]&0xc0 != 0x80 || p[7]&0x80 == 0 {
		return 0, false
	}
	pts := int64(p[9]>>1&0x07)<<30 | int64(p[10])<<22 | int64(p[11]>>1)<<15 |
		int64(p[12])<<7 | int64(p[13]>>1)
	return pts, true
}

// psiSection returns the body of the PSI section starting in payload (after
// the pointer field) with the given table_id, up to but excluding its CRC,
// or nil if it is absent, malformed or does not fit in one packet.
func psiSection(payload []byte, tableID byte) []byte {
	if len(payload) < 1 {
		return nil
	}
	p := 1 + int(payload[0]) // skip pointer_field + filler
	if p+3 > len(payload) || payload[p] != tableID {
		return nil
	}
	secLen := int(payload[p+1]&0x0f)<<8 | int(payload[p+2])
	end := p + 3 + secLen - 4 // exclude CRC_32
	if secLen < 9 || end > len(payload) {
		return nil
	}
	return payload[p+8 : end] // skip the 8-byte long-form section header
}

// parsePAT returns the PMT PIDs listed in a PAT section.
func parsePAT(payload []byte) []uint16 {
	body := psiSection(payload, 0x00)
	var pids []uint16
	for i := 0; i+4 <= len(body); i += 4 {
		program := uint16(body[i])<<8 | uint16(body[i+1])
		if program == 0 {
			continue // network PID, not a PMT
		}
		pids = append(pids, uint16(body[i+2]&0x1f)<<8|uint16(body[i+3]))
	}
	return pids
}

// parsePMT records the video/audio elementary PIDs of a PMT section in
// kinds and reports whether the section parsed.
func parsePMT(payload []byte, kinds map[uint16]byte) bool {
	body := psiSection(payload, 0x02)
	if len(body) < 4 {
		return false
	}
	i := 4 + (int(body[2]&0x0f)<<8 | int(body[3])) // PCR_PID, program_info_length
	for i+5 <= len(body) {
		st := body[i]
		pid := uint16(body[i+1]&0x1f)<<8 | uint16(body[i+2])
		switch st {
		case 0x01, 0x02, 0x10, 0x1b, 0x24, 0x42, 0xd1, 0xea: // MPEG-1/2, MPEG-4 part 2, H.264, HEVC, AVS, Dirac, VC-1
			kinds[pid] = 'v'
		case 0x03, 0x04, 0x0f, 0x11, 0x81, 0x82, 0x87: // MPEG audio, AAC (ADTS/LATM), AC-3, DTS, E-AC-3
			kinds[pid] = 'a'
		}
		i += 5 + (int(body[i+3]&0x0f)<<8 | int(body[i+4]))
	}
	return true
}

// ── probe helpers ─────────────────────────────────────────────────────────────

// parseStreamDuration returns an ffprobe stream's duration in seconds from
// its "duration" field or, failing that (Matroska), its DURATION tag
// ("HH:MM:SS.nnnnnnnnn"); 0 if neither parses.
func parseStreamDuration(duration, tag string) float64 {
	if d, err := strconv.ParseFloat(duration, 64); err == nil && d > 0 && !math.IsInf(d, 0) {
		return d
	}
	h, rest, ok1 := strings.Cut(tag, ":")
	m, s, ok2 := strings.Cut(rest, ":")
	if !ok1 || !ok2 {
		return 0
	}
	hv, err1 := strconv.Atoi(h)
	mv, err2 := strconv.Atoi(m)
	sv, err3 := strconv.ParseFloat(s, 64)
	if err1 != nil || err2 != nil || err3 != nil || hv < 0 || mv < 0 || !(sv >= 0) {
		return 0
	}
	return float64(hv*3600+mv*60) + sv
}
