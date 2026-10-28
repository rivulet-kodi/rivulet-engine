// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// SubtitlesTracks fetches the subtitle file at subsURL and parses it into cue
// tracks. Returns map[string]interface{}{"tracks": [...]}, where each track is
// map[string]interface{}{"startTime": <ms int>, "endTime": <ms int>, "text": <string>}.
//
// Supports SRT, WEBVTT, and ASS/SSA formats. Robust to BOM and CRLF line endings.
// Image-based subs (PGS/DVDSUB) are detected as binary and return empty tracks.
// Fetch supports only http/https URLs; file://, bare paths, and every other
// scheme are rejected by validateRemoteURL before any request is made.
func (p *prober) SubtitlesTracks(subsURL string) (interface{}, error) {
	return p.SubtitlesTracksContext(context.Background(), subsURL)
}

// SubtitlesTracksContext is SubtitlesTracks with caller cancellation, bounded
// concurrency and per-URL singleflight (see ProbeContext).
func (p *prober) SubtitlesTracksContext(ctx context.Context, subsURL string) (interface{}, error) {
	return p.guard.do(ctx, "subs|"+normalizeProbeKey(subsURL), func(runCtx context.Context) (interface{}, error) {
		if err := p.guard.acquire(runCtx); err != nil {
			return nil, err
		}
		defer p.guard.release()
		data, err := fetchSubBytesCtx(runCtx, subsURL, p.baseURLLocal)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"tracks": parseSubtitles(data)}, nil
	})
}

// WriteSubtitles fetches subtitles from `from`, applies offsetMs to every
// timestamp (clamped to ≥ 0), and writes the result to w.
//
//   - ext == "vtt" → WEBVTT; timestamps use HH:MM:SS.mmm, prefixed with "WEBVTT\n\n".
//   - ext == "srt" (or anything else) → SRT; timestamps use HH:MM:SS,mmm.
//
// Ampersands in cue text are escaped as &amp;.
// ASS/SSA inputs are converted to the requested output format transparently.
func (p *prober) WriteSubtitles(w io.Writer, from, ext string, offsetMs int) error {
	tracks, err := p.fetchParsedSubs(from)
	if err != nil {
		return err
	}

	isVTT := ext == "vtt"
	ew := &errWriter{w: w}

	if isVTT {
		ew.printf("WEBVTT\n\n")
	}

	for i, t := range tracks {
		start, _ := t["startTime"].(int)
		end, _ := t["endTime"].(int)
		text, _ := t["text"].(string)

		start += offsetMs
		if start < 0 {
			start = 0
		}
		end += offsetMs
		if end < 0 {
			end = 0
		}

		text = strings.ReplaceAll(text, "&", "&amp;")

		ew.printf("%d\n", i+1)
		ew.printf("%s --> %s\n", fmtTimestamp(start, isVTT), fmtTimestamp(end, isVTT))
		ew.printf("%s\n\n", text)

		if ew.err != nil {
			return ew.err
		}
	}
	return ew.err
}

// fetchParsedSubs is the shared helper used by SubtitlesTracks and WriteSubtitles.
func (p *prober) fetchParsedSubs(url string) ([]map[string]interface{}, error) {
	data, err := fetchSubBytes(url, p.baseURLLocal)
	if err != nil {
		return nil, err
	}
	return parseSubtitles(data), nil
}

// fetchSubBytes retrieves subtitle file bytes from a validated http/https URL.
// validateRemoteURL rejects any other scheme (file://, bare path, etc.) and
// any private/loopback/link-local/cloud-metadata host, preventing both
// arbitrary local-file reads and SSRF via the subtitle-fetch path. selfBase
// exempts this server's own origin, whose endpoints the caller can already
// reach directly.
func fetchSubBytes(url, selfBase string) ([]byte, error) {
	return fetchSubBytesCtx(context.Background(), url, selfBase)
}

// fetchSubBytesCtx is fetchSubBytes bounded additionally by ctx.
func fetchSubBytesCtx(parent context.Context, url, selfBase string) ([]byte, error) {
	if err := validateRemoteURL(url, selfBase); err != nil {
		return nil, err
	}
	// Map the self-signed https://…:12470 UI origin onto plain http://…:11470:
	// the client below cannot complete that TLS handshake.
	if isSelfOrigin(url, selfBase) {
		url = localize(url)
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	// Shared transport reuses the TCP connection for subtitle CDN requests;
	// this server's own origin goes through the loopback-permitting client.
	resp, err := subClientFor(url, selfBase).Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusGone:
		return nil, fmt.Errorf("subtitle fetch: upstream status %d: %w", resp.StatusCode, types.ErrSubtitleNotFound)
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		return nil, fmt.Errorf("subtitle fetch: upstream status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 25<<20))
}

// parseSubtitles parses raw subtitle bytes into a slice of cue maps.
// Each map has keys "startTime" (int, ms), "endTime" (int, ms), "text" (string).
//
// Supported input formats:
//   - ASS/SSA: detected via [Script Info] or ScriptType: header; parsed via [Events].
//   - SRT: block-based with HH:MM:SS,mmm --> HH:MM:SS,mmm timestamps.
//   - WEBVTT: block-based with HH:MM:SS.mmm --> HH:MM:SS.mmm timestamps.
//
// Image-based subs (PGS, DVDSUB) are binary — detected and skipped gracefully
// (returns nil). These formats require OCR for text extraction, which is out of
// scope; the caller receives empty tracks rather than a crash.
func parseSubtitles(data []byte) []map[string]interface{} {
	// Detect image-based / binary subtitle formats (PGS, DVDSUB).
	// These cannot be converted to text without OCR — skip gracefully.
	if looksLikeBinary(data) {
		return nil
	}

	s := string(data)

	// Strip UTF-8 BOM.
	s = strings.TrimPrefix(s, "\xEF\xBB\xBF")

	// Normalize line endings to LF.
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")

	// ASS/SSA detection — handle before the block-based SRT/VTT parser.
	if isASS(s) {
		return parseASSSubtitles(s)
	}

	var tracks []map[string]interface{}

	// Cues (SRT or WEBVTT) are separated by blank lines.
	for _, block := range strings.Split(s, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		lines := strings.Split(block, "\n")

		// Locate the timestamp line — the one containing "-->".
		tsIdx := -1
		for i, l := range lines {
			if strings.Contains(l, "-->") {
				tsIdx = i
				break
			}
		}
		if tsIdx < 0 {
			// No timestamp: WEBVTT file header block, NOTE, STYLE, etc. — skip.
			continue
		}

		// Parse start and end timestamps.
		tsParts := strings.SplitN(lines[tsIdx], "-->", 2)
		if len(tsParts) != 2 {
			continue
		}
		start := parseTimestamp(strings.TrimSpace(tsParts[0]))
		end := parseTimestamp(strings.TrimSpace(tsParts[1]))

		// Everything after the timestamp line is cue text.
		text := strings.TrimSpace(strings.Join(lines[tsIdx+1:], "\n"))
		if text == "" {
			continue
		}

		tracks = append(tracks, map[string]interface{}{
			"startTime": start,
			"endTime":   end,
			"text":      text,
		})
	}

	return tracks
}

// looksLikeBinary reports whether data is likely a binary (image-based) subtitle.
// PGS and DVDSUB carry raw binary payloads; a null byte in the first 512 bytes
// is a reliable indicator of non-text content.
func looksLikeBinary(data []byte) bool {
	check := data
	if len(check) > 512 {
		check = check[:512]
	}
	for _, b := range check {
		if b == 0x00 {
			return true
		}
	}
	return false
}

// isASS reports whether s is an ASS/SSA subtitle file.
// Both [Script Info] (header section) and ScriptType: (variant header) are reliable
// markers; [Events] alone is also sufficient.
func isASS(s string) bool {
	// Only scan the first 2 KB — enough to find the section headers.
	head := s
	if len(head) > 2048 {
		head = head[:2048]
	}
	return strings.Contains(head, "[Script Info]") ||
		strings.Contains(head, "ScriptType: v4") ||
		strings.Contains(head, "[Events]")
}

// parseASSSubtitles converts ASS/SSA subtitle text to cue maps compatible with
// the SRT/VTT output produced by parseSubtitles.
//
// Algorithm:
//  1. Scan for [Events] section; read the Format: line to find field indices for
//     Start, End, and Text (defaults: 1, 2, 9 — the standard v4.00+ order).
//  2. For each Dialogue: line, split into exactly (maxIdx+1) comma-separated
//     parts, where maxIdx is the highest of the three tracked indices, so every
//     tracked index is always in bounds — a crafted Format: line can never
//     cause an out-of-range panic. If Text isn't the highest-indexed of the
//     three (so it isn't the part that absorbs trailing commas), the line is
//     skipped rather than risk extracting the wrong field as subtitle text.
//  3. Convert ASS timestamps (H:MM:SS.cc, centiseconds) to milliseconds.
//  4. Strip ASS override tags {…} and convert soft line-breaks (\N, \n) to
//     real newline characters.
func parseASSSubtitles(s string) []map[string]interface{} {
	// Default field indices for standard ASS v4.00+:
	// Layer(0), Start(1), End(2), Style(3), Name(4), ML(5), MR(6), MV(7), Effect(8), Text(9)
	startIdx := 1
	endIdx := 2
	textIdx := 9

	// Locate [Events] section and parse the Format: line.
	inEvents := false
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.EqualFold(trimmed, "[Events]") {
			inEvents = true
			continue
		}
		// A new [Section] header closes [Events].
		if inEvents && len(trimmed) > 0 && trimmed[0] == '[' && strings.HasSuffix(trimmed, "]") {
			break
		}
		if inEvents && strings.HasPrefix(trimmed, "Format:") {
			fields := strings.Split(strings.TrimSpace(trimmed[7:]), ",")
			for i, f := range fields {
				switch strings.TrimSpace(f) {
				case "Start":
					startIdx = i
				case "End":
					endIdx = i
				case "Text":
					textIdx = i
				}
			}
			break // Format: appears once per [Events] section
		}
	}

	// maxIdx is the highest of the three tracked indices; splitting into
	// maxIdx+1 parts guarantees parts[startIdx], parts[endIdx], and
	// parts[textIdx] are always in bounds, regardless of how a crafted or
	// unusual Format: line orders its columns (the original bug used
	// textIdx+1 alone, which panicked whenever Start or End was listed after
	// Text — e.g. "Format: Layer, Text, Start, End").
	maxIdx := startIdx
	if endIdx > maxIdx {
		maxIdx = endIdx
	}
	if textIdx > maxIdx {
		maxIdx = textIdx
	}
	nFields := maxIdx + 1 // split into this many parts; the last part absorbs any commas in Text
	// textIsLast is true only when Text is the highest-indexed of the three
	// tracked fields, i.e. the part SplitN lets absorb trailing commas is
	// actually the Text field. When Start or End is listed after Text, that
	// assumption breaks — the "last part absorbs commas" part would capture
	// the wrong field — so those lines are skipped instead of emitting
	// mis-parsed text.
	textIsLast := textIdx == maxIdx

	var tracks []map[string]interface{}
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "Dialogue:") {
			continue
		}
		// Strip "Dialogue:" prefix and leading whitespace (format: "Dialogue: …")
		rest := strings.TrimSpace(trimmed[9:])

		// Split into exactly nFields parts so Text (last field) absorbs commas.
		// Bounds are always safe: nFields is 1 + the highest tracked index.
		parts := strings.SplitN(rest, ",", nFields)
		if len(parts) < nFields {
			continue // malformed line
		}
		if !textIsLast {
			continue // Text isn't the field SplitN lets absorb commas; skip safely
		}

		start := parseASSTimestamp(strings.TrimSpace(parts[startIdx]))
		end := parseASSTimestamp(strings.TrimSpace(parts[endIdx]))

		rawText := parts[textIdx]
		text := stripASSMarkup(rawText)
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}

		tracks = append(tracks, map[string]interface{}{
			"startTime": start,
			"endTime":   end,
			"text":      text,
		})
	}
	return tracks
}

// parseASSTimestamp converts an ASS time string "H:MM:SS.cc" to milliseconds.
//
// ASS uses centiseconds (hundredths of a second) for sub-second precision:
//
//	ms = H*3600000 + MM*60000 + SS*1000 + cc*10
func parseASSTimestamp(s string) int {
	// Expected: H:MM:SS.cc  (H may be multi-digit per ASS spec)
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0
	}
	h, _ := strconv.Atoi(parts[0])
	m, _ := strconv.Atoi(parts[1])
	// parts[2] is "SS.cc"
	secParts := strings.SplitN(parts[2], ".", 2)
	sec, _ := strconv.Atoi(secParts[0])
	cs := 0
	if len(secParts) == 2 {
		cs, _ = strconv.Atoi(secParts[1])
	}
	return h*3600000 + m*60000 + sec*1000 + cs*10
}

// stripASSMarkup removes ASS inline override tags ({…}) and converts ASS
// soft line-breaks (\N and \n) to real newline characters. All other backslash
// sequences are passed through unchanged.
func stripASSMarkup(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	i := 0
	for i < len(s) {
		ch := s[i]
		switch {
		case ch == '{':
			// Scan forward to the matching '}', discarding the tag block.
			j := i + 1
			for j < len(s) && s[j] != '}' {
				j++
			}
			if j < len(s) {
				i = j + 1 // skip past '}'
			} else {
				i = j // unclosed tag — skip to end
			}
		case ch == '\\' && i+1 < len(s):
			next := s[i+1]
			if next == 'N' || next == 'n' {
				out.WriteByte('\n')
				i += 2
			} else {
				out.WriteByte(ch)
				i++
			}
		default:
			out.WriteByte(ch)
			i++
		}
	}
	return out.String()
}

// parseTimestamp converts an SRT/WEBVTT timestamp string to milliseconds.
//
// Accepted formats: HH:MM:SS,mmm  HH:MM:SS.mmm  MM:SS,mmm  MM:SS.mmm
// Optional WEBVTT cue settings that follow the timestamp (e.g. "align:left")
// are discarded.
func parseTimestamp(s string) int {
	// Discard WEBVTT cue settings (space/tab separated from the timestamp).
	if idx := strings.IndexAny(s, " \t"); idx >= 0 {
		s = s[:idx]
	}
	// Normalize the decimal separator.
	s = strings.Replace(s, ",", ".", 1)

	parts := strings.Split(s, ":")
	var h, m, sec, ms int
	switch len(parts) {
	case 3: // HH:MM:SS.mmm
		h, _ = strconv.Atoi(parts[0])
		m, _ = strconv.Atoi(parts[1])
		sec, ms = parseSecMs(parts[2])
	case 2: // MM:SS.mmm
		m, _ = strconv.Atoi(parts[0])
		sec, ms = parseSecMs(parts[1])
	}
	return h*3600000 + m*60000 + sec*1000 + ms
}

// parseSecMs splits "SS.mmm" into integral seconds and milliseconds.
func parseSecMs(s string) (sec, ms int) {
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		sec, _ = strconv.Atoi(s[:dot])
		ms, _ = strconv.Atoi(s[dot+1:])
	} else {
		sec, _ = strconv.Atoi(s)
	}
	return
}

// fmtTimestamp formats a millisecond value as HH:MM:SS,mmm (SRT) or
// HH:MM:SS.mmm (WEBVTT).
func fmtTimestamp(ms int, isVTT bool) string {
	if ms < 0 {
		ms = 0
	}
	h := ms / 3600000
	ms %= 3600000
	m := ms / 60000
	ms %= 60000
	s := ms / 1000
	ms %= 1000
	sep := ","
	if isVTT {
		sep = "."
	}
	return fmt.Sprintf("%02d:%02d:%02d%s%03d", h, m, s, sep, ms)
}

// errWriter wraps io.Writer and captures the first write error so callers can
// issue multiple writes and check once at the end.
type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) printf(format string, args ...interface{}) {
	if ew.err != nil {
		return
	}
	_, ew.err = fmt.Fprintf(ew.w, format, args...)
}
