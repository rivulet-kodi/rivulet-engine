// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

// XMLTV programme guide for the /iptv add-on (RIVULET_IPTV_EPG).
//
// Each guide URL is loaded in the background by a streaming parse
// (encoding/xml token decoder; gzip detected by magic bytes), keeping only the
// <programme>s whose channel matches a configured channel's tvg-id and that
// overlap [now-iptvEPGPast, now+iptvEPGFuture). The result is cached for
// iptvTTL and kept when a refresh fails. A request waits at most
// iptvEPGFirstWait for the very first load and otherwise answers without
// programmes. Memory is bounded by the caps below, not by the guide size.
//
// A programme becomes a stremio-core Video with VideoEpgInfo flattened onto
// it (src/types/resource/meta_item.rs): id "<channelId>:<startUnix>", title,
// released = startTime, startTime/endTime (RFC 3339), overview, thumbnail,
// genres, releaseInfo.

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

const (
	iptvEPGFetchTimeout  = 3 * time.Minute
	iptvEPGMaxCompressed = 64 << 20  // gzip body cap
	iptvEPGMaxBytes      = 512 << 20 // decoded XML cap (also the cap for a plain body)
	iptvEPGPast          = 24 * time.Hour
	iptvEPGFuture        = 48 * time.Hour
	iptvEPGMaxPerChannel = 300
	iptvEPGMaxProgrammes = 300000 // retained programmes over all channels
	iptvEPGMaxTitle      = 200
	iptvEPGMaxDesc       = 1000
	iptvEPGMaxGenres     = 8
	iptvEPGMaxGenreLen   = 60
)

// iptvEPGFirstWait bounds how long a request waits for a first guide load; a
// variable so tests need not sit out two seconds.
var iptvEPGFirstWait = 2 * time.Second

var iptvEPGClient = &http.Client{Timeout: iptvEPGFetchTimeout}

// iptvProgramme is one scheduled programme of a channel.
type iptvProgramme struct {
	Start, End time.Time
	Title      string // "Title - Subtitle" when the guide has a sub-title
	Desc       string
	Icon       string
	Episode    string // releaseInfo
	Genres     []string
}

// video renders the programme as a stremio-core Video with epg info.
func (p *iptvProgramme) video(channelID string) map[string]any {
	start := p.Start.UTC().Format(time.RFC3339)
	v := map[string]any{
		"id":        channelID + ":" + strconv.FormatInt(p.Start.Unix(), 10),
		"title":     p.Title,
		"released":  start,
		"startTime": start,
		"endTime":   p.End.UTC().Format(time.RFC3339),
	}
	if p.Desc != "" {
		v["overview"] = p.Desc
	}
	if p.Icon != "" {
		v["thumbnail"] = p.Icon
	}
	if p.Episode != "" {
		v["releaseInfo"] = p.Episode
	}
	if len(p.Genres) > 0 {
		v["genres"] = p.Genres
	}
	return v
}

// iptvEPGKey is the form of a tvg-id / XMLTV channel id programmes are
// matched by: trimmed, case-insensitive.
func iptvEPGKey(id string) string { return strings.ToLower(strings.TrimSpace(id)) }

// iptvTVGSig fingerprints the set of tvg-ids of a playlist (order-free), so a
// guide is reloaded when a playlist refresh changes which channels it needs.
func iptvTVGSig(chs []iptvChannel) string {
	var x uint64
	n := 0
	for i := range chs {
		if chs[i].TVGID == "" {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(iptvEPGKey(chs[i].TVGID)))
		x ^= h.Sum64()
		n++
	}
	return strconv.Itoa(n) + ":" + strconv.FormatUint(x, 16)
}

// ---- cache -----------------------------------------------------------------

// iptvEPGSource is the cached state of one configured guide URL.
type iptvEPGSource struct {
	mu      sync.Mutex // guards the fields below
	progs   map[string][]iptvProgramme
	fetched time.Time     // last successful load
	failed  time.Time     // last failed load
	sig     string        // channel fingerprint progs was filtered with
	loading chan struct{} // non-nil, closed on completion, while a load runs
}

var iptvEPGSources = map[string]*iptvEPGSource{} // guarded by iptvMu

func iptvEPGSourceFor(u string) *iptvEPGSource {
	iptvMu.Lock()
	defer iptvMu.Unlock()
	e := iptvEPGSources[u]
	if e == nil {
		e = &iptvEPGSource{}
		iptvEPGSources[u] = e
	}
	return e
}

// ensure starts a background load when the cache is stale (older than
// iptvTTL, or built for other channels than sig) and no failed load is
// recent. It returns a channel to wait on while no load has ever finished,
// nil otherwise (stale data is served as-is).
func (e *iptvEPGSource) ensure(u, sig string, wanted func() map[string]struct{}) <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	if e.loading == nil && (e.fetched.IsZero() || now.Sub(e.fetched) >= iptvTTL || e.sig != sig) && now.Sub(e.failed) >= iptvRetryAfter {
		e.loading = make(chan struct{})
		go e.load(u, sig, wanted, e.loading)
	}
	if e.fetched.IsZero() {
		return e.loading
	}
	return nil
}

func (e *iptvEPGSource) load(u, sig string, wanted func() map[string]struct{}, done chan struct{}) {
	ids := wanted()
	progs := map[string][]iptvProgramme{}
	var (
		truncated bool
		err       error
	)
	if len(ids) > 0 { // nothing to match: skip downloading the guide
		ctx, cancel := context.WithTimeout(context.Background(), iptvEPGFetchTimeout)
		progs, truncated, err = iptvEPGFetch(ctx, u, ids, time.Now())
		cancel()
	}
	now := time.Now()
	e.mu.Lock()
	e.loading = nil
	if err != nil {
		e.failed = now
	} else {
		e.progs, e.fetched, e.failed, e.sig = progs, now, time.Time{}, sig
	}
	e.mu.Unlock()
	close(done)
	if err != nil {
		logging.For("iptv").Warn("guide load failed", "url", redactURL(u), "err", err)
		return
	}
	n := 0
	for _, ps := range progs {
		n += len(ps)
	}
	logging.For("iptv").Info("guide loaded", "url", redactURL(u), "channels", len(progs), "programmes", n, "truncated", truncated)
}

func (e *iptvEPGSource) snapshot() map[string][]iptvProgramme {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.progs
}

// iptvGuide is the programme lookup over the configured guide sources; the
// zero value (no guide configured) has no programmes.
type iptvGuide struct {
	enabled bool
	srcs    []map[string][]iptvProgramme
}

// programmes returns the channel's programmes sorted by start time, nil when
// it has no tvg-id or none in the guide.
func (g iptvGuide) programmes(c *iptvChannel) []iptvProgramme {
	if c.TVGID == "" {
		return nil
	}
	key := iptvEPGKey(c.TVGID)
	var out []iptvProgramme
	for _, src := range g.srcs {
		ps := src[key]
		switch {
		case len(ps) == 0:
		case out == nil:
			out = ps
		default: // several guides cover the channel: merge, first start wins
			out = slices.Concat(out, ps)
			slices.SortStableFunc(out, func(a, b iptvProgramme) int { return a.Start.Compare(b.Start) })
			out = slices.CompactFunc(out, func(a, b iptvProgramme) bool { return a.Start.Equal(b.Start) })
		}
	}
	return out
}

// iptvGuide returns the guide for the configured RIVULET_IPTV_EPG sources,
// starting any needed background load. With wait it holds the request for a
// first load for at most iptvEPGFirstWait.
func (s *server) iptvGuide(r *http.Request, wait bool) iptvGuide {
	if len(s.cfg.IPTVEPG) == 0 {
		return iptvGuide{}
	}
	var (
		inputs [][]iptvChannel
		sigs   []string
	)
	for _, u := range s.cfg.IPTVPlaylists {
		chs, sig := iptvList(iptvCtx(r), u).guideInput()
		inputs = append(inputs, chs)
		sigs = append(sigs, sig)
	}
	sig := strings.Join(sigs, ",")
	wanted := func() map[string]struct{} {
		ids := map[string]struct{}{}
		for _, chs := range inputs {
			for i := range chs {
				if chs[i].TVGID != "" {
					ids[iptvEPGKey(chs[i].TVGID)] = struct{}{}
				}
			}
		}
		return ids
	}
	g := iptvGuide{enabled: true}
	var waits []<-chan struct{}
	srcs := make([]*iptvEPGSource, len(s.cfg.IPTVEPG))
	for i, u := range s.cfg.IPTVEPG {
		srcs[i] = iptvEPGSourceFor(u)
		if ch := srcs[i].ensure(u, sig, wanted); ch != nil {
			waits = append(waits, ch)
		}
	}
	if wait && len(waits) > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), iptvEPGFirstWait)
		defer cancel()
	loop:
		for _, ch := range waits {
			select {
			case <-ch:
			case <-ctx.Done():
				break loop
			}
		}
	}
	for _, src := range srcs {
		if p := src.snapshot(); len(p) > 0 {
			g.srcs = append(g.srcs, p)
		}
	}
	return g
}

// ---- fetch + parse ---------------------------------------------------------

var errIPTVEPGTooLarge = errors.New("guide exceeds the size cap")

// iptvCapReader fails with errIPTVEPGTooLarge once more than max bytes have
// been read, so an oversized guide is rejected instead of silently cut.
type iptvCapReader struct {
	r   io.Reader
	n   int64
	max int64
}

func (c *iptvCapReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if c.n > c.max {
		return 0, errIPTVEPGTooLarge
	}
	return n, err
}

func iptvEPGFetch(ctx context.Context, u string, wanted map[string]struct{}, now time.Time) (map[string][]iptvProgramme, bool, error) {
	resp, err := iptvGet(ctx, iptvEPGClient, u, "guide")
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	br := bufio.NewReaderSize(resp.Body, 64<<10)
	var rd io.Reader = &iptvCapReader{r: br, max: iptvEPGMaxBytes}
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, err := gzip.NewReader(&iptvCapReader{r: br, max: iptvEPGMaxCompressed})
		if err != nil {
			return nil, false, fmt.Errorf("gzip guide: %w", err)
		}
		defer zr.Close()
		rd = &iptvCapReader{r: zr, max: iptvEPGMaxBytes}
	}
	return parseXMLTV(rd, wanted, now)
}

// xmltvProgramme holds the children of a <programme> the guide uses.
type xmltvProgramme struct {
	Titles     []string `xml:"title"`
	SubTitles  []string `xml:"sub-title"`
	Descs      []string `xml:"desc"`
	Categories []string `xml:"category"`
	Icons      []struct {
		Src string `xml:"src,attr"`
	} `xml:"icon"`
	Episodes []struct {
		System string `xml:"system,attr"`
		Text   string `xml:",chardata"`
	} `xml:"episode-num"`
}

// parseXMLTV streams an XMLTV document and returns, per lower-cased channel
// id in wanted, the programmes overlapping [now-iptvEPGPast, now+iptvEPGFuture)
// sorted by start (at most iptvEPGMaxPerChannel each, iptvEPGMaxProgrammes in
// total; truncated reports that the total cap stopped the parse).
func parseXMLTV(rd io.Reader, wanted map[string]struct{}, now time.Time) (progs map[string][]iptvProgramme, truncated bool, err error) {
	winStart, winEnd := now.Add(-iptvEPGPast), now.Add(iptvEPGFuture)
	d := xml.NewDecoder(rd)
	d.Strict = false
	d.CharsetReader = iptvCharset
	progs = map[string][]iptvProgramme{}
	total := 0
	for {
		tok, err := d.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, false, fmt.Errorf("parse guide: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if se.Name.Local == "tv" {
			continue
		}
		if se.Name.Local != "programme" {
			if err := d.Skip(); err != nil {
				return nil, false, fmt.Errorf("parse guide: %w", err)
			}
			continue
		}
		var channel, start, stop string
		for _, a := range se.Attr {
			switch a.Name.Local {
			case "channel":
				channel = a.Value
			case "start":
				start = a.Value
			case "stop":
				stop = a.Value
			}
		}
		key := iptvEPGKey(channel)
		st, okStart := parseXMLTVTime(start)
		en, okStop := parseXMLTVTime(stop)
		_, match := wanted[key]
		if !match || !okStart || !st.Before(winEnd) || (okStop && !en.After(winStart)) || len(progs[key]) >= iptvEPGMaxPerChannel {
			if err := d.Skip(); err != nil {
				return nil, false, fmt.Errorf("parse guide: %w", err)
			}
			continue
		}
		var x xmltvProgramme
		if err := d.DecodeElement(&x, &se); err != nil {
			return nil, false, fmt.Errorf("parse guide: %w", err)
		}
		if p, ok := x.programme(st, en, okStop); ok {
			progs[key] = append(progs[key], p)
			if total++; total >= iptvEPGMaxProgrammes {
				truncated = true
				break
			}
		}
	}
	for key, ps := range progs {
		if ps = finishProgrammes(ps); len(ps) == 0 {
			delete(progs, key)
		} else {
			progs[key] = ps
		}
	}
	return progs, truncated, nil
}

// finishProgrammes sorts by start, drops repeated starts and gives a
// programme without a stop time the next one's start (the last one without a
// stop is dropped: its length is unknown).
func finishProgrammes(ps []iptvProgramme) []iptvProgramme {
	slices.SortStableFunc(ps, func(a, b iptvProgramme) int { return a.Start.Compare(b.Start) })
	ps = slices.CompactFunc(ps, func(a, b iptvProgramme) bool { return a.Start.Equal(b.Start) })
	out := ps[:0]
	for i := range ps {
		if ps[i].End.IsZero() {
			if i+1 == len(ps) {
				continue
			}
			ps[i].End = ps[i+1].Start
		}
		if ps[i].End.After(ps[i].Start) {
			out = append(out, ps[i])
		}
	}
	return slices.Clip(out)
}

// programme maps the XMLTV children to a programme; ok is false without a
// title. A zero end means the stop time is unknown.
func (x *xmltvProgramme) programme(start, end time.Time, hasEnd bool) (iptvProgramme, bool) {
	p := iptvProgramme{Start: start}
	if hasEnd {
		p.End = end
	}
	if len(x.Titles) == 0 || strings.TrimSpace(x.Titles[0]) == "" {
		return p, false
	}
	p.Title = clipText(x.Titles[0], iptvEPGMaxTitle)
	if len(x.SubTitles) > 0 {
		if sub := clipText(x.SubTitles[0], iptvEPGMaxTitle); sub != "" {
			p.Title += " - " + sub
		}
	}
	if len(x.Descs) > 0 {
		p.Desc = clipText(x.Descs[0], iptvEPGMaxDesc)
	}
	if len(x.Icons) > 0 {
		p.Icon = strings.TrimSpace(x.Icons[0].Src)
	}
	for _, c := range x.Categories {
		if c = clipText(c, iptvEPGMaxGenreLen); c != "" && len(p.Genres) < iptvEPGMaxGenres && !slices.Contains(p.Genres, c) {
			p.Genres = append(p.Genres, c)
		}
	}
	for _, e := range x.Episodes {
		switch e.System {
		case "onscreen":
			p.Episode = clipText(e.Text, iptvEPGMaxGenreLen)
		case "xmltv_ns":
			if p.Episode == "" {
				p.Episode = xmltvNSEpisode(e.Text)
			}
		}
		if e.System == "onscreen" && p.Episode != "" {
			break // onscreen wins over xmltv_ns
		}
	}
	return p, true
}

// clipText trims s and cuts it to at most n bytes on a rune boundary; the
// result never aliases a larger backing string.
func clipText(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return strings.Clone(s[:n])
}

// xmltvNSEpisode renders an xmltv_ns "season.episode.part" (zero-based, each
// part possibly "n/total") as "S1E6", "E6"; "" when the episode is unknown.
func xmltvNSEpisode(s string) string {
	parts := strings.Split(s, ".")
	num := func(i int) int {
		if i >= len(parts) {
			return -1
		}
		v, _, _ := strings.Cut(parts[i], "/")
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n < 0 {
			return -1
		}
		return n + 1
	}
	season, ep := num(0), num(1)
	switch {
	case ep < 0:
		return ""
	case season < 0:
		return "E" + strconv.Itoa(ep)
	}
	return "S" + strconv.Itoa(season) + "E" + strconv.Itoa(ep)
}

// parseXMLTVTime parses an XMLTV time, "20261105200000 +0100": YYYYMMDD
// optionally followed by HHMM[SS], then an optional zone (±HHMM, ±HH:MM, ±HH,
// Z, UTC or GMT; none means UTC). The result is in UTC.
func parseXMLTVTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	i := strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' })
	digits, zone := s, ""
	if i >= 0 {
		digits, zone = s[:i], strings.TrimSpace(s[i:])
	}
	if len(digits) != 8 && len(digits) != 12 && len(digits) != 14 {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102150405", digits+"000000"[:14-len(digits)])
	if err != nil {
		return time.Time{}, false
	}
	switch strings.ToUpper(zone) {
	case "", "Z", "UTC", "GMT":
		return t, true
	}
	sign := 1
	switch zone[0] {
	case '+':
	case '-':
		sign = -1
	default:
		return time.Time{}, false
	}
	z := strings.ReplaceAll(zone[1:], ":", "")
	if len(z) == 2 {
		z += "00"
	}
	hh, err1 := strconv.Atoi(z[:min(2, len(z))])
	mm, err2 := strconv.Atoi(z[min(2, len(z)):])
	if len(z) != 4 || err1 != nil || err2 != nil {
		return time.Time{}, false
	}
	return t.Add(-time.Duration(sign) * (time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute)), true
}

// iptvCharset lets the XML decoder read the Latin-1 guides some providers
// still publish; every other non-UTF-8 charset is rejected.
func iptvCharset(label string, in io.Reader) (io.Reader, error) {
	switch strings.ToLower(label) {
	case "utf-8", "utf8", "us-ascii", "ascii":
		return in, nil
	case "iso-8859-1", "latin1", "windows-1252":
		return &latin1Reader{r: in}, nil
	}
	return nil, fmt.Errorf("unsupported guide charset %q", label)
}

// latin1Reader transcodes ISO-8859-1 to UTF-8 (windows-1252 is treated alike).
type latin1Reader struct {
	r   io.Reader
	buf []byte
}

func (l *latin1Reader) Read(p []byte) (int, error) {
	if len(p) < 2 {
		return 0, io.ErrShortBuffer
	}
	n := len(p) / 2
	if cap(l.buf) < n {
		l.buf = make([]byte, n)
	}
	in, err := l.r.Read(l.buf[:n])
	out := 0
	for _, b := range l.buf[:in] {
		if b < 0x80 {
			p[out] = b
			out++
			continue
		}
		p[out], p[out+1] = 0xC0|b>>6, 0x80|b&0x3F
		out += 2
	}
	return out, err
}
