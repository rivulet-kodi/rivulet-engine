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
// programmes. Memory is bounded by the caps below, not by the guide size:
// a text node or tag over iptvEPGMaxTextRun fails the load (encoding/xml
// buffers each one whole), each programme keeps a bounded set of fields, and
// at most iptvEPGMaxPerChannel / iptvEPGMaxProgrammes programmes are retained.
//
// A programme becomes a stremio-core Video with VideoEpgInfo flattened onto
// it (src/types/resource/meta_item.rs): id "<channelId>:<startUnix>", title,
// released = startTime, startTime/endTime (RFC 3339), overview, thumbnail,
// genres, releaseInfo.

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
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
	iptvEPGPast          = 24 * time.Hour
	iptvEPGFuture        = 48 * time.Hour
	iptvEPGMaxPerChannel = 300
	iptvEPGMaxTitle      = 200
	iptvEPGMaxDesc       = 1000
	iptvEPGMaxIcon       = 500
	iptvEPGMaxGenres     = 8
	iptvEPGMaxGenreLen   = 60
)

// The caps below are variables only so tests can lower them.
var (
	iptvEPGMaxCompressed = int64(64 << 20)  // gzip body cap
	iptvEPGMaxBytes      = int64(512 << 20) // decoded XML cap (also the cap for a plain body)
	iptvEPGMaxProgrammes = 300000           // retained programmes over all channels
	// iptvEPGMaxTextRun caps the bytes between two '<' (a text node, or a tag
	// with its attributes): encoding/xml buffers a whole one in memory.
	iptvEPGMaxTextRun = 256 << 10
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

// iptvTVGSig fingerprints the set of distinct tvg-ids of a playlist (the
// sorted, lower-cased ids hashed together), so a guide is reloaded exactly when
// a playlist refresh changes which channels it needs.
func iptvTVGSig(chs []iptvChannel) string {
	ids := make([]string, 0, len(chs))
	for i := range chs {
		if chs[i].TVGID != "" {
			ids = append(ids, iptvEPGKey(chs[i].TVGID))
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	h := sha256.New()
	for _, id := range ids {
		_, _ = h.Write([]byte(id))
		_, _ = h.Write([]byte{'\n'})
	}
	return strconv.Itoa(len(ids)) + ":" + hex.EncodeToString(h.Sum(nil)[:12])
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
		st  iptvEPGStats
		err error
	)
	if len(ids) > 0 { // nothing to match: skip downloading the guide
		ctx, cancel := context.WithTimeout(context.Background(), iptvEPGFetchTimeout)
		progs, st, err = iptvEPGFetch(ctx, u, ids, time.Now())
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
	logging.For("iptv").Info("guide loaded", "url", redactURL(u), "channels", len(progs), "programmes", n)
	if st.DroppedTotal > 0 || st.DroppedPerChannel > 0 {
		logging.For("iptv").Warn("guide programmes dropped by the caps", "url", redactURL(u),
			"over_total_cap", st.DroppedTotal, "channels_left_empty", st.ChannelsEmpty, "over_channel_cap", st.DroppedPerChannel)
	}
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

var (
	errIPTVEPGTooLarge = errors.New("guide exceeds the size cap")
	errIPTVEPGTextRun  = errors.New("guide has an oversized text node")
)

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

// iptvRunReader fails with errIPTVEPGTextRun when more than max bytes pass
// without a '<'. encoding/xml buffers a whole text node (even one that is
// skipped) and a whole tag, so this bounds its per-token memory.
type iptvRunReader struct {
	r   io.Reader
	run int
	max int
}

func (c *iptvRunReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	for rest := p[:n]; ; {
		i := bytes.IndexByte(rest, '<')
		if i < 0 {
			c.run += len(rest)
			break
		}
		if c.run+i > c.max {
			return 0, errIPTVEPGTextRun
		}
		c.run, rest = 0, rest[i+1:]
	}
	if c.run > c.max {
		return 0, errIPTVEPGTextRun
	}
	return n, err
}

// iptvEPGStats reports what the caps cost a load.
type iptvEPGStats struct {
	DroppedTotal      int // matching programmes dropped by iptvEPGMaxProgrammes
	ChannelsEmpty     int // channels that got none because of it (the ones later in the file)
	DroppedPerChannel int // matching programmes dropped by iptvEPGMaxPerChannel
}

func iptvEPGFetch(ctx context.Context, u string, wanted map[string]struct{}, now time.Time) (map[string][]iptvProgramme, iptvEPGStats, error) {
	resp, err := iptvGet(ctx, iptvEPGClient, u, "guide")
	if err != nil {
		return nil, iptvEPGStats{}, err
	}
	defer resp.Body.Close()
	br := bufio.NewReaderSize(resp.Body, 64<<10)
	var rd io.Reader = &iptvCapReader{r: br, max: iptvEPGMaxBytes}
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, err := gzip.NewReader(&iptvCapReader{r: br, max: iptvEPGMaxCompressed})
		if err != nil {
			return nil, iptvEPGStats{}, fmt.Errorf("gzip guide: %w", err)
		}
		defer func() { _ = zr.Close() }() // reader only: Close has nothing to flush
		rd = &iptvCapReader{r: zr, max: iptvEPGMaxBytes}
	}
	return parseXMLTV(&iptvRunReader{r: rd, max: iptvEPGMaxTextRun}, wanted, now)
}

// xmltvProgramme holds what is read of a <programme>'s children; everything
// is bounded (first title / sub-title / desc / icon, a few categories and
// episode numbers), however many elements the guide has.
type xmltvProgramme struct {
	title, subTitle, desc, icon string
	categories                  []string
	onscreen, xmltvNS           string
}

// readProgramme reads the children of the <programme> start element se up to
// its end tag, keeping only the bounded fields of xmltvProgramme.
func readProgramme(d *xml.Decoder) (x xmltvProgramme, err error) {
	for {
		tok, err := d.Token()
		if err != nil {
			return x, err
		}
		switch t := tok.(type) {
		case xml.EndElement:
			return x, nil
		case xml.StartElement:
			switch t.Name.Local {
			case "icon":
				if x.icon == "" {
					for _, a := range t.Attr {
						if a.Name.Local == "src" {
							x.icon = clipText(a.Value, iptvEPGMaxIcon)
						}
					}
				}
				err = d.Skip()
			case "title", "sub-title", "desc", "category", "episode-num":
				var text string
				if text, err = readText(d, 4*iptvEPGMaxDesc); err == nil {
					x.set(t, text)
				}
			default:
				err = d.Skip()
			}
			if err != nil {
				return x, err
			}
		}
	}
}

// set stores the text of one child element (first value wins, categories and
// episode numbers up to their caps).
func (x *xmltvProgramme) set(se xml.StartElement, text string) {
	switch se.Name.Local {
	case "title":
		if x.title == "" {
			x.title = clipText(text, iptvEPGMaxTitle)
		}
	case "sub-title":
		if x.subTitle == "" {
			x.subTitle = clipText(text, iptvEPGMaxTitle)
		}
	case "desc":
		if x.desc == "" {
			x.desc = clipText(text, iptvEPGMaxDesc)
		}
	case "category":
		if c := clipText(text, iptvEPGMaxGenreLen); c != "" && len(x.categories) < iptvEPGMaxGenres && !slices.Contains(x.categories, c) {
			x.categories = append(x.categories, c)
		}
	case "episode-num":
		system := ""
		for _, a := range se.Attr {
			if a.Name.Local == "system" {
				system = a.Value
			}
		}
		switch system {
		case "onscreen":
			if x.onscreen == "" {
				x.onscreen = clipText(text, iptvEPGMaxGenreLen)
			}
		case "xmltv_ns":
			if x.xmltvNS == "" {
				x.xmltvNS = xmltvNSEpisode(text)
			}
		}
	}
}

// readText collects the character data of the element just opened, up to its
// end tag, keeping at most limit bytes; nested elements are skipped.
func readText(d *xml.Decoder, limit int) (string, error) {
	var b strings.Builder
	for {
		tok, err := d.Token()
		if err != nil {
			return "", err
		}
		switch t := tok.(type) {
		case xml.CharData:
			if room := limit - b.Len(); room > 0 {
				b.Write(t[:min(room, len(t))])
			}
		case xml.StartElement:
			if err := d.Skip(); err != nil {
				return "", err
			}
		case xml.EndElement:
			return b.String(), nil
		}
	}
}

// parseXMLTV streams an XMLTV document and returns, per lower-cased channel
// id in wanted, the programmes overlapping [now-iptvEPGPast, now+iptvEPGFuture)
// sorted by start, at most iptvEPGMaxPerChannel each and iptvEPGMaxProgrammes
// in total. Over the total cap the programmes of channels later in the file
// are dropped (the rest of the file is still read, to count them in stats).
func parseXMLTV(rd io.Reader, wanted map[string]struct{}, now time.Time) (progs map[string][]iptvProgramme, st iptvEPGStats, err error) {
	winStart, winEnd := now.Add(-iptvEPGPast), now.Add(iptvEPGFuture)
	d := xml.NewDecoder(rd)
	d.Strict = false
	d.CharsetReader = iptvCharset
	progs = map[string][]iptvProgramme{}
	empty := map[string]struct{}{} // channels that lost every programme to the total cap
	total := 0
	for {
		tok, err := d.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, st, fmt.Errorf("parse guide: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local == "tv" {
			continue
		}
		if se.Name.Local != "programme" {
			if err := d.Skip(); err != nil {
				return nil, st, fmt.Errorf("parse guide: %w", err)
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
		st0, okStart := parseXMLTVTime(start)
		en, okStop := parseXMLTVTime(stop)
		_, match := wanted[key]
		take := match && okStart && st0.Before(winEnd) && (!okStop || en.After(winStart))
		switch {
		case take && len(progs[key]) >= iptvEPGMaxPerChannel:
			st.DroppedPerChannel++
			take = false
		case take && total >= iptvEPGMaxProgrammes:
			st.DroppedTotal++
			if len(progs[key]) == 0 {
				empty[key] = struct{}{}
			}
			take = false
		}
		if !take {
			if err := d.Skip(); err != nil {
				return nil, st, fmt.Errorf("parse guide: %w", err)
			}
			continue
		}
		x, err := readProgramme(d)
		if err != nil {
			return nil, st, fmt.Errorf("parse guide: %w", err)
		}
		if p, ok := x.programme(st0, en, okStop); ok {
			progs[key] = append(progs[key], p)
			total++
		}
	}
	st.ChannelsEmpty = len(empty)
	for key, ps := range progs {
		if ps = finishProgrammes(ps); len(ps) == 0 {
			delete(progs, key)
		} else {
			progs[key] = ps
		}
	}
	return progs, st, nil
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
	if x.title == "" {
		return p, false
	}
	p.Title = x.title
	if x.subTitle != "" {
		p.Title += " - " + x.subTitle
	}
	p.Desc, p.Icon, p.Genres = x.desc, x.icon, x.categories
	p.Episode = x.onscreen // onscreen wins over xmltv_ns
	if p.Episode == "" {
		p.Episode = x.xmltvNS
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

// iptvCharset lets the XML decoder read the Latin-1 / Windows-1252 guides some
// providers still publish; every other non-UTF-8 charset is rejected.
func iptvCharset(label string, in io.Reader) (io.Reader, error) {
	switch strings.ToLower(label) {
	case "utf-8", "utf8", "us-ascii", "ascii":
		return in, nil
	case "iso-8859-1", "latin1":
		return &latin1Reader{r: in}, nil
	case "windows-1252", "cp1252":
		return &latin1Reader{r: in, cp1252: true}, nil
	}
	return nil, fmt.Errorf("unsupported guide charset %q", label)
}

// cp1252High maps the bytes 0x80-0x9F of Windows-1252 (the five undefined
// ones to the C1 controls, as browsers do).
var cp1252High = [32]rune{
	0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
	0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
	0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
	0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
}

// latin1Reader transcodes ISO-8859-1, or with cp1252 Windows-1252, to UTF-8.
type latin1Reader struct {
	r      io.Reader
	cp1252 bool
	buf    []byte
}

func (l *latin1Reader) Read(p []byte) (int, error) {
	if len(p) < utf8.UTFMax {
		return 0, io.ErrShortBuffer
	}
	n := len(p) / utf8.UTFMax // every input byte expands to at most 3
	if cap(l.buf) < n {
		l.buf = make([]byte, n)
	}
	in, err := l.r.Read(l.buf[:n])
	out := p[:0]
	for _, b := range l.buf[:in] {
		r := rune(b)
		if l.cp1252 && b >= 0x80 && b < 0xA0 {
			r = cp1252High[b-0x80]
		}
		out = utf8.AppendRune(out, r)
	}
	return len(out), err
}
