// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package engine wraps anacrolix/torrent to provide the EngineManager and
// Engine interfaces declared in internal/types. It creates a single dual-stack
// (IPv4+IPv6, TCP+uTP, BEP32 DHT) torrent Client and exposes idempotent torrent
// management over it.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/dht/v2"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/mse"
	"golang.org/x/net/proxy"
	"golang.org/x/time/rate"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// videoExts is the set of extensions considered "playable" for GuessFileIdx.
var videoExts = map[string]bool{
	".mkv":  true,
	".mp4":  true,
	".avi":  true,
	".mov":  true,
	".m4v":  true,
	".webm": true,
	".flv":  true,
	".wmv":  true,
	".mpg":  true,
	".mpeg": true,
	".ts":   true,
	".m2ts": true,
}

// speedSample holds a bandwidth checkpoint for delta-speed calculation.
type speedSample struct {
	at         time.Time
	downloaded int64
	uploaded   int64
}

// engine is a single torrent handle satisfying types.Engine.
type engine struct {
	t        *torrent.Torrent
	infoHash string // lower-cased
	path     string // cache path (reported in Stats.opts.path)

	// connectionTries is a best-effort count of outbound dial churn: it sums
	// increases in HalfOpenPeers between successive Stats() samples. anacrolix
	// exposes no cumulative dial counter, so dials that start and finish between
	// two polls are missed; it is reported as swarm.tries, which server.js fills
	// from its own dial counter.
	connTriesMu     sync.Mutex
	prevHalfOpen    int
	connectionTries int

	// optsSource rebuilds the manager-derived, non-per-torrent subset of
	// stats.opts (connection budget, client timeouts, swarm caps). It is a
	// function, not a value captured at engine creation, so Stats() re-reads it
	// on each announce-cache refresh: a runtime soft-limit change is then
	// reflected (instead of frozen) and the soft-limit callback is never invoked
	// under the manager lock. Set once at creation.
	optsSource func() types.Options

	mu               sync.Mutex
	last             speedSample
	lastDLSpeed      float64          // cached bytes/sec from last Stats(); used for readahead scaling
	selected         map[int]struct{} // file indices marked for background download
	primed           map[int]struct{} // file indices whose moov/header pieces are primed (once each)
	lastAccess       time.Time        // last real use (EnsureEngine reuse, NewReader, reader Close, per-torrent Stats); global AllStats does not bump it; drives janitor LRU/idle
	openReaders      int              // active NewReader handles; >0 pins the torrent against eviction
	reading          map[int]int      // file idx -> open reader count; a file with readers is never demoted
	tailWarmed       map[int]struct{} // file idx -> tail (moov) actively pre-read once, to beat front starvation
	prefetched       map[int]struct{} // source file idx -> next-file boundary already prefetched (once each)
	onceMetaPriority sync.Once        // ensures boundary-piece prioritization runs exactly once

	// boundaryMu serializes explicit piece-priority writes (primeBoundary /
	// prefetchNext raises vs. releaseBoundary resets) and guards bprio.
	// Lock order: boundaryMu -> mu. Never held while calling into anacrolix
	// from a path that already holds mu.
	boundaryMu sync.Mutex
	bprio      map[int]torrent.PiecePriority // piece idx -> priority last set explicitly via Piece.SetPriority

	// Soft-limit peer-discovery pause state (see manager.SetSoftLimitFn /
	// enforceSoftLimit). softSample is a dedicated bandwidth checkpoint,
	// independent of `last` (used by the public Stats() speed calc), so the
	// 5 s enforcement tick gets an accurate delta regardless of how often
	// Stats() is polled by clients.
	softSample speedSample
	softPaused bool

	// uploadBlocked records whether the seed-ratio enforcer has currently
	// paused uploading for this torrent, so enforceSeedRatio only calls into
	// anacrolix (and logs) on an actual state transition rather than every tick.
	uploadBlocked bool

	// Announce-URL cache: refreshed at most once per annoTTL to avoid acquiring
	// the anacrolix Metainfo()/DistinctValues() locks on every Stats() call.
	annoMu        sync.Mutex
	annoExpiry    time.Time
	cachedURLs    []string
	cachedOpts    types.Options    // full Opts; rebuilt when cachedURLs are refreshed
	cachedFiles   []types.FileInfo // memoized after first hasInfo(); immutable once set
	cachedSources []types.Source   // Sources skeleton cached alongside cachedURLs under annoMu
}

// ensureDownloading marks file idx for full background download and demotes any
// previously-selected file that no longer has an open reader back to
// PiecePriorityNone. Streaming clients read one file at a time; without this,
// every file the user has ever clicked keeps downloading forever, so the
// available bandwidth fragments across all of them and the file actually being
// played cannot win its leading pieces — playback stalls even while the torrent
// downloads fast. Files with a live reader are never demoted.
func (e *engine) ensureDownloading(idx int) {
	if idx < 0 {
		return
	}
	files := e.t.Files()
	if idx >= len(files) {
		return
	}
	e.mu.Lock()
	if e.selected == nil {
		e.selected = map[int]struct{}{}
	}
	_, already := e.selected[idx]
	demote := make([]int, 0, len(e.selected))
	for i := range e.selected {
		if i != idx && e.reading[i] == 0 {
			demote = append(demote, i)
			delete(e.selected, i)
			delete(e.primed, i)
			delete(e.prefetched, i)
		}
	}
	// Files primed only by GuessFileIdx (never selected) must release their
	// boundary pieces once another file is the one being played.
	for i := range e.primed {
		if _, sel := e.selected[i]; !sel && i != idx && e.reading[i] == 0 {
			demote = append(demote, i)
			delete(e.primed, i)
			delete(e.prefetched, i)
		}
	}
	if !already {
		e.selected[idx] = struct{}{}
		delete(e.primed, idx) // (re)prime the moov of the file now being played
	}
	e.mu.Unlock()

	for _, i := range demote {
		if i < len(files) {
			files[i].SetPriority(torrent.PiecePriorityNone)
		}
	}
	e.releaseBoundary(demote)
	if !already {
		files[idx].Download()
	}
}

// manager owns the anacrolix Client and the live engine map, satisfying types.EngineManager.
type manager struct {
	maxEngines  int // active-engine cap; <=0 unlimited
	client      *torrent.Client
	cfg         types.Config
	downLimiter *rate.Limiter // shared pointer in cc.DownloadRateLimiter; updated by SetLimitFn
	upLimiter   *rate.Limiter // shared pointer in cc.UploadRateLimiter; updated by SetLimitFn

	// handshakeTimeout / dialTimeout are the anacrolix client-config timeouts
	// (cc.HandshakesTimeout / cc.NominalDialTimeout) reported in stats.opts
	// (milliseconds), so opts reflects the real client instead of a constant.
	handshakeTimeout time.Duration
	dialTimeout      time.Duration

	// storage is the piece storage backend handed to anacrolix as
	// DefaultStorage. anacrolix does not Close a user-provided DefaultStorage,
	// so the manager owns closing it (see Close).
	storage io.Closer

	mu        sync.RWMutex
	engines   map[string]*engine // keyed by lower-cased infoHash
	done      chan struct{}      // closed by Close() to stop background goroutines
	closeOnce sync.Once          // ensures done is closed exactly once (idempotent Close)
	limitOnce sync.Once          // ensures SetLimitFn goroutine is started at most once

	// limitMu guards limitFn and softLimitFn below, read once per tick by the
	// shared background goroutine started by SetLimitFn/SetSoftLimitFn (see
	// limitOnce/startLimitLoop).
	limitMu     sync.Mutex
	limitFn     func() (downBytesPerSec, upBytesPerSec int64)
	softLimitFn func() (softLimitBytesPerSec int64, minPeersForStable int)

	// establishedConnsPerTorrent is the configured per-torrent connection
	// budget (see peerBudget / cc.EstablishedConnsPerTorrent in New). It is
	// the value the soft-limit enforcer (enforceSoftLimit) restores a
	// torrent to once its download speed drops back below
	// btDownloadSpeedSoftLimit.
	establishedConnsPerTorrent int

	// refetching dedupes in-flight VerifyData re-downloads of evicted in-RAM
	// pieces, keyed by refetchKey. The value is the chan struct{} closed when
	// that verify has finished, which evicted reads wait on. Only exercised
	// when the in-RAM cache is on.
	refetching sync.Map

	// purging tracks infohashes whose on-disk cache directory is mid-delete
	// (RemoveEngine/RemoveAll/evict/evictIdle running os.RemoveAll outside
	// m.mu — see beginPurge). EnsureEngine waits on the channel instead of
	// recreating the same directory while the delete is still running.
	purging map[string]chan struct{}

	// scan memoises the last under-budget cache walk so the janitor can skip
	// re-walking the cache on ticks where nothing could have grown (evictTick).
	scan diskScan
}

// Compile-time interface satisfaction checks.
var _ types.EngineManager = (*manager)(nil)
var _ types.Engine = (*engine)(nil)

// refetchKey identifies an in-flight re-download of one evicted in-RAM piece.
type refetchKey struct {
	infoHash string
	piece    int
}

// refetchPiece forces anacrolix to re-download a piece whose in-RAM bytes were
// evicted. memStorage.ReadAt calls it when an evicted piece is read: VerifyData
// re-hashes the (now empty) piece, the hash fails, anacrolix clears the piece's
// dirty-chunk bitmap and re-requests it, and the reader blocks for the
// re-download instead of spinning reader.readAt into a stack overflow. Each
// (infohash, piece) verify runs at most once concurrently.
//
// The returned channel is closed once that verify has finished (the piece is
// then pending a re-download), so the evicted read can wait on it instead of
// polling; callers that join an in-flight verify get the same channel. It is
// nil when there is no live torrent to verify.
func (m *manager) refetchPiece(ih metainfo.Hash, piece int) <-chan struct{} {
	key := ih.HexString()
	m.mu.RLock()
	e := m.engines[key]
	m.mu.RUnlock()
	// NumPieces panics before the metadata (Info) has arrived; storage only
	// exists once it has, but the guard keeps the hook safe regardless.
	if e == nil || e.t == nil || e.t.Info() == nil {
		return nil
	}
	if piece < 0 || piece >= e.t.NumPieces() {
		return nil
	}
	rk := refetchKey{infoHash: key, piece: piece}
	if v, inflight := m.refetching.Load(rk); inflight {
		return v.(chan struct{})
	}
	done := make(chan struct{})
	if v, inflight := m.refetching.LoadOrStore(rk, done); inflight {
		return v.(chan struct{})
	}
	go func() {
		// Deferred in this order so the dedupe entry is gone before waiters
		// wake: a read that retries immediately starts a fresh verify.
		defer close(done)
		defer m.refetching.Delete(rk)
		// Guard: re-check the engine is still active before calling
		// VerifyData. A torrent that has been Drop()ped must not be used;
		// pointer identity avoids a false-positive when the same infohash
		// is re-added as a new engine after a drop.
		m.mu.RLock()
		current := m.engines[key]
		m.mu.RUnlock()
		if current != e {
			return
		}
		_ = e.t.Piece(piece).VerifyData()
	}()
	return done
}

// New creates a dual-stack anacrolix torrent Client and returns an EngineManager.
// ListenPort 0 lets the OS assign a port. CacheRoot is where piece data is stored.
func New(cfg types.Config) (types.EngineManager, error) {
	cc := torrent.NewDefaultClientConfig()

	// Dual-stack: IPv4 + IPv6, TCP + uTP. DHT (BEP32 v4+v6) is on by default.
	cc.DisableIPv4 = false
	cc.DisableIPv6 = false
	cc.DisableUTP = false
	cc.DisableTCP = false

	// Maximize peer sourcing + feature set.
	cc.DisablePEX = false                        // BEP11 peer exchange
	cc.DisableWebtorrent = cfg.DisableWebtorrent // WebRTC/WebTorrent peers (pion); gated by RIVULET_DISABLE_WEBTORRENT
	cc.DisableWebseeds = false                   // BEP19 HTTP webseeds
	cc.Seed = true                               // keep uploading while/after streaming (swarm health; uses IPv6 inbound)
	cc.AcceptPeerConnections = true
	cc.DisableAcceptRateLimiting = false // rate-limit inbound accepts to bound connection-handling CPU
	// Disable tracker announces when requested — avoids the upstream anacrolix tracker/udp
	// data race in tests and suppresses network fetches in private/DHT-only mode.
	cc.DisableTrackers = cfg.DisableTrackers

	// Silence anacrolix's benign context-canceled reader ERROR spam (player
	// seeks/disconnects and our background warm/prefetch cancellations) while
	// preserving genuine errors and the existing log format.
	cc.Slogger = slog.New(newReadCancelFilter(logging.For("torrent").Handler()))

	// Connection budgets tuned for streaming: enough peers for full throughput
	// without excessive MSE/RC4 handshakes and dial churn, which are the dominant
	// engine CPU cost on large, fast swarms (4K). Scaled by RIVULET_PEERS_PER_TORRENT
	// (cfg.PeersPerTorrent); default 0 keeps the historical 50/25/500 ratios.
	est, half, high := peerBudget(cfg.PeersPerTorrent)
	cc.EstablishedConnsPerTorrent = est
	cc.HalfOpenConnsPerTorrent = half
	cc.TorrentPeersHighWater = high

	// Honor the documented contract: 0 = OS-assigned (random) port. anacrolix's
	// NewDefaultClientConfig hardcodes 42069, so we must set it unconditionally
	// or two instances (and the test suite) collide on that port.
	cc.ListenPort = cfg.ListenPort

	// Storage backend. Default (MemoryCacheSize == 0): file storage partitioned
	// by info-hash so torrents don't collide on disk. Opt-in: a bounded in-RAM
	// backend that never writes piece data to disk (mobile/Termux/low-disk/
	// HuggingFace). Both backends are ClientImplClosers; anacrolix only closes
	// the default storage it creates itself, never a provided one, so the manager
	// closes it (see Close).
	var storageCloser io.Closer
	var memStore *memStorage
	if cfg.MemoryCacheSize > 0 {
		ms := newMemStorage(cfg.MemoryCacheSize)
		cc.DefaultStorage = ms
		storageCloser = ms
		memStore = ms
		logging.For("engine").Info("in-RAM piece cache enabled; piece data not written to disk", "bytes", cfg.MemoryCacheSize)
	} else {
		fs := newFileStorage(cfg.CacheRoot, cfg.StorageMmap)
		cc.DefaultStorage = fs
		storageCloser = fs
		fileIO := "classic"
		if cfg.StorageMmap {
			fileIO = "mmap"
		}
		logging.For("engine").Info("disk piece cache", "path", cfg.CacheRoot, "file_io", fileIO)
	}

	// Bandwidth rate limiters — start unlimited; SetLimitFn() adjusts them live.
	// We keep the *rate.Limiter pointers so we can call SetLimit/SetBurst without
	// changing the pointer stored in cc (anacrolix does not allow pointer replacement
	// after client creation). Burst for download: ≥1 MiB (anacrolix default floor);
	// for upload: ≥512 KiB (largest peer-request chunk size we might serve).
	downLimiter := rate.NewLimiter(rate.Inf, 1<<22) // 4 MiB burst, unlimited
	upLimiter := rate.NewLimiter(rate.Inf, 1<<22)   // 4 MiB burst, unlimited
	cc.DownloadRateLimiter = downLimiter
	cc.UploadRateLimiter = upLimiter

	// Censorship resistance / anonymity (all default = anacrolix default behavior)
	applyBTEncryption(cc, cfg.BTEncryption)
	if err := applyBTProxy(cc, cfg.BTProxy); err != nil {
		_ = storageCloser.Close()
		return nil, fmt.Errorf("engine: bt proxy: %w", err)
	}
	applyDHTBootstrap(cc, cfg.DHTBootstrap)
	cc.AnonymousMode = cfg.BTAnonymous
	if cfg.BTAnonymous {
		logging.For("engine").Info("bittorrent anonymous mode enabled")
	}

	client, err := torrent.NewClient(cc)
	if err != nil {
		_ = storageCloser.Close()
		return nil, fmt.Errorf("engine: create torrent client: %w", err)
	}

	// Create the done channel first so initTrackers can stop its 24h goroutine
	// when Close() is called.
	done := make(chan struct{})

	// Load curated trackers (cached) and refresh + rank from upstream in the background.
	// Skip tracker list fetch when announces are disabled (tests / private mode).
	if !cfg.DisableTrackers {
		initTrackers(cfg.CacheRoot, cfg.TrackersMax, cfg.TrackersURL, cfg.BTProxy, done)
	}

	m := &manager{
		client:                     client,
		cfg:                        cfg,
		downLimiter:                downLimiter,
		upLimiter:                  upLimiter,
		handshakeTimeout:           cc.HandshakesTimeout,
		dialTimeout:                cc.NominalDialTimeout,
		engines:                    make(map[string]*engine),
		done:                       done,
		storage:                    storageCloser,
		purging:                    make(map[string]chan struct{}),
		establishedConnsPerTorrent: est,
		maxEngines:                 maxEnginesFromEnv(),
	}
	// Wire the evicted-piece re-download hook now that the manager (and its
	// engine map) exists. No torrents are added until EnsureEngine, so this is
	// set before any read can occur.
	if memStore != nil {
		memStore.refetch = m.refetchPiece
	}
	return m, nil
}

// ErrTooManyEngines aliases types.ErrTooManyEngines.
var ErrTooManyEngines = types.ErrTooManyEngines

// defaultMaxEngines is the active-engine cap when RIVULET_MAX_ENGINES is unset.
const defaultMaxEngines = 20

func maxEnginesFromEnv() int {
	if v := strings.TrimSpace(os.Getenv("RIVULET_MAX_ENGINES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n // <=0 disables the cap
		}
	}
	return defaultMaxEngines
}

// oldestIdleLocked returns the least-recently-used engine with no open
// readers, or "". Caller holds m.mu.
func (m *manager) oldestIdleLocked() string {
	var best string
	var bestT time.Time
	for ih, e := range m.engines {
		e.mu.Lock()
		idle, la := e.openReaders == 0, e.lastAccess
		e.mu.Unlock()
		if idle && (best == "" || la.Before(bestT)) {
			best, bestT = ih, la
		}
	}
	return best
}

// --------------------------------------------------------------------------
// EngineManager implementation
// --------------------------------------------------------------------------

// EnsureEngine returns the existing engine for infoHash or creates one.
// A second call with additional trackers merges them without restarting, and
// bumps lastAccess in both reuse paths so the reuse itself counts as activity
// for the LRU/idle janitor — otherwise a torrent that is only ever re-fetched
// via EnsureEngine (never Stats()/NewReader) could look idle and be evicted
// before the caller gets a chance to touch it.
//
// If infoHash is currently mid-purge (a concurrent RemoveEngine/RemoveAll/
// evict/evictIdle is still running os.RemoveAll on its cache directory —
// see beginPurge), EnsureEngine blocks until that purge finishes before
// creating a fresh engine at the same on-disk path, so the new engine's
// directory can never be deleted out from under it by the tail end of the
// in-flight removal.
func (m *manager) EnsureEngine(infoHash string, opts types.AddOptions) (types.Engine, error) {
	ih := strings.ToLower(infoHash)
	if strings.Trim(ih, "0") == "" {
		return nil, fmt.Errorf("engine: info hash %q: %w", ih, types.ErrInvalidInfoHash)
	}

	for {
		// Fast path: already exists.
		m.mu.RLock()
		if e, ok := m.engines[ih]; ok {
			t := e.t // read e.t under RLock before releasing
			// Touch lastAccess while m.mu is still held: the janitor's
			// re-check takes m.mu.Lock, so it cannot evict between lookup
			// and the bump. Lock order m.mu -> e.mu matches the slow path.
			e.mu.Lock()
			e.lastAccess = time.Now()
			e.mu.Unlock()
			m.mu.RUnlock()
			mergeTrackers(t, opts, !m.cfg.DisableWebtorrent)
			return e, nil
		}
		waitCh := m.purging[ih]
		m.mu.RUnlock()
		if waitCh != nil {
			<-waitCh
			continue // purge finished (or another one started) — re-check from the top
		}

		// Slow path: acquire write lock, re-check (double-check locking), then add.
		m.mu.Lock()
		if e, ok := m.engines[ih]; ok {
			mergeTrackers(e.t, opts, !m.cfg.DisableWebtorrent)
			e.mu.Lock()
			e.lastAccess = time.Now()
			e.mu.Unlock()
			m.mu.Unlock()
			return e, nil
		}
		if waitCh := m.purging[ih]; waitCh != nil {
			m.mu.Unlock()
			<-waitCh
			continue
		}
		if m.maxEngines > 0 && len(m.engines) >= m.maxEngines {
			victim := m.oldestIdleLocked()
			if victim == "" {
				n := len(m.engines)
				m.mu.Unlock()
				return nil, fmt.Errorf("engine: %d engines active: %w", n, ErrTooManyEngines)
			}
			finish := m.beginPurge(victim, m.engines[victim])
			m.mu.Unlock()
			finish()
			continue
		}

		var (
			t   *torrent.Torrent
			err error
		)

		if opts.MetaInfo != nil {
			// Raw .torrent bytes provided — parse and add.
			mi, miErr := metainfo.Load(bytes.NewReader(opts.MetaInfo))
			if miErr != nil {
				m.mu.Unlock()
				return nil, fmt.Errorf("engine: parse metainfo for %s: %w", ih, miErr)
			}
			// Drop ws/wss (and any non-http(s)/udp) announces when WebTorrent is
			// disabled; otherwise anacrolix's regular dispatcher panics on them.
			allowWS := !m.cfg.DisableWebtorrent
			mi.Announce = strings.Join(announceableTrackers([]string{mi.Announce}, allowWS), "")
			mi.AnnounceList = announceableTiers(mi.AnnounceList, allowWS)
			t, err = m.client.AddTorrent(mi)
		} else {
			// Only the info-hash is known; start with a plain magnet URI.
			magnet := "magnet:?xt=urn:btih:" + ih
			t, err = m.client.AddMagnet(magnet)
		}
		if err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("engine: add torrent %s: %w", ih, err)
		}

		mergeTrackers(t, opts, !m.cfg.DisableWebtorrent)
		// Inject a baseline public-tracker list (like the official server) so bare /
		// trackerless magnets still find peers instead of relying on DHT alone.
		t.AddTrackers([][]string{announceableTrackers(getTrackers(), !m.cfg.DisableWebtorrent)})

		e := &engine{
			t:          t,
			infoHash:   ih,
			path:       filepath.Join(m.cfg.CacheRoot, ih),
			lastAccess: time.Now(),
			optsSource: m.statsOptsSource,
		}
		m.engines[ih] = e
		m.mu.Unlock()

		// Spawn a one-shot goroutine that boosts container-metadata (moov-atom /
		// codec-init) reads once the torrent info dict is available.
		go applyBoundaryPriorities(e)

		return e, nil
	}
}

// GetEngine returns the engine for infoHash if it exists.
func (m *manager) GetEngine(infoHash string) (types.Engine, bool) {
	ih := strings.ToLower(infoHash)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.engines[ih]; ok {
		return e, true
	}
	return nil, false
}

// beginPurge removes ih's engine from the live map and drops its torrent
// while still holding m.mu (both O(1)), then returns a function that runs
// the (potentially slow) os.RemoveAll off the lock and clears the purge
// marker when done. Previously RemoveAll ran while still holding m.mu,
// which stalled every other API call (including /stats.json polling) for
// the duration of a bulk filesystem delete on slow storage.
//
// While the returned function is in flight, ih is recorded in m.purging so a
// concurrent EnsureEngine for the same infohash blocks instead of racing to
// recreate e.path mid-delete (see EnsureEngine). Callers must hold m.mu for
// writing when calling beginPurge, and must invoke the returned function only
// after releasing m.mu.
func (m *manager) beginPurge(ih string, e *engine) func() {
	e.t.Drop()
	delete(m.engines, ih)
	done := make(chan struct{})
	m.purging[ih] = done
	return func() {
		_ = os.RemoveAll(e.path)
		m.mu.Lock()
		delete(m.purging, ih)
		m.mu.Unlock()
		close(done)
	}
}

// RemoveEngine stops and removes the torrent identified by infoHash.
func (m *manager) RemoveEngine(infoHash string) error {
	ih := strings.ToLower(infoHash)
	m.mu.Lock()
	e, ok := m.engines[ih]
	if !ok {
		m.mu.Unlock()
		return nil // idempotent
	}
	finish := m.beginPurge(ih, e)
	m.mu.Unlock()
	finish()
	return nil
}

// RemoveAll stops and removes all active torrents.
func (m *manager) RemoveAll() {
	m.mu.Lock()
	finishers := make([]func(), 0, len(m.engines))
	for ih, e := range m.engines {
		finishers = append(finishers, m.beginPurge(ih, e))
	}
	m.mu.Unlock()
	for _, finish := range finishers {
		finish()
	}
}

// ListEngines returns the lower-cased info-hashes of all active engines.
func (m *manager) ListEngines() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.engines))
	for ih := range m.engines {
		out = append(out, ih)
	}
	return out
}

// NumTorrents returns the number of currently active torrent engines.
func (m *manager) NumTorrents() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.engines)
}

// AllStats returns torrent-level stats (idx=-1) keyed by lower-cased infoHash.
// It is read-only observability: unlike a per-torrent Stats call it does not
// refresh any torrent's idle clock (issue #41).
func (m *manager) AllStats() map[string]*types.Stats {
	m.mu.RLock()
	snap := make(map[string]*engine, len(m.engines))
	for ih, e := range m.engines {
		snap[ih] = e
	}
	m.mu.RUnlock()

	result := make(map[string]*types.Stats, len(snap))
	for ih, e := range snap {
		result[ih] = e.stats(-1, false)
	}
	return result
}

// Close shuts down the anacrolix client (drops all torrents, closes sockets).
// It also signals the janitor goroutine (if running) to stop.
func (m *manager) Close() error {
	m.closeOnce.Do(func() { close(m.done) })
	m.client.Close()
	if m.storage != nil {
		if err := m.storage.Close(); err != nil {
			logging.For("engine").Error("closing piece storage", "err", err)
		}
	}
	return nil
}

// StartJanitor launches a background goroutine that evicts least-recently-used
// engines whenever the on-disk cache exceeds the budget returned by cacheSizeFn,
// drops idle torrents, and enforces the configured maximum seed ratio.
// The goroutine stops when Close() is called (which closes m.done).
func (m *manager) StartJanitor(cacheSizeFn func() int64) {
	go func() {
		ticker := time.NewTicker(janitorTick)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.evictTick(cacheSizeFn())
				m.evictIdle(m.cfg.IdleTimeout)
				m.enforceSeedRatio(m.cfg.MaxSeedRatio)
			case <-m.done:
				return
			}
		}
	}()
}

// applyRateLimit sets limiter's rate/burst from n bytes/sec (0 or negative
// means unlimited). Burst must be at least as large as the biggest single
// Read/Write the underlying layer performs: for download that's ~1 MiB
// (HTTP/2 frame); for upload the peer-request chunk is 16 KiB. Both get
// generous headroom.
func applyRateLimit(limiter *rate.Limiter, n int64, isUpload bool) {
	if n <= 0 {
		// 0 or negative → unlimited
		limiter.SetLimit(rate.Inf)
		limiter.SetBurst(1 << 22) // 4 MiB
		return
	}
	limiter.SetLimit(rate.Limit(n))
	burst := int(min(max(n, 1<<20), math.MaxInt32))
	if isUpload {
		burst = int(min(max(n, 1<<17), math.MaxInt32)) // 128 KiB floor for upload
	}
	limiter.SetBurst(burst)
}

// SetLimitFn wires live download/upload bandwidth limits from a settings getter.
// fn returns (downBytesPerSec, upBytesPerSec); 0 = unlimited, positive = cap in bytes/sec.
// For upload, callers should return 1 to indicate "effectively disabled" when seeding
// is turned off — this keeps the burst valid while making actual seeding negligible.
// Applied on the same 5 s background goroutine as SetSoftLimitFn (started at
// most once by whichever setter runs first — see startLimitLoop/limitOnce).
func (m *manager) SetLimitFn(fn func() (downBytesPerSec, upBytesPerSec int64)) {
	m.limitMu.Lock()
	m.limitFn = fn
	m.limitMu.Unlock()
	m.startLimitLoop()
}

// SetSoftLimitFn wires the live peer-discovery soft-limit decision from a
// settings getter. fn returns (btDownloadSpeedSoftLimit bytes/sec,
// btMinPeersForStable); softLimitBytesPerSec<=0 disables the feature (and
// releases any pause a previously-set, positive limit applied).
//
// Official semantics (blog.stremio.com/modify-bittorrent-settings):
// btDownloadSpeedSoftLimit is NOT a throughput cap — that's
// btDownloadSpeedHardLimit (SetLimitFn), a hard bytes/sec ceiling enforced by
// the rate limiter. The soft limit instead stops the client from searching
// for *additional* peers once a torrent is already downloading at or above
// the soft limit AND has at least btMinPeersForStable peers connected: the
// existing peers are proven enough to sustain that speed, so spending more
// CPU/bandwidth on DHT/tracker/PEX discovery for that torrent is wasted. It
// resumes discovery the moment speed drops back below the limit (e.g. a peer
// disconnects).
//
// anacrolix/torrent has no direct "pause new peer search" toggle — DHT
// announces and tracker scrapes are client-wide, not per-torrent, and
// Torrent.AllowDataDownload/DisallowDataDownload gate *accepting piece
// data*, not peer acquisition (the wrong knob: it would stall the transfer
// instead of just capping discovery). The closest safe, per-torrent knob is
// Torrent.SetMaxEstablishedConns: wantIncomingConns/wantOutgoingConns (and
// therefore openNewConns) gate purely on `len(conns) < maxEstablishedConns`,
// so clamping it to the torrent's current established connection count stops
// new peers from being taken on without dropping any already-open
// connection; raising it back to the configured per-torrent budget
// (establishedConnsPerTorrent, from peerBudget) resumes discovery. See
// engine.enforceSoftLimit.
func (m *manager) SetSoftLimitFn(fn func() (softLimitBytesPerSec int64, minPeersForStable int)) {
	m.limitMu.Lock()
	m.softLimitFn = fn
	m.limitMu.Unlock()
	m.startLimitLoop()
}

// statsOptsSource returns the effective connection/timeout/swarm-cap values for
// stats.opts, derived from the manager config, the anacrolix client timeouts,
// and the configured soft limit. These mirror what the official server reports
// (connections, timeout, handshakeTimeout, swarmCap.minPeers/maxSpeed) instead
// of leaving them null. maxSpeed stays null when no soft limit is configured,
// matching server.js when unset. Per-torrent fields (peerSearch, path, dht,
// tracker) are filled in by Stats().
func (m *manager) statsOptsSource() types.Options {
	est, _, _ := peerBudget(m.cfg.PeersPerTorrent)
	conns := est
	// Read the timeouts from the client config (ms, like server.js): the
	// handshake timeout is anacrolix HandshakesTimeout (4s default), the dial
	// timeout is NominalDialTimeout (20s default).
	handshakeMs := int(m.handshakeTimeout / time.Millisecond)
	dialMs := int(m.dialTimeout / time.Millisecond)
	opts := types.Options{
		Connections:      &conns,
		HandshakeTimeout: &handshakeMs,
		Timeout:          &dialMs,
		Virtual:          false,
	}
	m.limitMu.Lock()
	soft := m.softLimitFn
	m.limitMu.Unlock()
	if soft != nil {
		if softBytes, minPeers := soft(); softBytes > 0 {
			ms := float64(softBytes)
			mp := minPeers
			opts.SwarmCap = types.SwarmCap{MaxSpeed: &ms, MinPeers: &mp}
		}
	}
	return opts
}

// startLimitLoop launches the shared 5 s background goroutine (stopped on
// Close) that applies both the bandwidth rate limits (SetLimitFn) and the
// peer-discovery soft limit (SetSoftLimitFn), whichever are set. Idempotent:
// only the first call, from whichever setter runs first, starts it.
func (m *manager) startLimitLoop() {
	m.limitOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					m.applyLimits()
				case <-m.done:
					return
				}
			}
		}()
	})
}

// applyLimits runs one tick of both live-settings passes: global bandwidth
// rate limits, and, per live torrent, the peer-discovery soft limit.
func (m *manager) applyLimits() {
	m.limitMu.Lock()
	limitFn := m.limitFn
	softLimitFn := m.softLimitFn
	m.limitMu.Unlock()

	if limitFn != nil {
		down, up := limitFn()
		applyRateLimit(m.downLimiter, down, false)
		applyRateLimit(m.upLimiter, up, true)
	}

	if softLimitFn != nil {
		soft, minPeers := softLimitFn()
		m.mu.RLock()
		engines := make([]*engine, 0, len(m.engines))
		for _, e := range m.engines {
			engines = append(engines, e)
		}
		m.mu.RUnlock()
		for _, e := range engines {
			e.enforceSoftLimit(soft, minPeers, m.establishedConnsPerTorrent)
		}
	}
}

// softLimitDecision reports whether peer discovery should be paused for a
// torrent currently downloading at speedBytesPerSec with connectedPeers
// established connections, given the configured soft limit and the minimum
// peer count for the "stable" threshold. See SetSoftLimitFn for the full
// semantics. softLimitBytesPerSec<=0 always returns false (feature off).
func softLimitDecision(speedBytesPerSec float64, softLimitBytesPerSec int64, connectedPeers, minPeersForStable int) bool {
	if softLimitBytesPerSec <= 0 {
		return false
	}
	return speedBytesPerSec >= float64(softLimitBytesPerSec) && connectedPeers >= minPeersForStable
}

// enforceSoftLimit applies one tick of the btDownloadSpeedSoftLimit decision
// (see manager.SetSoftLimitFn) to this torrent: pauses peer discovery
// (clamping SetMaxEstablishedConns to the current connection count) once the
// torrent is already sustaining softLimitBytesPerSec with minPeers peers
// connected, and restores establishedConnsPerTorrent (the configured budget)
// once it drops back below. Only calls into anacrolix on an actual state
// transition (mirrors applySeedRatio), so a steady-state torrent isn't
// re-clamped every 5 s.
func (e *engine) enforceSoftLimit(softLimitBytesPerSec int64, minPeers, establishedConnsPerTorrent int) {
	ts := e.t.Stats()
	dl := ts.BytesReadData.Int64()
	now := time.Now()

	e.mu.Lock()
	var speed float64
	if !e.softSample.at.IsZero() {
		if dt := now.Sub(e.softSample.at).Seconds(); dt > 0 {
			speed = float64(dl-e.softSample.downloaded) / dt
		}
	}
	e.softSample = speedSample{at: now, downloaded: dl}
	wasPaused := e.softPaused
	e.mu.Unlock()

	if speed < 0 {
		speed = 0
	}
	connectedPeers := ts.ActivePeers
	wantPaused := softLimitDecision(speed, softLimitBytesPerSec, connectedPeers, minPeers)
	if wantPaused == wasPaused {
		return
	}

	if wantPaused {
		e.t.SetMaxEstablishedConns(connectedPeers)
		logging.For("engine").Debug("soft-limit: pausing peer discovery", "info_hash", e.infoHash,
			"speed_bytes", int64(speed), "peers", connectedPeers, "soft_limit_bytes", softLimitBytesPerSec)
	} else {
		if establishedConnsPerTorrent <= 0 {
			establishedConnsPerTorrent = 50 // matches peerBudget's own default fallback
		}
		e.t.SetMaxEstablishedConns(establishedConnsPerTorrent)
		logging.For("engine").Debug("soft-limit: resuming peer discovery", "info_hash", e.infoHash,
			"speed_bytes", int64(speed), "peers", connectedPeers, "max_conns", establishedConnsPerTorrent)
	}

	e.mu.Lock()
	e.softPaused = wantPaused
	e.mu.Unlock()
}

// evict enforces the cache budget returned by cacheSizeFn. Contract:
// budget < 0 means unlimited (no eviction ever runs). budget == 0 means
// "no caching": every engine with zero open readers is purged as soon as its
// grace window has elapsed, regardless of on-disk size. budget > 0 is a byte
// cap: least-recently-used, reader-less engines are dropped until total
// on-disk usage is back within budget.
//
// Lock ordering: sizes are measured without any lock held; the write lock is
// acquired only for individual map mutations, minimising contention with readers.
func (m *manager) evict(budget int64) {
	if budget < 0 {
		return // unlimited — never purge
	}

	const graceWindow = 90 * time.Second

	// Snapshot engine pointers under read lock (avoids holding the lock during
	// expensive filesystem walks).
	m.mu.RLock()
	snap := make([]*engine, 0, len(m.engines))
	for _, e := range m.engines {
		snap = append(snap, e)
	}
	m.mu.RUnlock()

	if len(snap) == 0 {
		return
	}

	if budget == 0 {
		// "no caching": on-disk size is irrelevant — purge every reader-less
		// engine once its grace window has elapsed.
		m.purgeReaderless(snap, graceWindow, "evicted torrent (no caching)")
		return
	}

	// In-RAM mode keeps no piece data on disk, so there is nothing to measure or
	// evict by size: skip the walk entirely.
	if m.cfg.MemoryCacheSize > 0 {
		return
	}

	// Measure each live engine's cache directory. Only those directories are
	// walked, not the whole cache root, so leftover directories of torrents that
	// are no longer loaded (and any other files under the root) cost no stats.
	// Peer data counters are sampled before the walk so growth during it shows
	// up on the next tick (see cacheQuiet, diskScan.observe).
	type entry struct {
		e           *engine
		size        int64
		lastAccess  time.Time
		openReaders int
	}
	dl := make(map[*engine]int64, len(snap))
	dirSizes := make(map[string]int64, len(snap))
	for _, e := range snap {
		dl[e] = e.downloaded()
		dirSizes[e.infoHash] = dirAllocated(filepath.Join(m.cfg.CacheRoot, e.infoHash))
	}

	entries := make([]entry, 0, len(snap))
	var total int64
	for _, e := range snap {
		e.mu.Lock()
		la := e.lastAccess
		or := e.openReaders
		e.mu.Unlock()
		sz := dirSizes[e.infoHash]
		entries = append(entries, entry{e: e, size: sz, lastAccess: la, openReaders: or})
		total += sz
	}

	// An under-budget reading is only memoised once the data counters have
	// held still since the previous measurement (see diskScan.observe).
	m.scan.observe(total <= budget, total, dl)
	if total <= budget {
		return
	}

	// Sort ascending by lastAccess (oldest first). The MRU engine is last and
	// is always preserved — we iterate only up to len(entries)-1.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastAccess.Before(entries[j].lastAccess)
	})

	now := time.Now()
	for i := 0; i < len(entries)-1 && total > budget; i++ {
		ent := entries[i]
		if ent.openReaders > 0 || now.Sub(ent.lastAccess) < graceWindow {
			// Actively streaming or recently accessed — never evict.
			continue
		}
		ih := ent.e.infoHash
		// Re-check under write lock: compare pointer identity, not just key
		// existence. A concurrent Remove+EnsureEngine may have replaced the
		// engine instance; evicting its snapshot would delete a live engine's
		// map entry and on-disk cache.
		m.mu.Lock()
		if m.engines[ih] != ent.e {
			m.mu.Unlock()
			continue // already removed or replaced by a concurrent caller
		}
		// Re-check under the write lock: a reader may have opened since the
		// snapshot. Closes the snapshot->Drop race so a live stream is never
		// closed out from under its HTTP reader. Lock order m.mu -> e.mu.
		ent.e.mu.Lock()
		pinned := ent.e.openReaders > 0
		ent.e.mu.Unlock()
		if pinned {
			m.mu.Unlock()
			continue
		}
		finish := m.beginPurge(ih, ent.e)
		m.mu.Unlock()
		finish()
		total -= ent.size
		logging.For("engine").Info("evicted torrent", "info_hash", ih, "freed_bytes", ent.size, "budget_bytes", budget)
	}
}

// diskScanMaxAge bounds how long the janitor trusts a cache-size measurement
// without walking the cache again, whatever the download counters say. It
// backstops the activity heuristic in cacheQuiet (filesystems that allocate
// whole files up front or lag in reporting blocks, external writers), so a
// stale reading is corrected within a few ticks.
const diskScanMaxAge = 4 * janitorTick

// janitorTick is the period of the janitor's size/idle/seed-ratio pass.
const janitorTick = 30 * time.Second

// diskScan is the janitor's memo of its last under-budget cache walk.
type diskScan struct {
	mu    sync.Mutex
	at    time.Time         // when the walk was taken
	total int64             // allocated bytes attributed to the live engines then
	dl    map[*engine]int64 // per-engine peer data bytes received at that point; nil = no memo
	prev  map[*engine]int64 // counters sampled before the previous walk (any outcome)
}

// observe records the result of a walk whose counters were sampled before it.
// The walk is memoised only when it was within budget AND every counter equals
// the one sampled before the previous walk. The library bumps its received-bytes
// counter before the chunk is written, so a bump just before a sample may still
// be unwritten when the walk runs; a counter that has been still for a whole
// janitor interval guarantees such in-flight writes have landed.
func (s *diskScan) observe(withinBudget bool, total int64, dl map[*engine]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stable := s.prev != nil && len(s.prev) == len(dl)
	if stable {
		for e, n := range dl {
			if p, ok := s.prev[e]; !ok || p != n {
				stable = false
				break
			}
		}
	}
	s.prev = dl
	if withinBudget && stable {
		s.at, s.total, s.dl = time.Now(), total, dl
		return
	}
	s.dl = nil
}

// downloaded returns the cumulative peer data bytes received for the torrent
// (0 without a live torrent). Cache growth only follows received data, so an
// unchanged value means no new piece data reached the cache.
func (e *engine) downloaded() int64 {
	if e.t == nil {
		return 0
	}
	ts := e.t.Stats()
	return ts.BytesReadData.Int64()
}

// dirAllocated returns the allocated bytes (see allocatedSize) of every file
// under dir. A missing or unreadable dir or entry counts as zero.
func dirAllocated(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, ierr := d.Info(); ierr == nil {
			total += allocatedSize(info)
		}
		return nil
	})
	return total
}

// evictTick is the janitor's size-based pass: evict, except when a cheap check
// proves the on-disk cache cannot have outgrown the budget since the last walk,
// which spares stat'ing every cached file each tick. Direct evict calls always
// walk.
func (m *manager) evictTick(budget int64) {
	if budget > 0 && m.cacheQuiet(budget) {
		return
	}
	m.evict(budget)
}

// cacheQuiet reports whether the last walk found the cache within budget and
// nothing has been written since: the same engines are loaded, none has
// received a data byte (cache files only grow when piece data arrives), and the
// reading is younger than diskScanMaxAge. The check costs one Stats() per
// engine and no filesystem access.
func (m *manager) cacheQuiet(budget int64) bool {
	m.scan.mu.Lock()
	defer m.scan.mu.Unlock()
	if m.scan.dl == nil || m.scan.total > budget || time.Since(m.scan.at) >= diskScanMaxAge {
		return false
	}
	m.mu.RLock()
	live := make([]*engine, 0, len(m.engines))
	for _, e := range m.engines {
		live = append(live, e)
	}
	m.mu.RUnlock()
	if len(live) != len(m.scan.dl) {
		return false
	}
	for _, e := range live {
		base, ok := m.scan.dl[e]
		if !ok || e.downloaded() != base {
			return false
		}
	}
	return true
}

// purgeReaderless drops every engine in snap that currently has no open
// readers and has been idle (no EnsureEngine/NewReader/reader-Close or
// per-torrent Stats; the global AllStats does not count) for at least
// threshold, logging each removal under event. It is the shared reader-less
// scan used by both evict(0) ("no caching", threshold = the fixed grace
// window) and evictIdle (threshold = the configured idle timeout).
//
// Lock ordering matches evict: m.mu -> e.mu; the engine is re-checked under
// the write lock so a reader that opened since the snapshot is never dropped.
func (m *manager) purgeReaderless(snap []*engine, threshold time.Duration, event string) {
	now := time.Now()
	for _, e := range snap {
		e.mu.Lock()
		idleFor := now.Sub(e.lastAccess)
		pinned := e.openReaders > 0
		e.mu.Unlock()
		if pinned || idleFor < threshold {
			continue
		}
		ih := e.infoHash
		m.mu.Lock()
		if m.engines[ih] != e {
			m.mu.Unlock()
			continue // already removed or replaced by a concurrent caller
		}
		// Re-check under the write lock: a reader may have opened since the
		// snapshot. Lock order m.mu -> e.mu.
		e.mu.Lock()
		stillPinned := e.openReaders > 0 || now.Sub(e.lastAccess) < threshold
		e.mu.Unlock()
		if stillPinned {
			m.mu.Unlock()
			continue
		}
		finish := m.beginPurge(ih, e)
		m.mu.Unlock()
		finish()
		logging.For("engine").Info(event, "info_hash", ih, "idle_for", idleFor.Round(time.Second).String())
	}
}

// evictIdle drops torrents that have had no open stream readers and no access
// for longer than idle. idle <= 0 disables it. This mirrors the official
// server's inactive-torrent reclaim (~5 min) so a stopped torrent is dropped
// even when cacheSize is unlimited (the size-based evict never fires then),
// while still keeping it alive long enough for instant scrub/resume/next-episode.
func (m *manager) evictIdle(idle time.Duration) {
	if idle <= 0 {
		return
	}

	m.mu.RLock()
	snap := make([]*engine, 0, len(m.engines))
	for _, e := range m.engines {
		snap = append(snap, e)
	}
	m.mu.RUnlock()

	m.purgeReaderless(snap, idle, "removed idle torrent")
}

// uploadToggler is the slice of *torrent.Torrent the seed-ratio enforcer needs.
// The indirection exists so the enforcement state machine can be exercised
// without standing up a live torrent client.
type uploadToggler interface {
	AllowDataUpload()
	DisallowDataUpload()
}

// seedRatioExceeded reports whether a torrent that has uploaded ul bytes and
// downloaded dl bytes has reached maxRatio, along with the ratio itself.
//
// maxRatio <= 0 means unlimited. A torrent that has downloaded nothing is never
// blocked: a zero denominator has no meaningful ratio, and pausing upload there
// would stall the tit-for-tat exchange a fresh torrent needs to get its first
// pieces.
func seedRatioExceeded(ul, dl int64, maxRatio float64) (exceeded bool, ratio float64) {
	if maxRatio <= 0 || dl <= 0 {
		return false, 0
	}
	ratio = float64(ul) / float64(dl)
	return ratio >= maxRatio, ratio
}

// applySeedRatio moves one engine to the desired upload state, calling into
// anacrolix (and logging) only on an actual transition rather than every tick.
// ctl is the torrent whose upload is toggled; it is nil only in tests that
// exercise the bookkeeping alone.
func (e *engine) applySeedRatio(ctl uploadToggler, ul, dl int64, maxRatio float64) {
	block, ratio := seedRatioExceeded(ul, dl, maxRatio)

	e.mu.Lock()
	if e.uploadBlocked == block {
		e.mu.Unlock()
		return
	}
	e.uploadBlocked = block
	e.mu.Unlock()

	if ctl == nil {
		return
	}
	if block {
		ctl.DisallowDataUpload()
		logging.For("engine").Info("seed ratio reached; pausing upload",
			"info_hash", e.infoHash, "ratio", math.Round(ratio*100)/100, "max_ratio", maxRatio)
		return
	}
	ctl.AllowDataUpload()
	logging.For("engine").Info("seed ratio below cap; resuming upload",
		"info_hash", e.infoHash, "ratio", math.Round(ratio*100)/100, "max_ratio", maxRatio)
}

// enforceSeedRatio pauses uploading for any torrent whose share ratio
// (bytes uploaded / bytes downloaded) has reached maxRatio, and resumes it if
// the ratio later falls back below the cap. maxRatio <= 0 disables the feature
// and also releases any pause a previous, higher-ratio configuration applied.
//
// This is deliberately decoupled from evictIdle: hitting the ratio only stops
// this torrent from uploading, it never drops the torrent or purges its cache.
// Seeding can therefore be capped without shortening how long a torrent stays
// resident for instant scrub/resume, which is what makes the two knobs
// independent.
func (m *manager) enforceSeedRatio(maxRatio float64) {
	m.mu.RLock()
	snap := make([]*engine, 0, len(m.engines))
	for _, e := range m.engines {
		snap = append(snap, e)
	}
	m.mu.RUnlock()

	for _, e := range snap {
		ts := e.t.Stats()
		e.applySeedRatio(e.t, ts.BytesWrittenData.Int64(), ts.BytesReadData.Int64(), maxRatio)
	}
}

// --------------------------------------------------------------------------
// Engine implementation
// --------------------------------------------------------------------------

// InfoHash returns the lower-cased hex info-hash string.
func (e *engine) InfoHash() string {
	return e.infoHash
}

// Ready blocks until torrent metadata is available or ctx is done.
func (e *engine) Ready(ctx context.Context) error {
	select {
	case <-e.t.GotInfo():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// hasInfo reports whether metadata (the info dict) is already available without blocking.
func (e *engine) hasInfo() bool {
	select {
	case <-e.t.GotInfo():
		return true
	default:
		return false
	}
}

// Files returns the list of files in the torrent. Returns nil before metadata is ready.
func (e *engine) Files() []types.FileInfo {
	if !e.hasInfo() {
		return nil
	}
	// Fast path: file metadata is immutable once the info dict is available,
	// so we build the slice once and reuse it on every subsequent Stats() call.
	e.mu.Lock()
	cached := e.cachedFiles
	e.mu.Unlock()
	if cached != nil {
		return cached
	}
	files := e.t.Files()
	out := make([]types.FileInfo, len(files))
	for i, f := range files {
		// DisplayPath is the slash-joined relative path; for single-file
		// torrents it returns the info.Name directly.
		name := f.DisplayPath()
		out[i] = types.FileInfo{
			Name:   name,
			Path:   name,
			Length: f.Length(),
			Offset: f.Offset(),
		}
	}
	// Store under lock; a benign race to build produces identical immutable results.
	e.mu.Lock()
	if e.cachedFiles == nil {
		e.cachedFiles = out
	}
	e.mu.Unlock()
	return out
}

// NewReader returns a streaming, seek-capable reader for the file at idx. The
// readahead window starts small (8 MiB) so playback begins quickly, then grows
// with measured throughput up to 64 MiB for a deep buffer (see pinnedReader).
func (e *engine) NewReader(idx int) (io.ReadSeekCloser, int64, error) {
	// Mark this engine as recently active before any blocking work.
	e.mu.Lock()
	e.lastAccess = time.Now()
	cachedSpeed := e.lastDLSpeed // bytes/sec from last Stats(); 0 before first call
	e.mu.Unlock()
	if !e.hasInfo() {
		return nil, 0, fmt.Errorf("engine: metadata not yet available for %s", e.infoHash)
	}
	files := e.t.Files()
	if idx < 0 || idx >= len(files) {
		return nil, 0, fmt.Errorf("engine: file index %d out of range [0,%d)", idx, len(files))
	}
	f := files[idx]
	e.mu.Lock()
	e.openReaders++
	if e.reading == nil {
		e.reading = map[int]int{}
	}
	e.reading[idx]++
	e.mu.Unlock()
	e.ensureDownloading(idx) // fetch the file being played; demote abandoned ones
	e.primeBoundary(idx)     // top-priority the header/moov so playback starts fast
	e.warmMoov(idx)          // pre-read the tail so the moov downloads with the front
	e.prefetchNext(idx)      // opportunistically warm the next episode's header (binge UX)
	r := f.NewReader()
	r.SetReadahead(readaheadFor(cachedSpeed))
	now := time.Now()
	// ctx is the reader's long-lived context: it lives for the reader's
	// whole lifetime and is cancelled only from pinnedReader.Close (see the
	// pinnedReader doc comment for why a fresh, request-scoped context is
	// required here instead of reusing ReadContext's deadline context).
	ctx, cancel := context.WithCancel(context.Background())
	pr := &pinnedReader{
		tr:       r,
		e:        e,
		idx:      idx,
		deadline: now.Add(startupReadTimeout),
		ctx:      ctx,
		cancel:   cancel,
	}
	// nextRAAdj is an atomic.Int64 (unix-nanos); must be set after construction.
	pr.nextRAAdj.Store(now.Add(readaheadAdjustInterval).UnixNano())
	return pr, f.Length(), nil
}

// warmMoov pre-fetches the bytes a player needs to start before they would
// arrive on their own. Non-faststart MP4s (e.g. most RARBG encodes) and MKVs
// keep their index (moov atom / Cues) at the END of the file; primeBoundary
// marks those pieces top-priority, but within equal priority the actively-read
// front wins, so the index trickles in last and playback stalls for a long time
// even while the torrent downloads fast. warmMoov opens a dedicated reader on
// the last few MiB and drains it so the index gets real demand and arrives
// alongside the header. This is the anacrolix equivalent of qBittorrent's
// "download first and last piece first" / Elementum's start-end buffering.
//
// Faststart MP4s already carry the moov at the front, so the main stream read
// covers it — warming the tail would only steal bandwidth and delay start, so
// it is skipped. Runs once per file; bounded by a timeout so a tail that never
// arrives (dead swarm) cannot leak the goroutine; tiny files are ignored.
func (e *engine) warmMoov(idx int) {
	if idx < 0 || !e.hasInfo() {
		return
	}
	e.mu.Lock()
	if e.tailWarmed == nil {
		e.tailWarmed = map[int]struct{}{}
	}
	if _, ok := e.tailWarmed[idx]; ok {
		e.mu.Unlock()
		return
	}
	e.tailWarmed[idx] = struct{}{}
	e.mu.Unlock()

	files := e.t.Files()
	if idx >= len(files) {
		return
	}
	f := files[idx]
	const minWarmSize int64 = 16 << 20 // ignore subtitles/samples/tiny files
	if f.Length() < minWarmSize {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if !needsTailWarm(ctx, f) {
			return // faststart MP4: index already at the front
		}
		r := f.NewReader()
		defer func() { _ = r.Close() }()
		r.SetReadahead(tailWindowBytes)
		off := f.Length() - tailWindowBytes
		if off < 0 {
			off = 0
		}
		if _, err := r.Seek(off, io.SeekStart); err != nil {
			e.abandonTailWarm(idx)
			return
		}
		if !drainTail(ctx, r) {
			e.abandonTailWarm(idx)
		}
	}()
}

// tailReader is the slice of torrent.Reader that drainTail needs.
type tailReader interface {
	ReadContext(ctx context.Context, b []byte) (int, error)
}

// drainTail reads r through to the end, discarding the bytes, so the tail
// pieces get real demand (see warmMoov). It reports whether the tail was
// delivered: true on a clean io.EOF, false when the read failed first (ctx
// deadline, torrent dropped) or stopped making progress. Reaching EOF is the
// success case — the tail is in hand — so warmMoov must NOT treat it like a
// timed-out pre-read (abandonTailWarm clears the once-only marker, which made
// every later NewReader — i.e. every Range request a player issues — re-run
// needsTailWarm and re-read the whole tail window).
func drainTail(ctx context.Context, r tailReader) bool {
	buf := make([]byte, 64<<10)
	for {
		n, err := r.ReadContext(ctx, buf) // bounded; errors on timeout or torrent drop
		if err != nil {
			return errors.Is(err, io.EOF)
		}
		if n == 0 {
			return false
		}
	}
}

// abandonTailWarm gives up on a bounded tail pre-read that did not complete:
// it demotes the file's pieces back to "not wanted" (the readahead window the
// warm reader set) and clears the once-only marker so a later NewReader can
// retry, instead of leaving the tail requested forever with no reader.
func (e *engine) abandonTailWarm(idx int) {
	e.mu.Lock()
	delete(e.tailWarmed, idx)
	// Reader and selection state are read under one lock so the demotion
	// decision cannot race a concurrent NewReader or Close.
	wanted := e.tailWarmWantedLocked(idx)
	e.mu.Unlock()
	if wanted || !e.hasInfo() {
		return
	}
	files := e.t.Files()
	if idx >= 0 && idx < len(files) {
		files[idx].SetPriority(torrent.PiecePriorityNone)
	}
}

// tailWarmWantedLocked reports whether file idx is still wanted, i.e. whether a
// reader is streaming it or it is selected for full background download. Callers
// hold e.mu.
func (e *engine) tailWarmWantedLocked(idx int) bool {
	if e.reading[idx] > 0 {
		return true
	}
	_, selected := e.selected[idx]
	return selected
}

// needsTailWarm walks the leading MP4 box headers and reports false only when it
// confirms a faststart layout (a moov box appears before any mdat). For mdat
// first (moov at the end), non-MP4 containers (e.g. MKV, whose Cues live at the
// end), or anything it cannot parse, it returns true so the tail is warmed — the
// safe default. Reads are bounded by ctx.
func needsTailWarm(ctx context.Context, f *torrent.File) bool {
	r := f.NewReader()
	defer func() { _ = r.Close() }()
	r.SetReadahead(256 << 10)
	hdr := make([]byte, 8)
	var pos int64
	for i := 0; i < 6; i++ {
		if _, err := r.Seek(pos, io.SeekStart); err != nil {
			return true
		}
		if err := readFullCtx(ctx, r, hdr); err != nil {
			return true
		}
		size := int64(hdr[0])<<24 | int64(hdr[1])<<16 | int64(hdr[2])<<8 | int64(hdr[3])
		switch string(hdr[4:8]) {
		case "moov":
			return false // faststart: index before media data
		case "mdat":
			return true // media first: moov is at the end
		case "ftyp", "free", "skip", "wide", "pdin":
			// header/filler box; advance to the next one
		default:
			return true // not a recognizable faststart MP4 (e.g. MKV)
		}
		if size < 8 {
			return true // 64-bit/edge size: bail to the safe default
		}
		pos += size
	}
	return true
}

// readFullCtx fills b from r, honouring ctx for cancellation/timeout.
func readFullCtx(ctx context.Context, r torrent.Reader, b []byte) error {
	for got := 0; got < len(b); {
		n, err := r.ReadContext(ctx, b[got:])
		got += n
		if err != nil {
			return err
		}
	}
	return nil
}

// Streaming readahead bounds + timing.
const (
	minReadahead            int64         = 8 << 20          // 8 MiB floor — fast start
	maxReadahead            int64         = 64 << 20         // 64 MiB ceiling — deep buffer on fast swarms
	readaheadAdjustInterval time.Duration = 3 * time.Second  // how often a live reader recomputes its window
	startupReadTimeout      time.Duration = 90 * time.Second // bound the first read so a dead swarm errors instead of hanging
)

// readaheadFor returns the streaming readahead window for a measured download
// speed: ~2 s of throughput, clamped to [minReadahead, maxReadahead].
func readaheadFor(speed float64) int64 {
	ra := minReadahead
	if speed > 0 {
		if s := int64(2 * speed); s > ra {
			ra = s
		}
	}
	if ra > maxReadahead {
		ra = maxReadahead
	}
	return ra
}

// pinnedReader wraps a torrent file reader. While it is open the engine's
// openReaders count is >0 so the janitor never evicts the torrent mid-stream
// (Close decrements exactly once). It also (a) grows the readahead window as the
// measured speed rises — small at first for a fast start, deeper later for
// smooth playback — and (b) bounds the FIRST read with a deadline so a dead
// swarm returns an error instead of hanging the player forever.
//
// Context handling is deliberately NOT built on torrent.Reader.ReadContext:
// ReadContext stores whatever context it is given directly on the reader
// (anacrolix/torrent reader.go SetContext) with NO expiry tied to the call
// returning. A naive `ctx, cancel := context.WithDeadline(...); defer
// cancel(); tr.ReadContext(ctx, b)` therefore poisons the reader forever:
// the deferred cancel() fires when Read returns, leaving tr holding an
// already-cancelled context, so a well-buffered reader (which never checks
// ctx while data is locally available — see waitAvailable) looks fine right
// up until the first genuine buffer under-run, which then fails instantly
// with context.Canceled instead of blocking for the swarm.
//
// Instead, ctx/cancel below is a context.WithCancel(context.Background())
// created once at construction (see NewReader) and cancelled ONLY from
// Close(). SetContext is called explicitly: with a deadline-bounded child
// of ctx for the first read (so a dead swarm still errors out promptly),
// then swapped back onto the long-lived ctx the moment the first byte
// arrives, so later stalls block for the swarm — and a client disconnect
// still unblocks a stalled read immediately via Close() cancelling ctx.
type pinnedReader struct {
	tr   torrent.Reader
	e    *engine
	idx  int
	once sync.Once

	// ctx/cancel: see the type doc above. ctx is only ever installed on tr
	// by Read (single-goroutine caller, per torrent.Reader's "not safe for
	// concurrent use" contract); cancel is safe to call from any goroutine,
	// any number of times (stdlib context.CancelFunc contract).
	ctx    context.Context
	cancel context.CancelFunc

	// started and nextRAAdj are hot-path fields accessed on every Read(); using
	// atomics eliminates the per-Read mutex round-trip on the fast path.
	started   atomic.Bool  // set once to true after first byte; false->true exactly once
	deadline  time.Time    // written once at construction; read-only thereafter (no lock needed)
	nextRAAdj atomic.Int64 // unix-nanos of next readahead recompute; monotonically advancing

	closeErr error // result of the single real tr.Close() call; cached by Close's sync.Once
}

func (p *pinnedReader) Read(b []byte) (int, error) {
	p.adjustReadahead()
	// Fast path: once started, tr already holds the long-lived ctx (installed
	// below on the first successful read), so reads block for the swarm
	// instead of racing a deadline. Lock-free.
	if p.started.Load() {
		return p.tr.Read(b)
	}
	// First byte not yet received: bound this read with the startup deadline
	// so a dead swarm errors out instead of hanging the player forever. The
	// deadline context is a child of p.ctx so an external Close() (which
	// cancels p.ctx) unblocks this first read immediately too.
	deadlineCtx, cancelDeadline := context.WithDeadline(p.ctx, p.deadline)
	p.tr.SetContext(deadlineCtx)
	n, err := p.tr.Read(b)
	if n > 0 {
		p.started.Store(true)
		// Promote to the long-lived context BEFORE releasing the deadline
		// one below, so tr is never left holding an already-cancelled
		// context (that ordering is precisely what the old ReadContext-based
		// code got wrong).
		p.tr.SetContext(p.ctx)
	}
	cancelDeadline()
	return n, err
}

func (p *pinnedReader) Seek(offset int64, whence int) (int64, error) {
	return p.tr.Seek(offset, whence)
}

func (p *pinnedReader) Close() error {
	// Cancel unconditionally and first, before the Once: this is what
	// unblocks a Read() stalled inside the vendored reader's waitAvailable
	// (see the pinnedReader doc comment on SetContext). context.CancelFunc
	// is safe to call from multiple goroutines and multiple times, so this
	// is safe even when the request handler's deferred Close() races a
	// disconnect-watcher goroutine's Close() to get here first.
	p.cancel()
	p.once.Do(func() {
		// tr.Close() is guarded inside the Once (rather than called
		// unconditionally on every Close() call as before) so it runs
		// exactly once even under that same race, instead of relying on the
		// vendored reader's own idempotency (map-delete + nil-guarded
		// storageReader close) to make a concurrent double call harmless.
		p.closeErr = p.tr.Close()
		var demote bool
		p.e.mu.Lock()
		p.e.openReaders--
		// The idle clock restarts when the stream is released, not when it was
		// opened: otherwise a long playback (lastAccess = NewReader time, and
		// nothing may have polled its stats) would look idle for hours the
		// instant it stops and be dropped on the next janitor tick, instead of
		// surviving the configured idle window for instant resume.
		p.e.lastAccess = time.Now()
		if p.e.reading[p.idx] > 0 {
			p.e.reading[p.idx]--
		}
		// Last reader for this file is gone: stop greedily downloading the rest
		// of it. Without this the file stays marked Download() and keeps pulling
		// pieces to completion in the background after the user pressed stop. The
		// torrent itself stays connected (only the janitor's idle/LRU passes drop
		// it), so scrub/resume/next-episode are still instant — reopening the file
		// re-marks it via ensureDownloading + primeBoundary.
		if p.e.reading[p.idx] == 0 {
			delete(p.e.selected, p.idx)
			delete(p.e.primed, p.idx)
			demote = true
		}
		p.e.mu.Unlock()
		if demote {
			// Re-check under mu: a reader may have reopened this file between
			// the unlock above and now; demoting then would starve it.
			p.e.mu.Lock()
			_, sel := p.e.selected[p.idx]
			still := p.e.reading[p.idx] == 0 && !sel
			if still {
				delete(p.e.prefetched, p.idx)
				files := p.e.t.Files()
				if p.idx >= 0 && p.idx < len(files) {
					files[p.idx].SetPriority(torrent.PiecePriorityNone)
				}
			}
			p.e.mu.Unlock()
			if still {
				p.e.releaseBoundary([]int{p.idx})
			}
		}
	})
	return p.closeErr
}

// adjustReadahead recomputes the window from the latest measured speed, at most
// once per readaheadAdjustInterval, so the buffer deepens as throughput rises.
func (p *pinnedReader) adjustReadahead() {
	now := time.Now().UnixNano()
	// Lock-free throttle: skip recompute if still within the current interval.
	// A benign double-adjust on a rare CAS race is acceptable and harmless.
	if now < p.nextRAAdj.Load() {
		return
	}
	p.nextRAAdj.Store(now + int64(readaheadAdjustInterval))
	p.e.mu.Lock()
	sp := p.e.lastDLSpeed
	p.e.mu.Unlock()
	p.tr.SetReadahead(readaheadFor(sp))
}

// prefetchNext opportunistically warms the NEXT video file's header and tail
// (moov) at low (Normal) priority, so advancing to the next episode of a season
// pack starts fast. It touches only a few boundary pieces (not the whole file)
// and never raises them above the active stream's read window, so it uses only
// spare bandwidth and cannot starve current playback. Runs once per source file;
// the prefetch target is not added to e.selected, so demotion never touches it.
func (e *engine) prefetchNext(idx int) {
	if idx < 0 || !e.hasInfo() {
		return
	}
	e.mu.Lock()
	if e.prefetched == nil {
		e.prefetched = map[int]struct{}{}
	}
	if _, ok := e.prefetched[idx]; ok {
		e.mu.Unlock()
		return
	}
	e.prefetched[idx] = struct{}{}
	e.mu.Unlock()

	next := e.nextVideoIdx(idx)
	if next < 0 {
		return
	}
	files := e.t.Files()
	f := files[next]
	begin := f.BeginPieceIndex()
	end := f.EndPieceIndex()
	_ = begin
	// Raise-only: a boundary piece already at Now (e.g. shared with the file
	// being played) must never be lowered to Normal.
	e.raiseBoundary(prefetchSpans(begin, end), torrent.PiecePriorityNormal, e.setPiecePriority)
	logging.For("engine").Debug("prefetched next-file boundary", "info_hash", e.infoHash, "file_idx", next)
}

// prefetchSpans returns the header and tail piece spans prefetchNext raises for
// a file whose piece range is [begin,end).
func prefetchSpans(begin, end int) []pieceSpan {
	const prefetchPieces = 4 // header + index only, opportunistic
	tailStart := end - prefetchPieces
	if tailStart < begin {
		tailStart = begin
	}
	return []pieceSpan{{begin, min(begin+prefetchPieces, end)}, {tailStart, end}}
}

// nextVideoIdx returns the index of the first video file after idx, or -1.
func (e *engine) nextVideoIdx(idx int) int {
	files := e.t.Files()
	for i := idx + 1; i < len(files); i++ {
		if videoExts[strings.ToLower(filepath.Ext(files[i].DisplayPath()))] {
			return i
		}
	}
	return -1
}

// GuessFileIdx returns the index of the largest video file, falling back to the
// largest file overall, or -1 if the torrent has no files yet.
func (e *engine) GuessFileIdx() int {
	if !e.hasInfo() {
		return -1
	}
	files := e.t.Files()
	if len(files) == 0 {
		return -1
	}

	bestVideo := -1
	bestVideoLen := int64(-1)
	bestAny := 0
	bestAnyLen := int64(-1)

	for i, f := range files {
		l := f.Length()
		ext := strings.ToLower(filepath.Ext(f.DisplayPath()))
		if videoExts[ext] && l > bestVideoLen {
			bestVideoLen = l
			bestVideo = i
		}
		if l > bestAnyLen {
			bestAnyLen = l
			bestAny = i
		}
	}

	if bestVideo >= 0 {
		return bestVideo
	}
	return bestAny
}

// accumulateConnTries folds this sample's half-open peer count into the running
// dial-churn heuristic and returns the new total (see the engine field comment).
func (e *engine) accumulateConnTries(halfOpen int) int {
	e.connTriesMu.Lock()
	if halfOpen > e.prevHalfOpen {
		e.connectionTries += halfOpen - e.prevHalfOpen
	}
	e.prevHalfOpen = halfOpen
	n := e.connectionTries
	e.connTriesMu.Unlock()
	return n
}

// Stats returns a types.Stats snapshot. When idx >= 0 the per-file stream
// fields (StreamLen, StreamName, StreamProgress) are also populated.
// Download/upload speeds are computed from byte deltas between successive calls.
//
// A per-torrent Stats call counts as use and refreshes the idle clock: it backs
// /{ih}/stats.json and /{ih}/{idx}/stats.json, which the stremio-core player
// polls only while that torrent is open (e.g. paused with the HTTP reader
// dropped), so the torrent stays resident for an instant resume. The global
// /stats.json aggregate goes through AllStats, which does not refresh it.
func (e *engine) Stats(idx int) *types.Stats {
	return e.stats(idx, true)
}

// stats is Stats with control over whether the call refreshes lastAccess.
func (e *engine) stats(idx int, touch bool) *types.Stats {
	ts := e.t.Stats()

	// Bandwidth counters — ConnStats is embedded directly in TorrentStats →
	// AllConnStats → ConnStats, so field access is ts.BytesReadData.Int64().
	dl := ts.BytesReadData.Int64()
	ul := ts.BytesWrittenData.Int64()

	now := time.Now()
	e.mu.Lock()
	var dlSpeed, ulSpeed float64
	if !e.last.at.IsZero() {
		dt := now.Sub(e.last.at).Seconds()
		if dt > 0 {
			dlSpeed = float64(dl-e.last.downloaded) / dt
			ulSpeed = float64(ul-e.last.uploaded) / dt
		}
	}
	e.last = speedSample{at: now, downloaded: dl, uploaded: ul}
	// Only a per-torrent query counts as use. AllStats (the global /stats.json,
	// polled continuously by dashboards and monitors across every torrent)
	// passes touch=false: counting those polls reset the idle clock on every
	// call and kept abandoned torrents resident forever, so
	// RIVULET_TORRENT_IDLE_TIMEOUT never fired (issue #41).
	if touch {
		e.lastAccess = now
	}
	if dlSpeed > 0 {
		e.lastDLSpeed = dlSpeed // cache for NewReader readahead scaling
	}
	swarmPaused := e.softPaused // read under the lock; server.js's e.swarm.paused
	e.mu.Unlock()

	// Clamp negative speeds (can happen when the client resets counters).
	if dlSpeed < 0 {
		dlSpeed = 0
	}
	if ulSpeed < 0 {
		ulSpeed = 0
	}

	// Peer gauges — embedded via TorrentGauges.
	activePeers := ts.ActivePeers
	totalPeers := ts.TotalPeers
	halfOpen := ts.HalfOpenPeers
	pending := ts.PendingPeers

	// connectionTries: server.js exposes swarm.tries from its own dial counter.
	// anacrolix has no cumulative dial count, so this is a heuristic: it sums
	// increases in HalfOpenPeers between samples, so dials that start and finish
	// between two polls are missed. It still reflects real dial activity on a
	// polled torrent better than a constant 0.
	connTries := e.accumulateConnTries(halfOpen)

	files := e.Files()
	if files == nil {
		// stremio-core's Statistics deserializer requires files as a sequence
		// (Vec<File>, never Option<Vec<File>>), even before the torrent's
		// metadata — and therefore its file list — is available (COMPAT-1).
		files = []types.FileInfo{}
	}
	// Refresh the announce-URL / Opts cache when the TTL has elapsed.
	// Metainfo() and DistinctValues() each acquire anacrolix internal locks; at
	// ~1 Hz polling from stremio-web those contend heavily. We memoize the
	// announce list and the full Opts map (which is otherwise allocated fresh
	// on every call) and refresh at most once every 30 s.
	const annoTTL = 30 * time.Second
	e.annoMu.Lock()
	if now.After(e.annoExpiry) {
		mi := e.t.Metainfo()
		urls := mi.UpvertedAnnounceList().DistinctValues()
		peerSrcs := make([]string, 0, 1+len(urls))
		peerSrcs = append(peerSrcs, "dht:"+e.infoHash)
		peerSrcs = append(peerSrcs, urls...)
		e.cachedURLs = urls
		// Rebuild the manager-derived opts on each refresh (outside the manager
		// lock) so a runtime soft-limit change is reflected and swarmCap cannot
		// disagree with the live enforcement, then fill the per-torrent bits.
		base := e.optsSource()
		base.PeerSearch = types.PeerSearch{Min: 40, Max: 200, Sources: peerSrcs}
		base.DHT = true
		base.Growler = types.Growler{Flood: 0}
		base.Path = e.path
		base.Tracker = true
		e.cachedOpts = base
		// Build the Sources skeleton. server.js prefixes tracker announce URLs
		// with "tracker:"; match that so duplicate source URLs are not
		// ambiguous. anacrolix exposes no per-tracker announce count, so
		// numRequests is a documented constant rather than a poll-rate guess.
		// Per-call-varying fields (LastStarted, NumFound, NumFoundUniq) are
		// stamped onto a shallow copy below, avoiding re-iteration every Stats().
		skel := make([]types.Source, 0, len(urls))
		for _, u := range urls {
			if !strings.HasPrefix(u, "udp://") && !strings.HasPrefix(u, "http://") &&
				!strings.HasPrefix(u, "https://") {
				continue // skip wss:// and any non-URL forms
			}
			skel = append(skel, types.Source{URL: "tracker:" + u, NumRequests: 1})
		}
		e.cachedSources = skel
		e.annoExpiry = now.Add(annoTTL)
	}
	sourceSkel := e.cachedSources
	opts := e.cachedOpts
	e.annoMu.Unlock()

	// Stamp per-call-varying fields onto a shallow copy of the skeleton.
	// Only valid-URL schemes (udp://, http://, https://) appear in the skeleton —
	// the filter ran once at cache-build time above.
	started := now.UTC().Format(time.RFC3339)
	numFound := totalPeers
	if numFound < 1 {
		numFound = 1 // non-zero best-effort
	}
	sources := make([]types.Source, len(sourceSkel))
	copy(sources, sourceSkel)
	for i := range sources {
		sources[i].LastStarted = started
		sources[i].NumFound = numFound
		sources[i].NumFoundUniq = numFound
	}

	s := &types.Stats{
		InfoHash: e.infoHash,
		Name:     e.t.Name(),
		Peers:    activePeers,
		// server.js: unchoked = wires that are not choking us; we approximate as
		// active peers (anacrolix does not expose per-wire choke state here).
		Unchoked: activePeers,
		// queued = peers awaiting a connection slot: half-open dials in flight,
		// plus peers still pending discovery in anacrolix's dial queue.
		Queued: halfOpen + pending,
		Unique: totalPeers,
		// connectionTries = cumulative dial churn (see the accumulation above).
		ConnectionTries: connTries,
		// swarmPaused reflects the soft-limit peer-discovery pause, which is the
		// state server.js exposes as e.swarm.paused.
		SwarmPaused:       swarmPaused,
		SwarmConnections:  activePeers,
		SwarmSize:         totalPeers,
		Selections:        nil,
		Wires:             []types.Wire{},
		Files:             files,
		Downloaded:        dl,
		Uploaded:          ul,
		DownloadSpeed:     dlSpeed,
		UploadSpeed:       ulSpeed,
		Sources:           sources,
		Opts:              opts,
		PeerSearchRunning: !swarmPaused,
	}

	// Per-file extras when a file index is requested. stremio-core's Statistics
	// deserializer is strict: streamLen/streamName/streamProgress must be present
	// (non-null) for any per-file stats.json (idx>=0). So ALWAYS set them when
	// idx>=0 — falling back to zero values when metadata or the index is not (yet)
	// available — to avoid a transient deser failure during a brief
	// metadata-not-ready / re-create window. idx<0 (torrent-level / all-stats)
	// keeps them omitted.
	if idx >= 0 {
		var (
			length   int64
			name     string
			progress float64
		)
		if e.hasInfo() {
			torFiles := e.t.Files()
			if idx < len(torFiles) {
				e.ensureDownloading(idx) // polling a file's stats also starts its download
				f := torFiles[idx]
				length = f.Length()
				name = f.DisplayPath()
				if length > 0 {
					progress = float64(f.BytesCompleted()) / float64(length)
					if progress > 1 {
						progress = 1
					}
					if progress < 0 {
						progress = 0
					}
				}
			}
		}
		s.StreamLen = &length
		s.StreamName = &name
		s.StreamProgress = &progress
	}

	return s
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// mergeTrackers adds tracker URLs from opts into t. It strips the "tracker:"
// prefix from Sources entries and ignores "dht:" entries.
func mergeTrackers(t *torrent.Torrent, opts types.AddOptions, allowWS bool) {
	var urls []string
	for _, tr := range opts.Trackers {
		tr = strings.TrimSpace(tr)
		if tr != "" {
			urls = append(urls, tr)
		}
	}
	for _, src := range opts.Sources {
		src = strings.TrimSpace(src)
		if strings.HasPrefix(src, "tracker:") {
			u := strings.TrimPrefix(src, "tracker:")
			if u != "" {
				urls = append(urls, u)
			}
		}
		// Ignore "dht:<infohash>" entries; anacrolix DHT finds peers on its own.
	}

	// Also extract trackers from the Torrent JSON field (stremio-web sends
	// {infoHash, announce, announce-list, files...}).
	urls = append(urls, trackersFromTorrentJSON(opts.Torrent)...)

	// Drop unannounceable schemes (ws/wss when WebTorrent is off) before adding;
	// anacrolix synchronously builds a tracker client and panics on bad schemes.
	urls = announceableTrackers(urls, allowWS)
	if len(urls) > 0 {
		// Each call to AddTrackers appends; duplicates are handled by anacrolix.
		t.AddTrackers([][]string{urls})
	}
}

// trackersFromTorrentJSON parses the stremio-web "torrent" JSON object and
// extracts any announce/announce-list tracker URLs. Returns nil on parse error.
func trackersFromTorrentJSON(raw json.RawMessage) []string {
	if raw == nil {
		return nil
	}
	var obj struct {
		Announce     string     `json:"announce"`
		AnnounceList [][]string `json:"announce-list"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	var urls []string
	if obj.Announce != "" {
		urls = append(urls, obj.Announce)
	}
	for _, tier := range obj.AnnounceList {
		urls = append(urls, tier...)
	}
	return urls
}

// applyBoundaryPriorities boosts piece priority on the first and last 8 pieces
// of the guessed video file so the container header (moov atom / codec-init box)
// and trailer (chapter index, etc.) are fetched immediately when a torrent
// becomes ready. This mirrors the reference server's MAX_CONTAINER_METADATA_WINDOW
// (16 MiB) behaviour: priming these pieces means ffprobe and the HLS transcoder
// can start without stalling on a seek to the end of the file.
//
// The function is designed to be called as a goroutine exactly once per torrent
// (guarded by e.onceMetaPriority). It blocks until metadata is available and
// then returns immediately after setting priorities.
func applyBoundaryPriorities(e *engine) {
	e.onceMetaPriority.Do(func() {
		// Block until info dict is fetched OR the torrent is dropped (Closed returns
		// a receive channel that is closed when the torrent is removed). Without the
		// Closed case this goroutine would leak for the lifetime of the process when
		// the torrent is evicted before metadata arrives.
		select {
		case <-e.t.GotInfo():
		case <-e.t.Closed():
			return
		}

		idx := e.GuessFileIdx()
		if idx < 0 {
			return
		}
		e.primeBoundary(idx)
	})
}

// Boundary priming windows: byte ranges (not fixed piece counts) at the head
// and tail of the streamed file — see primeBoundary for why these are
// narrow (≈1 piece each at the default 16 MiB piece length) rather than the
// previous fixed 8-piece windows. tailWindowBytes also bounds warmMoov's
// dedicated tail read, so the two mechanisms agree on how much trailer data
// "the moov" means.
const (
	headWindowBytes int64 = 4 << 20 // 4 MiB: reaches ftyp/moov (faststart) or codec-init box
	tailWindowBytes int64 = 8 << 20 // 8 MiB: covers moov-at-end MP4 / MKV Cues+Tags trailer
)

// piecesForBytes returns the number of pieceLen-sized pieces needed to cover
// n bytes, rounded up, floored at 1 (n<=0 or pieceLen<=0 also yields 1 —
// defensive; real torrents have positive lengths once metadata is ready).
func piecesForBytes(n, pieceLen int64) int {
	if pieceLen <= 0 || n <= 0 {
		return 1
	}
	p := int((n + pieceLen - 1) / pieceLen)
	if p < 1 {
		p = 1
	}
	return p
}

// primeBoundaryRange computes the piece sub-ranges primeBoundary marks
// PiecePriorityNow: [begin,headEnd) covers headBytes at the front of the
// file's piece range [begin,end), and [tailBegin,end) covers tailBytes at
// the back. The two ranges may overlap on files with fewer than
// piecesForBytes(headBytes)+piecesForBytes(tailBytes) total pieces —
// harmless, since re-applying PiecePriorityNow to an already-Now piece is
// idempotent. Pulled out as a pure function so the piece math is
// unit-testable without a live torrent.Torrent.
func primeBoundaryRange(begin, end int, pieceLen, headBytes, tailBytes int64) (headEnd, tailBegin int) {
	if end <= begin {
		return begin, begin
	}
	headEnd = begin + piecesForBytes(headBytes, pieceLen)
	if headEnd > end {
		headEnd = end
	}
	tailBegin = end - piecesForBytes(tailBytes, pieceLen)
	if tailBegin < begin {
		tailBegin = begin
	}
	return headEnd, tailBegin
}

// primeBoundary raises piece priority on the pieces covering the first
// headWindowBytes and last tailWindowBytes of file idx — the container
// header (MP4 moov atom / codec-init box) and trailer — so the player can
// parse the file and begin playback long before it is fully downloaded. It
// runs at most once per file (guarded by e.primed) and requires metadata to
// be available. Unlike the one-shot GuessFileIdx priming, this is also
// called from NewReader for the file the client actually requested —
// essential for multi-file torrents/packs where the played file is not the
// largest one, whose header would otherwise never be prioritized and
// playback would never start.
//
// Deliberately byte-windowed (≈1 piece head + ≈1 piece tail at the default
// 16 MiB piece length) instead of a fixed piece count, and deliberately NOT
// extended to further head pieces: anacrolix's Piece.purePriority() (see
// piece.go in the module cache) already raises the reader's OWN current
// piece to PiecePriorityNow and its whole live readahead window to
// PiecePriorityReadahead automatically (Torrent.updateReaderPieces, driven
// by pinnedReader's growing SetReadahead calls) — see
// internal/request-strategy/order.go, which orders same-priority pieces
// partial -> rarest -> index with no further tie-break. The previous 8-piece
// head window (up to ~128 MiB at 16 MiB pieces) put many explicitly-pinned
// Now pieces at the SAME priority tier as the reader's current piece; on a
// thin swarm the tie-break could serve one of those instead, starving the
// actual read and stalling playback (Kodi/Rivulet's curllowspeedtime=20s
// then aborts). Priming only the true header/trailer bytes keeps just those
// two small windows at Now, so the reader's current piece never has to share
// the top priority tier with unrelated pinned pieces; everything else in the
// head is left to the reader's own dynamically-growing readahead window
// (already Readahead priority, strictly below Now) instead of being pinned
// here at Now.
func (e *engine) primeBoundary(idx int) {
	if idx < 0 || !e.hasInfo() {
		return
	}
	e.mu.Lock()
	if e.primed == nil {
		e.primed = map[int]struct{}{}
	}
	if _, ok := e.primed[idx]; ok {
		e.mu.Unlock()
		return
	}
	e.primed[idx] = struct{}{}
	e.mu.Unlock()

	files := e.t.Files()
	if idx >= len(files) {
		return
	}
	f := files[idx]
	begin := f.BeginPieceIndex()
	end := f.EndPieceIndex() // exclusive
	info := e.t.Info()
	if info == nil {
		return
	}
	headEnd, tailBegin := primeBoundaryRange(begin, end, info.PieceLength, headWindowBytes, tailWindowBytes)
	e.raiseBoundary([]pieceSpan{{begin, headEnd}, {tailBegin, end}}, torrent.PiecePriorityNow, e.setPiecePriority)
	logging.For("engine").Debug("boundary-prioritized pieces", "info_hash", e.infoHash, "file_idx", idx,
		"begin", begin, "end", end,
		"head_bytes", headWindowBytes, "head_pieces", headEnd-begin,
		"tail_bytes", tailWindowBytes, "tail_pieces", end-tailBegin)
}

// pieceSpan is a half-open piece index range [begin,end).
type pieceSpan struct{ begin, end int }

// boundarySpans returns the head and tail piece spans primeBoundary marks Now
// for a file whose piece range is [begin,end).
func boundarySpans(begin, end int, pieceLen int64) []pieceSpan {
	headEnd, tailBegin := primeBoundaryRange(begin, end, pieceLen, headWindowBytes, tailWindowBytes)
	return []pieceSpan{{begin, headEnd}, {tailBegin, end}}
}

// piecesToRelease returns the piece indices in demoted that are not covered
// by any protected span (a piece shared with a file that is still selected,
// primed or being read must keep its boundary priority). Result is deduplicated
// and ascending.
func piecesToRelease(demoted, protected []pieceSpan) []int {
	keep := map[int]struct{}{}
	for _, s := range protected {
		for i := s.begin; i < s.end; i++ {
			keep[i] = struct{}{}
		}
	}
	seen := map[int]struct{}{}
	var out []int
	for _, s := range demoted {
		for i := s.begin; i < s.end; i++ {
			if _, ok := keep[i]; ok {
				continue
			}
			if _, ok := seen[i]; ok {
				continue
			}
			seen[i] = struct{}{}
			out = append(out, i)
		}
	}
	sort.Ints(out)
	return out
}

func (e *engine) setPiecePriority(i int, prio torrent.PiecePriority) {
	e.t.Piece(i).SetPriority(prio)
}

// raiseBoundary sets prio on every piece in spans whose explicitly-set
// priority is lower (or unset). It never lowers a piece, so a low-priority
// prefetch cannot downgrade a boundary piece already at Now.
func (e *engine) raiseBoundary(spans []pieceSpan, prio torrent.PiecePriority, set func(int, torrent.PiecePriority)) {
	e.boundaryMu.Lock()
	defer e.boundaryMu.Unlock()
	if e.bprio == nil {
		e.bprio = map[int]torrent.PiecePriority{}
	}
	for _, s := range spans {
		for i := s.begin; i < s.end; i++ {
			if cur, ok := e.bprio[i]; ok && cur >= prio {
				continue
			}
			e.bprio[i] = prio
			set(i, prio)
		}
	}
}

// releaseBoundary resets the explicitly-set boundary piece priorities of the
// demoted files back to None (File.SetPriority(None) does not touch the
// separate per-piece priority), skipping pieces shared with a file that is
// still selected, primed or being read.
func (e *engine) releaseBoundary(demoted []int) {
	if len(demoted) == 0 || !e.hasInfo() {
		return
	}
	info := e.t.Info()
	if info == nil {
		return
	}
	files := e.t.Files()
	spansOf := func(i int) []pieceSpan {
		if i < 0 || i >= len(files) {
			return nil
		}
		out := boundarySpans(files[i].BeginPieceIndex(), files[i].EndPieceIndex(), info.PieceLength)
		// Next-file prefetch pieces raised on behalf of file i.
		if n := e.nextVideoIdx(i); n >= 0 {
			out = append(out, prefetchSpans(files[n].BeginPieceIndex(), files[n].EndPieceIndex())...)
		}
		return out
	}

	e.boundaryMu.Lock()
	defer e.boundaryMu.Unlock()
	e.mu.Lock()
	keep := map[int]struct{}{}
	for i := range e.selected {
		keep[i] = struct{}{}
	}
	for i := range e.primed {
		keep[i] = struct{}{}
	}
	for i, n := range e.reading {
		if n > 0 {
			keep[i] = struct{}{}
		}
	}
	e.mu.Unlock()
	var demotedSpans, protected []pieceSpan
	for _, d := range demoted {
		delete(keep, d)
		demotedSpans = append(demotedSpans, spansOf(d)...)
	}
	for i := range keep {
		protected = append(protected, spansOf(i)...)
	}
	e.releasePieces(piecesToRelease(demotedSpans, protected), e.setPiecePriority)
}

// releasePieces resets pieces we explicitly raised back to None. Caller holds
// boundaryMu.
func (e *engine) releasePieces(pieces []int, set func(int, torrent.PiecePriority)) {
	for _, i := range pieces {
		if _, ok := e.bprio[i]; !ok {
			continue // never raised by us
		}
		delete(e.bprio, i)
		set(i, torrent.PiecePriorityNone)
	}
}

// peerBudget derives the anacrolix per-torrent connection budget from a single
// knob. n<=0 uses the default 50. half-open = n/2, high-water = n*10 — the same
// ratios as the historical 50/25/500 defaults (peerBudget(0) == 50,25,500).
func peerBudget(n int) (established, halfOpen, highWater int) {
	if n <= 0 {
		n = 50
	}
	return n, n / 2, n * 10
}

// --------------------------------------------------------------------------
// Censorship-resistance / anonymity helpers
// --------------------------------------------------------------------------

// applyBTEncryption sets the MSE/RC4 header-obfuscation policy on cc.
// mode is compared case-insensitively after trimming whitespace.
//   - "require": only RC4-encrypted peer connections are accepted.
//   - "disable": plaintext only; no MSE handshake offered.
//   - anything else ("prefer" / ""): anacrolix defaults are left untouched
//     (MSE preferred, plaintext accepted — already the safest default).
func applyBTEncryption(cc *torrent.ClientConfig, mode string) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "require":
		cc.HeaderObfuscationPolicy = torrent.HeaderObfuscationPolicy{Preferred: true, RequirePreferred: true}
		cc.CryptoProvides = mse.CryptoMethodRC4
		cc.CryptoSelector = func(mse.CryptoMethod) mse.CryptoMethod { return mse.CryptoMethodRC4 }
		logging.For("engine").Info("bt encryption required", "mode", "require")
	case "disable":
		cc.HeaderObfuscationPolicy = torrent.HeaderObfuscationPolicy{Preferred: false, RequirePreferred: false}
		logging.For("engine").Info("bt encryption disabled", "mode", "disable")
		// "prefer" / "" — anacrolix defaults already correct; nothing to do
	}
}

// applyBTProxy routes tracker announces, HTTP webseeds, and metainfo fetches
// through proxyURL (socks5[h]://… or http[s]://…). When proxyURL is empty the
// function is a no-op and returns nil.
//
// Peer TCP/uTP connections are NOT routed through the proxy: anacrolix's peer
// dialer uses the listen-bound socket path (not HTTPDialContext/TrackerDialContext).
// Use encryption=require for DPI-evasion of peer traffic.
func applyBTProxy(cc *torrent.ClientConfig, proxyURL string) error {
	if proxyURL == "" {
		return nil
	}
	u, err := url.Parse(proxyURL)
	if err != nil {
		return fmt.Errorf("parse proxy url: %w", err)
	}
	cc.HTTPProxy = http.ProxyURL(u)
	scheme := strings.ToLower(u.Scheme)
	if scheme == "socks5" || scheme == "socks5h" {
		d, err := proxy.FromURL(u, proxy.Direct)
		if err != nil {
			return fmt.Errorf("create socks5 dialer: %w", err)
		}
		if cd, ok := d.(proxy.ContextDialer); ok {
			cc.TrackerDialContext = cd.DialContext
			cc.HTTPDialContext = cd.DialContext
		}
	}
	// Log scheme://host only — never expose userinfo (credentials).
	logging.For("engine").Info("bittorrent proxy enabled", "url", u.Scheme+"://"+u.Host)
	return nil
}

// applyDHTBootstrap appends extra DHT bootstrap nodes to the default global
// list. nodes is a comma-separated list of host:port pairs; blank entries are
// silently skipped. When nodes is empty the function is a no-op.
func applyDHTBootstrap(cc *torrent.ClientConfig, nodes string) {
	if nodes == "" {
		return
	}
	var parsed []string
	for _, hp := range strings.Split(nodes, ",") {
		if hp = strings.TrimSpace(hp); hp != "" {
			parsed = append(parsed, hp)
		}
	}
	if len(parsed) == 0 {
		return
	}
	cc.DhtStartingNodes = func(network string) dht.StartingNodesGetter {
		return func() ([]dht.Addr, error) {
			addrs, _ := dht.GlobalBootstrapAddrs(network)
			for _, hp := range parsed {
				if ua, e := net.ResolveUDPAddr(network, hp); e == nil {
					addrs = append(addrs, dht.NewAddr(ua))
				}
			}
			return addrs, nil
		}
	}
	logging.For("engine").Info("extra dht bootstrap nodes", "count", len(parsed))
}
