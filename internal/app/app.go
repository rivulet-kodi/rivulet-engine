// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // G108: pprof is opt-in via RIVULET_PPROF, on its own listener, never the main handler
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/api"
	"github.com/rivulet-kodi/rivulet-engine/internal/earlyenv"
	"github.com/rivulet-kodi/rivulet-engine/internal/engine"
	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/media"
	"github.com/rivulet-kodi/rivulet-engine/internal/settings"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// DefaultVersion is the Stremio-compatible serverVersion reported when
// Config.Version is empty; keep it aligned with a real Stremio server
// version so stremio-web does not gate features.
const DefaultVersion = "4.21.0"

// Config configures a single Run call. It intentionally does not embed
// types.Config directly: types.Config is built from Lookup inside Run so the
// exact same code path serves both the os.Getenv-backed executable and a
// JSON-map-backed library-mode caller.
type Config struct {
	// Lookup resolves configuration keys (env var names). Nil defaults to
	// OSLookup (the real process environment).
	Lookup Lookup
	// Version is reported as settings.serverVersion. Empty defaults to
	// DefaultVersion.
	Version string
}

// safeGo runs fn in a new goroutine, recovering any panic so a bug in a
// background task never brings down the whole host process — critical in
// library mode, where a panic crosses into a shared library loaded by an
// Android app via dlopen/JNI rather than a disposable standalone process.
func safeGo(component string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.For(component).Error("recovered from panic", "panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

// Run wires and serves the enginefs HTTP(S) API until ctx is cancelled or an
// unrecoverable startup/listener error occurs, then shuts down gracefully and
// returns. It builds all state fresh on every call (no package-level state is
// mutated in a way that would break a later call), so it may be invoked again
// after a prior call has returned — the basis for library mode's
// Stop-then-Start-again contract. logw receives the process log output; nil
// defaults to os.Stderr.
func Run(ctx context.Context, cfg Config, logw io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logging.Setup(logw)
	if earlyenv.Applied {
		logging.For("engine").Info("32-bit build: using classic file I/O for torrent storage (mmap cannot map files of 4 GiB or more)")
	}

	lookup := cfg.Lookup
	if lookup == nil {
		lookup = OSLookup
	}
	version := cfg.Version
	if version == "" {
		version = DefaultVersion
	}

	home, homeErr := os.UserHomeDir()
	if homeErr != nil {
		logging.For("config").Warn("cannot determine home directory; using /tmp", "err", homeErr)
		home = "/tmp"
	}
	appPath := getenv(lookup, "APP_PATH", "")
	if appPath == "" {
		appPath = defaultAppPath(home)
	}
	if err := os.MkdirAll(appPath, 0o755); err != nil {
		return fmt.Errorf("cannot create app path %s: %w", appPath, err)
	}

	// RIVULET_CACHE_ROOT moves the disk piece cache off APP_PATH, e.g. to a
	// tmpfs, USB drive or HDD when APP_PATH sits on an SD card that streaming
	// would wear out (rivulet-kodi/plugin.video.rivulet#96).
	cacheRoot := strings.TrimSpace(getenv(lookup, "RIVULET_CACHE_ROOT", ""))
	if cacheRoot == "" {
		cacheRoot = appPath
	} else if err := os.MkdirAll(cacheRoot, 0o755); err != nil {
		return fmt.Errorf("cannot create cache root %s: %w", cacheRoot, err)
	}

	allowedOriginsExtra, allowAllOrigins := allowedOrigins(lookup)
	proxySecretVal, err := proxySecret(appPath, lookup)
	if err != nil {
		return err
	}
	publicURL := publicBaseURL(lookup, "RIVULET_PUBLIC_URL")
	// RIVULET_PUBLIC_URL is the general base the stream proxy falls back to
	// when its own URL is unset. It deliberately does NOT enable the
	// local-files HTTP endpoint: that stays an explicit opt-in via
	// RIVULET_LOCAL_FILES_PUBLIC_URL, since it exposes file downloads.
	proxyPublicURL := strings.TrimRight(getenv(lookup, "RIVULET_PROXY_PUBLIC_URL", ""), "/")
	if proxyPublicURL == "" {
		proxyPublicURL = publicURL
	}
	localFilesPublicURL := strings.TrimRight(getenv(lookup, "RIVULET_LOCAL_FILES_PUBLIC_URL", ""), "/")
	tcfg := types.Config{
		HTTPPort:            envInt(lookup, "HTTP_PORT", 11470),
		HTTPSPort:           envInt(lookup, "HTTPS_PORT", 12470), // self-signed HTTPS for https web UIs (WebKitGTK)
		AppPath:             appPath,
		CacheRoot:           cacheRoot,
		MemoryCacheSize:     envInt64(lookup, "RIVULET_MEMORY_CACHE_SIZE", 0),                                                    // bytes; 0 = disabled (write pieces to disk)
		StorageMmap:         strings.EqualFold(strings.TrimSpace(getenv(lookup, "TORRENT_STORAGE_DEFAULT_FILE_IO", "")), "mmap"), // default classic pread/pwrite; "mmap" opts in
		ListenPort:          envInt(lookup, "BT_LISTEN_PORT", 0),
		WebUI:               getenv(lookup, "WEB_UI_LOCATION", "https://web.stremio.com/"),
		PublicURL:           publicURL,
		Version:             version,
		TrackersMax:         envInt(lookup, "RIVULET_TRACKERS_MAX", 5),
		ProxyPassword:       getenv(lookup, "RIVULET_PROXY_PASSWORD", ""),
		ProxySecret:         proxySecretVal,
		ProxyIPACL:          getenv(lookup, "RIVULET_PROXY_IP_ACL", ""),
		ProxyPrebuffer:      envInt(lookup, "RIVULET_PROXY_PREBUFFER", 3),
		ProxySegCacheTTL:    envInt(lookup, "RIVULET_PROXY_SEG_CACHE_TTL", 300),
		ProxyPublicURL:      proxyPublicURL,
		ProxyUpstream:       getenv(lookup, "RIVULET_PROXY_UPSTREAM", ""),
		ProxyPrivateAllow:   getenv(lookup, "RIVULET_PROXY_PRIVATE_ALLOW", ""), // IPs/CIDRs/hostnames reachable even when private destinations are blocked
		ExtractorsFile:      getenv(lookup, "RIVULET_EXTRACTORS_FILE", ""),
		ExtractorsURL:       optionalURL(lookup, "RIVULET_EXTRACTORS_URL"), // remote extractor definitions; "" / off disables
		ExtractorsPubKey:    getenv(lookup, "RIVULET_EXTRACTORS_PUBKEY", ""),
		BitmagnetURL:        getenv(lookup, "RIVULET_BITMAGNET_URL", ""),
		TorznabURL:          getenv(lookup, "RIVULET_TORZNAB_URL", ""),
		TorznabAPIKey:       getenv(lookup, "RIVULET_TORZNAB_APIKEY", ""),
		MetadataURL:         metadataURL(lookup),                                 // Cinemeta-compatible meta addon base; "" disables (RIVULET_METADATA_URL)
		DisableTrackers:     envBool(lookup, "RIVULET_DISABLE_TRACKERS", false),  // disable all tracker announces (DHT/PEX/webseeds still used); accepts 1/true/0/false
		DisableWebtorrent:   envBool(lookup, "RIVULET_DISABLE_WEBTORRENT", true), // default disabled; set =0/false to enable WebRTC/WebTorrent (pion) peers
		EnableDLNA:          envBool(lookup, "RIVULET_ENABLE_DLNA", false),       // default disabled; set =1/true to enable /casting DLNA discovery + control
		PeersPerTorrent:     envInt(lookup, "RIVULET_PEERS_PER_TORRENT", 0),      // 0 = default 50/25/500; lower (e.g. 30) trims peer goroutines & RAM
		TrackersURL:         trackersURL(lookup),                                 // remote tracker list; "" disables remote fetch (RIVULET_TRACKERS_URL)
		LocalIMDB:           envBool(lookup, "RIVULET_LOCAL_IMDB", true),         // local-files addon IMDB resolution; default on
		LocalFilesPublicURL: localFilesPublicURL,
		BTEncryption:        getenv(lookup, "RIVULET_BT_ENCRYPTION", "prefer"),
		BTProxy:             getenv(lookup, "RIVULET_BT_PROXY", ""),
		DHTBootstrap:        getenv(lookup, "RIVULET_DHT_BOOTSTRAP", ""),
		BTAnonymous:         envBool(lookup, "RIVULET_BT_ANONYMOUS", false),
		IdleTimeout:         time.Duration(envInt(lookup, "RIVULET_TORRENT_IDLE_TIMEOUT", 300)) * time.Second, // 0 = disabled
		MaxSeedRatio:        envFloat(lookup, "RIVULET_MAX_SEED_RATIO", 0),                                    // 0 = unlimited seeding
		HTTPLog:             envBool(lookup, "RIVULET_HTTP_LOG", false),                                       // structured access-log line per request
		AllowedOrigins:      allowedOriginsExtra,                                                              // extra CORS origins (RIVULET_ALLOWED_ORIGINS)
		AllowAllOrigins:     allowAllOrigins,                                                                  // RIVULET_ALLOWED_ORIGINS="*" => legacy no-check CORS
		// CreateMetadataWait bounds /create, /{infoHash}/create, and
		// /{infoHash}/{fileIdx}'s wait for a torrent's metadata (issue #20).
		CreateMetadataWait: envDuration(lookup, "RIVULET_CREATE_METADATA_TIMEOUT", 90*time.Second),
		DVREnabled:         envBool(lookup, "RIVULET_DVR_ENABLED", false), // opt-in /record DVR (EasyProxy-compatible)
		DVRDir:             getenv(lookup, "RIVULET_DVR_DIR", ""),         // "" = <APP_PATH>/recordings
		DVRDefaultDuration: envDuration(lookup, "RIVULET_DVR_DEFAULT_DURATION", 4*time.Hour),
		DVRMaxDuration:     envDuration(lookup, "RIVULET_DVR_MAX_DURATION", 8*time.Hour),
		DVRMaxActive:       envInt(lookup, "RIVULET_DVR_MAX_ACTIVE", 2),
		DVRMaxBytes:        envInt64(lookup, "RIVULET_DVR_MAX_BYTES", 0), // 0 = unlimited
		DVRRetentionDays:   envInt(lookup, "RIVULET_DVR_RETENTION_DAYS", 7),
	}
	if tcfg.DisableWebtorrent {
		logging.For("engine").Info("webtorrent/webrtc peers disabled")
	}
	if !tcfg.EnableDLNA {
		logging.For("casting").Info("dlna disabled")
	}
	if tcfg.MetadataURL == "" {
		logging.For("metadata").Info("metadata (cinemeta) resolution disabled")
	}
	if tcfg.TrackersURL == "" {
		logging.For("engine").Info("remote tracker list disabled (DHT/PEX/embedded only)")
	}
	if !tcfg.LocalIMDB {
		logging.For("localaddon").Info("IMDB resolution disabled")
	}
	if tcfg.BTEncryption == "require" {
		logging.For("engine").Info("bittorrent encryption required (plaintext peers refused)")
	}
	if tcfg.BTEncryption == "disable" {
		logging.For("engine").Info("bittorrent encryption disabled")
	}
	if tcfg.BTProxy != "" {
		logging.For("engine").Info("bittorrent proxy configured (trackers/webseeds/metainfo only; peers direct)", "proxy", redactProxyURL(tcfg.BTProxy))
	}
	if tcfg.DHTBootstrap != "" {
		logging.For("engine").Info("extra dht bootstrap nodes configured", "nodes", tcfg.DHTBootstrap)
	}
	if tcfg.BTAnonymous {
		logging.For("engine").Info("anonymous mode enabled (client fingerprint hidden)")
	}
	if tcfg.IdleTimeout <= 0 {
		logging.For("engine").Info("idle torrent removal disabled")
	} else {
		logging.For("engine").Info("idle torrent removal enabled", "timeout", tcfg.IdleTimeout.String())
	}
	if tcfg.MaxSeedRatio > 0 {
		logging.For("engine").Info("max seed ratio enabled", "max_ratio", tcfg.MaxSeedRatio)
	}

	// Optional soft memory ceiling for RAM-constrained hosts (the runtime also
	// honors the native GOMEMLIMIT env). 0 = unset.
	if lim := envInt64(lookup, "RIVULET_MEM_LIMIT", 0); lim > 0 {
		debug.SetMemoryLimit(lim)
		logging.For("runtime").Info("soft memory limit set", "bytes", lim)
	}
	// Periodically return reclaimable idle heap (e.g. freed after a large
	// torrent or cache drained) to the OS. Guarded by a threshold so an idle or
	// steady-state server skips the forced GC entirely instead of paying a
	// stop-the-world collection on every tick. Stops when ctx is cancelled so a
	// restart never leaks this goroutine.
	safeGo("runtime", func() {
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapIdle-ms.HeapReleased > 64<<20 {
					debug.FreeOSMemory()
				}
			}
		}
	})

	ss, err := settings.New(tcfg)
	if err != nil {
		return fmt.Errorf("settings init failed: %w", err)
	}
	em, err := engine.New(tcfg)
	if err != nil {
		return fmt.Errorf("engine init failed: %w", err)
	}
	defer func() { _ = em.Close() }()

	// Wire the cache-eviction janitor without adding to the types interface.
	// StartJanitor is detected via structural type assertion on the concrete *manager.
	if j, ok := em.(interface{ StartJanitor(func() int64) }); ok {
		j.StartJanitor(func() int64 {
			// Contract 1: nil/unknown/non-numeric => -1 (unlimited); numeric =>
			// int64(n); a negative number is normalized to -1. engine.evict
			// interprets <0 as unlimited, 0 as "no caching" (purge readerless
			// engines past the grace window), >0 as a byte cap.
			switch n := ss.Get("cacheSize").(type) {
			case float64:
				if n < 0 {
					return -1
				}
				return int64(n)
			case int:
				if n < 0 {
					return -1
				}
				return int64(n)
			case int64:
				if n < 0 {
					return -1
				}
				return n
			default:
				return -1 // nil/unknown => unlimited
			}
		})
	}

	// Wire live bandwidth limits from settings — no changes to the types interface.
	// SetLimitFn is detected via structural assertion on the concrete *manager, matching
	// the pattern used for StartJanitor above.
	//   • btDownloadSpeedHardLimit: 0 = unlimited; positive = bytes/sec download cap.
	//   • seedingEnabled: false → upload effectively disabled (1 byte/sec); true → unlimited.
	if l, ok := em.(interface{ SetLimitFn(func() (int64, int64)) }); ok {
		l.SetLimitFn(func() (int64, int64) {
			// --- download cap ---
			var down int64
			switch n := ss.Get("btDownloadSpeedHardLimit").(type) {
			case float64:
				if n > 0 {
					down = int64(n)
				}
			case int:
				if n > 0 {
					down = int64(n)
				}
			case int64:
				if n > 0 {
					down = n
				}
			}

			// --- upload cap ---
			// seedingEnabled=false → 1 byte/sec (effectively no upload) so peers
			// get valid rate.Limiter reservations but seeding is negligible.
			// seedingEnabled=true → 0 (unlimited).
			var up int64
			if seeding, _ := ss.Get("seedingEnabled").(bool); !seeding {
				up = 1
			}

			return down, up
		})
	}

	// Wire the live peer-discovery soft limit from settings, same structural-
	// assertion pattern as SetLimitFn above.
	//   • btDownloadSpeedSoftLimit: 0 = disabled; positive = bytes/sec threshold
	//     above which peer discovery pauses once btMinPeersForStable peers are
	//     already connected (official semantics: NOT a throughput cap — see
	//     engine.SetSoftLimitFn).
	//   • btMinPeersForStable: minimum connected peers required before the
	//     soft limit is allowed to pause discovery.
	if l, ok := em.(interface {
		SetSoftLimitFn(func() (int64, int))
	}); ok {
		l.SetSoftLimitFn(func() (int64, int) {
			var soft int64
			switch n := ss.Get("btDownloadSpeedSoftLimit").(type) {
			case float64:
				if n > 0 {
					soft = int64(n)
				}
			case int:
				if n > 0 {
					soft = int64(n)
				}
			case int64:
				if n > 0 {
					soft = n
				}
			}

			minPeers := 0
			switch n := ss.Get("btMinPeersForStable").(type) {
			case float64:
				minPeers = int(n)
			case int:
				minPeers = n
			case int64:
				minPeers = int(n)
			}

			return soft, minPeers
		})
	}

	// BIND_ADDRESS restricts which network interface(s) the HTTP/HTTPS
	// listeners accept connections on. Empty (default) preserves the
	// historical all-interfaces behaviour (net.JoinHostPort("", port) ==
	// ":<port>", i.e. 0.0.0.0 + ::) — not a breaking change. This server has
	// no authentication by design, so operators on a multi-homed or
	// publicly-routable host should set this to a loopback or LAN-only
	// address (e.g. BIND_ADDRESS=127.0.0.1).
	bindAddr := getenv(lookup, "BIND_ADDRESS", "")
	wildcardBind := isWildcardBindHost(bindAddr)
	if wildcardBind {
		logging.For("http").Warn("no BIND_ADDRESS set; the unauthenticated API is reachable from every network interface", "bind_address", bindAddr)
	}
	httpAddr := net.JoinHostPort(bindAddr, strconv.Itoa(tcfg.HTTPPort))

	// Self URL vs BIND_ADDRESS (issue #20): ffmpeg/ffprobe (HLS transcode,
	// probe, /yt, /proxy self-fetch) always reach this server through
	// baseLocal, a loopback URL. When BIND_ADDRESS pins the main listener to
	// a specific non-loopback, non-wildcard interface, nothing listens on
	// loopback anymore and those self-requests would fail outright. Start a
	// dedicated loopback listener on 127.0.0.1:HTTP_PORT — same handler,
	// same ctx-driven shutdown, same recover wrapper as every other listener
	// below (see loopbackSrv/shutOne) — so ffmpeg/ffprobe keep working; if
	// that bind itself fails, fall back to reaching the server via the
	// configured bind address instead and warn loudly. Wildcard/loopback
	// binds are unaffected — loopback is already reachable either way.
	loopbackNeeded := !wildcardBind && !isLoopbackAddr(net.JoinHostPort(bindAddr, "0"))
	var loopbackLn net.Listener
	selfHost := selfHostFor(bindAddr)
	if loopbackNeeded {
		loopbackAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(tcfg.HTTPPort))
		ln, lerr := net.Listen("tcp", loopbackAddr)
		if lerr != nil {
			logging.For("http").Warn("BIND_ADDRESS is a non-loopback address and the fallback loopback listener failed to bind; ffmpeg/ffprobe self-requests will use the bind address instead", "bind_address", bindAddr, "loopback_addr", loopbackAddr, "err", lerr)
			selfHost = bindAddr
		} else {
			loopbackLn = ln
		}
	}
	baseLocal := fmt.Sprintf("http://%s", net.JoinHostPort(selfHost, strconv.Itoa(tcfg.HTTPPort)))
	tcfg.SelfURL = baseLocal
	prober := media.New(baseLocal, hlsConfig(lookup), ss)
	// prober owns a per-instance background reaper goroutine (and a working
	// directory) created fresh by media.New on every Run call; close it via
	// structural assertion on Run exit so a restart never leaks either one.
	if c, ok := prober.(interface{ CloseHLS() }); ok {
		defer c.CloseHLS()
	}

	handler := api.New(em, ss, prober, tcfg)

	// loopbackSrv serves the fallback loopback listener obtained above, when
	// one was needed and bound successfully.
	var loopbackSrv *http.Server
	if loopbackLn != nil {
		loopbackSrv = &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		safeGo("http-loopback", func() {
			logging.For("http-loopback").Info("listening (loopback fallback for BIND_ADDRESS self-requests)", "addr", loopbackLn.Addr().String())
			if err := loopbackSrv.Serve(loopbackLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logging.For("http-loopback").Error("server error", "err", err)
			}
		})
	}

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// fatalCh receives a listener error serious enough to end Run early (e.g.
	// the primary HTTP listener failed to bind). Buffered 1: at most the first
	// reporter's send is ever read; later sends are dropped via select/default.
	fatalCh := make(chan error, 1)
	reportFatal := func(err error) {
		select {
		case fatalCh <- err:
		default:
		}
	}

	// HTTPS cert setup (including the certReload hook GET /get-https invokes)
	// happens before the HTTP listener goroutine is spawned below: that
	// handler is shared by both listeners, so installing the hook first
	// closes the API-1 startup-window race where a very early GET /get-https
	// on plain HTTP could observe the hook still unset.
	var tlsSrv *http.Server
	if tcfg.HTTPSPort > 0 {
		// Prefer an API-issued cert (written by GET /get-https) over a self-signed one.
		// Both use the same HTTPS listener; cert preference is transparent to clients.
		certFile := filepath.Join(appPath, "https-cert.pem")
		keyFile := filepath.Join(appPath, "https-key.pem")
		cert, certErr := tls.LoadX509KeyPair(certFile, keyFile)
		if certErr != nil {
			logging.For("https").Warn("no persisted cert; falling back to self-signed", "err", certErr)
			cert, certErr = selfSignedCert()
		} else {
			logging.For("https").Info("using persisted cert", "path", certFile)
		}
		if certErr != nil {
			logging.For("https").Warn("cert init failed; https disabled", "err", certErr)
		} else {
			holder := &certHolder{}
			holder.set(cert)
			if h, ok := handler.(interface{ SetCertReloadHook(func()) }); ok {
				h.SetCertReloadHook(func() {
					c, err := tls.LoadX509KeyPair(certFile, keyFile)
					if err != nil {
						logging.For("https").Error("live cert reload failed", "err", err)
						return
					}
					holder.set(c)
					logging.For("https").Info("live cert reloaded", "path", certFile)
				})
			}
			httpsAddr := net.JoinHostPort(bindAddr, strconv.Itoa(tcfg.HTTPSPort))
			tlsSrv = &http.Server{
				Addr:              httpsAddr,
				Handler:           handler,
				ReadHeaderTimeout: 10 * time.Second,
				ReadTimeout:       30 * time.Second,
				IdleTimeout:       120 * time.Second,
				TLSConfig:         &tls.Config{GetCertificate: holder.get, MinVersion: tls.VersionTLS12},
			}
			safeGo("https", func() {
				logging.For("https").Info("listening", "version", version, "addr", fmt.Sprintf("https://127.0.0.1:%d", tcfg.HTTPSPort), "bind_addr", httpsAddr, "app_path", appPath)
				if err := tlsSrv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logging.For("https").Error("server error", "err", err)
				}
			})
			// Auto-provision/renew a browser-trusted cert from api.strem.io when an
			// authKey is available (env RIVULET_CERT_AUTHKEY or cached by a prior
			// /get-https call). Hot-swaps the live cert; no restart needed. Stops
			// when ctx is cancelled.
			safeGo("https", func() { renewCertLoop(ctx, appPath, holder, lookup) })
		}
	}

	safeGo("http", func() {
		logging.For("http").Info("listening", "version", version, "addr", baseLocal, "bind_addr", httpAddr, "app_path", appPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			reportFatal(fmt.Errorf("http server error: %w", err))
		}
	})
	// Resume persisted HLS preparation jobs now that the listener that feeds
	// ffmpeg its input is starting (RIVULET_HLS_PREPARE + RIVULET_HLS_PERSIST).
	if r, ok := prober.(interface{ ResumeHLSPrepare() }); ok {
		r.ResumeHLSPrepare()
	}

	// Optional pprof endpoint for diagnostics; disabled unless RIVULET_PPROF is
	// set (e.g. RIVULET_PPROF=127.0.0.1:6060). Handlers come from net/http/pprof.
	// The address is operator-supplied and NOT forced to loopback, so warn when
	// it would expose heap/goroutine dumps beyond the local host.
	// ppSrv is declared here so it can join the graceful-shutdown WaitGroup below.
	var ppSrv *http.Server
	if addr := getenv(lookup, "RIVULET_PPROF", ""); addr != "" {
		ppSrv = &http.Server{Addr: addr, ReadHeaderTimeout: 10 * time.Second}
		if !isLoopbackAddr(addr) {
			logging.For("pprof").Warn("pprof bound to a non-loopback address; heap and goroutine dumps are exposed", "addr", addr)
		}
		safeGo("pprof", func() {
			logging.For("pprof").Info("listening", "addr", addr)
			if err := ppSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logging.For("pprof").Error("server error", "err", err)
			}
		})
	}

	// Block until asked to stop (ctx cancelled — the executable's signal
	// handler or library mode's ServerStop) or a listener reports a fatal
	// startup error.
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-fatalCh:
	}
	logging.For("http").Info("shutting down")
	// Tell the HLS manager first: draining the servers (and stopping the
	// engines) can cut off the streams this process feeds a running ffmpeg,
	// whose output must then not be cached.
	if b, ok := prober.(interface{ BeginShutdown() }); ok {
		b.BeginShutdown()
	}

	var shutWg sync.WaitGroup
	shutOne := func(s *http.Server, name string) {
		defer shutWg.Done()
		defer func() {
			if r := recover(); r != nil {
				logging.For(name).Error("recovered from panic during shutdown", "panic", r)
			}
		}()
		// ctx is already cancelled here; drain on a detached deadline.
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		shutdownServer(sctx, s, name)
	}
	shutWg.Add(1)
	go shutOne(srv, "http")
	if loopbackSrv != nil {
		shutWg.Add(1)
		go shutOne(loopbackSrv, "http-loopback")
	}
	if tlsSrv != nil {
		shutWg.Add(1)
		go shutOne(tlsSrv, "https")
	}
	if ppSrv != nil {
		shutWg.Add(1)
		go shutOne(ppSrv, "pprof")
	}
	shutWg.Wait()
	if c, ok := handler.(io.Closer); ok {
		_ = c.Close()
	}

	return runErr
}

// proxySecret returns the proxy signing secret.
// Priority: RIVULET_PROXY_SECRET env var > <appPath>/proxy-secret file > auto-generated.
// A generated secret is persisted to <appPath>/proxy-secret (mode 0o600).
func proxySecret(appPath string, lookup Lookup) (string, error) {
	if s, ok := lookup("RIVULET_PROXY_SECRET"); ok && s != "" {
		return s, nil
	}
	secretFile := filepath.Join(appPath, "proxy-secret")
	if data, err := os.ReadFile(secretFile); err == nil {
		if s := strings.TrimSpace(string(data)); s != "" {
			return s, nil
		}
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("failed to generate proxy secret: %w", err)
	}
	secret := hex.EncodeToString(buf)
	tmp := secretFile + ".tmp"
	if err := os.WriteFile(tmp, []byte(secret), 0o600); err != nil {
		logging.For("proxy").Warn("failed to persist secret", "path", secretFile, "err", err)
	} else if err := os.Rename(tmp, secretFile); err != nil {
		_ = os.Remove(tmp)
		logging.For("proxy").Warn("failed to persist secret", "path", secretFile, "err", err)
	}
	return secret, nil
}

// selfHostFor returns the host ffmpeg/ffprobe self-requests should use: the
// bind address itself for a non-wildcard loopback bind (e.g. ::1, 127.0.0.2),
// otherwise 127.0.0.1.
func selfHostFor(bindAddr string) string {
	if !isWildcardBindHost(bindAddr) && isLoopbackAddr(net.JoinHostPort(bindAddr, "0")) {
		return bindAddr
	}
	return "127.0.0.1"
}

// redactProxyURL masks any userinfo password in a proxy URL for logging.
func redactProxyURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		if err != nil {
			return "<unparseable>"
		}
		return raw
	}
	return u.Redacted()
}

// isWildcardBindHost reports whether host (a BIND_ADDRESS value — the host
// part only, never host:port) leaves the listener reachable from every
// network interface: empty (net.JoinHostPort's all-interfaces default),
// "0.0.0.0" (all IPv4 interfaces), or "::" (all IPv6 interfaces).
func isWildcardBindHost(host string) bool {
	return host == "" || host == "0.0.0.0" || host == "::"
}

// isLoopbackAddr reports whether a host:port listen address binds only the
// local host. An empty or wildcard host (":6060", "0.0.0.0:6060", "[::]:6060")
// is treated as non-loopback, as is any hostname that does not resolve to a
// loopback IP.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return false
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}
	return true
}

// hlsConfig resolves media.HLSConfig from the process environment (issue
// #20). Every field mirrors a previously-hardcoded internal/media constant;
// media.DefaultHLSConfig() values are kept whenever the corresponding env
// var is unset, so unconfigured output stays byte-identical to before these
// knobs existed. Live /settings overrides (transcodeMaxBitRate,
// transcodeMaxWidth, transcodeConcurrency, transcodeHardwareAccel,
// transcodeProfile) are applied downstream by internal/media at HLS
// session-creation time — see media.HLSConfig and effectiveSessionConfig.
func hlsConfig(lookup Lookup) media.HLSConfig {
	d := media.DefaultHLSConfig()
	// envDuration keeps unset/negative/invalid at the 60s default but returns
	// an explicit "0" (or "0s") as 0, which means "never evict idle sessions"
	// (sessions then live until DELETE /hlsv2/{id}), like
	// RIVULET_TORRENT_IDLE_TIMEOUT=0. SessionTTL itself stays 0 here;
	// media's normalize resolves it back to the default, which is harmless
	// because DisableIdleEviction short-circuits the reaper, and
	// DefaultReaperInterval(0) keeps the 30s tick instead of a zero interval.
	sessionTTL := envDuration(lookup, "RIVULET_HLS_SESSION_TTL", d.SessionTTL)
	maxWidth := envInt(lookup, "RIVULET_TRANSCODE_MAX_WIDTH", d.MaxWidth)
	if maxWidth < 0 {
		logging.For("config").Warn("negative RIVULET_TRANSCODE_MAX_WIDTH", "value", maxWidth, "default", d.MaxWidth)
		maxWidth = d.MaxWidth
	}
	maxHeight := envInt(lookup, "RIVULET_TRANSCODE_MAX_HEIGHT", d.MaxHeight)
	if maxHeight < 0 {
		logging.For("config").Warn("negative RIVULET_TRANSCODE_MAX_HEIGHT", "value", maxHeight, "default", d.MaxHeight)
		maxHeight = d.MaxHeight
	}
	transcodeAhead := envInt(lookup, "RIVULET_HLS_TRANSCODE_AHEAD", d.TranscodeAhead)
	switch {
	case transcodeAhead < 0:
		logging.For("config").Warn("negative RIVULET_HLS_TRANSCODE_AHEAD", "value", transcodeAhead, "default", d.TranscodeAhead)
		transcodeAhead = d.TranscodeAhead
	case transcodeAhead > media.MaxTranscodeAhead:
		logging.For("config").Warn("RIVULET_HLS_TRANSCODE_AHEAD above the maximum; capped", "value", transcodeAhead, "max", media.MaxTranscodeAhead)
		transcodeAhead = media.MaxTranscodeAhead
	}
	relayCacheBytes := envInt64(lookup, "RIVULET_HLS_RELAY_CACHE_BYTES", d.RelayCacheBytes)
	if relayCacheBytes < 0 {
		logging.For("config").Warn("negative RIVULET_HLS_RELAY_CACHE_BYTES; cache off", "value", relayCacheBytes)
		relayCacheBytes = 0
	}
	return media.HLSConfig{
		SessionTTL:     sessionTTL,
		ReaperInterval: envDuration(lookup, "RIVULET_HLS_REAPER_INTERVAL", media.DefaultReaperInterval(sessionTTL)),
		NegProbeTTL:    envDuration(lookup, "RIVULET_HLS_NEG_PROBE_TTL", d.NegProbeTTL),
		PosProbeTTL:    envDuration(lookup, "RIVULET_HLS_POS_PROBE_TTL", d.PosProbeTTL),
		MaxSessions:    envInt(lookup, "RIVULET_HLS_MAX_SESSIONS", d.MaxSessions),
		WorkDir:        getenv(lookup, "RIVULET_HLS_WORK_DIR", d.WorkDir),
		Persist:        envBool(lookup, "RIVULET_HLS_PERSIST", d.Persist),

		// Engine-side preparation (POST /hlsv2/{id}/prepare); off by default.
		Prepare:            envBool(lookup, "RIVULET_HLS_PREPARE", d.Prepare),
		PrepareConcurrency: envInt(lookup, "RIVULET_HLS_PREPARE_CONCURRENCY", d.PrepareConcurrency),

		// Transcode-ahead (ahead.go); off by default.
		TranscodeAhead:   transcodeAhead,
		AheadConcurrency: envInt(lookup, "RIVULET_HLS_TRANSCODE_AHEAD_CONCURRENCY", d.AheadConcurrency),

		DisableIdleEviction: sessionTTL == 0,

		SessionOverrides: envBool(lookup, "RIVULET_HLS_SESSION_OVERRIDES", d.SessionOverrides),
		VideoBitrate:     envBitrate(lookup, "RIVULET_TRANSCODE_VIDEO_BITRATE", d.VideoBitrate),
		VideoMaxrate:     envBitrate(lookup, "RIVULET_TRANSCODE_MAXRATE", d.VideoMaxrate),
		VideoBufsize:     envBitrate(lookup, "RIVULET_TRANSCODE_BUFSIZE", d.VideoBufsize),
		MaxWidth:         maxWidth,
		MaxHeight:        maxHeight,

		VAAPIQP:     envInt(lookup, "RIVULET_TRANSCODE_VAAPI_QP", d.VAAPIQP),
		NVENCPreset: getenv(lookup, "RIVULET_TRANSCODE_NVENC_PRESET", d.NVENCPreset),
		QSVPreset:   getenv(lookup, "RIVULET_TRANSCODE_QSV_PRESET", d.QSVPreset),
		X264Preset:  getenv(lookup, "RIVULET_TRANSCODE_X264_PRESET", d.X264Preset),
		X264CRF:     envInt(lookup, "RIVULET_TRANSCODE_X264_CRF", d.X264CRF),
		Tonemap:     envTonemap(lookup, "RIVULET_TRANSCODE_TONEMAP"),

		AudioChannels: envInt(lookup, "RIVULET_TRANSCODE_AUDIO_CHANNELS", d.AudioChannels),
		AudioBitrate:  envBitrate(lookup, "RIVULET_TRANSCODE_AUDIO_BITRATE", d.AudioBitrate),

		// 0 (unset) resolves to runtime.NumCPU() inside newHLS/HLSConfig.normalize.
		SegmentConcurrency: envInt(lookup, "RIVULET_TRANSCODE_CONCURRENCY", 0),
		VAAPIDevice:        getenv(lookup, "RIVULET_HLS_VAAPI_DEVICE", d.VAAPIDevice),

		SegmentTimeout:  envDuration(lookup, "RIVULET_HLS_SEGMENT_TIMEOUT", d.SegmentTimeout),
		SubtitleTimeout: envDuration(lookup, "RIVULET_HLS_SUBTITLE_TIMEOUT", d.SubtitleTimeout),
		ProbeTimeout:    envDuration(lookup, "RIVULET_HLS_PROBE_TIMEOUT", d.ProbeTimeout),

		SeekPreroll: seekPreroll(lookup, d.SeekPreroll),

		// Per-session transcoded-segment disk cap: 0 = derived from bitrate,
		// negative = unlimited (RIVULET_HLS_SEGMENT_CACHE_BYTES).
		SegmentCacheBytes: envInt64(lookup, "RIVULET_HLS_SEGMENT_CACHE_BYTES", d.SegmentCacheBytes),

		// Private hosts a media URL may point at (RIVULET_HLS_PRIVATE_ALLOW);
		// unset allows none.
		PrivateAllow: envPrivateAllow(lookup, "RIVULET_HLS_PRIVATE_ALLOW"),

		// Memory for the relay's header/index cache
		// (RIVULET_HLS_RELAY_CACHE_BYTES); 0 = off.
		RelayCacheBytes: relayCacheBytes,
	}
}

// shutdownServer gracefully drains s until ctx expires, then force-closes any
// lingering connections so a stuck stream cannot outlive the shutdown window.
func shutdownServer(ctx context.Context, s *http.Server, name string) {
	err := s.Shutdown(ctx)
	if err == nil {
		return
	}
	logging.For(name).Error("shutdown error", "err", err)
	if errors.Is(err, context.DeadlineExceeded) {
		_ = s.Close()
	}
}
