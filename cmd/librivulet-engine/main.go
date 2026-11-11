// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Command librivulet-engine builds librivulet-engine.so, a c-shared version of
// rivulet-engine for SELinux-enforcing Android hosts where the shell's exec()
// of a bundled executable is blocked by policy but dlopen() of a shared
// library dropped next to the calling app's own native libs is not. It wraps
// internal/app.Run behind a tiny, panic-safe C ABI so a Kodi addon (or any
// other host process) can start/stop the server on a background thread
// in-process instead of spawning a child.
//
// Build: go build -buildmode=c-shared -o librivulet-engine.so ./cmd/librivulet-engine
//
// No os.Exit and no signal.Notify are reachable from this package or from
// internal/app: a fatal condition here must return an error code to the
// caller, never terminate the host process, and no signal handler is
// installed that could steal SIGINT/SIGTERM from the embedding app.
package main

/*
 */
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/app"
	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

// stopGrace bounds how long ServerStop waits for a blocking ServerStart call
// to return after cancellation before giving up (app.Run's own listener
// shutdowns are bounded at 5s each; this adds slack for goroutine teardown).
const stopGrace = 6 * time.Second

// serverVersionCStr is allocated exactly once and never freed: ServerVersion
// is documented as returning a static string the caller must not free, so a
// per-call C.CString (which the caller would then be responsible for
// freeing) would violate the contract. Deliberately leaked for the life of
// the process, like any other .rodata string.
var serverVersionCStr = C.CString(app.DefaultVersion)

// mu guards the fields below, which together describe the single in-process
// server lifecycle library mode supports. A Start after a Stop finds cancel
// and stopped both nil (Start's own goroutine-equivalent — the blocking
// ServerStart call itself — clears them right before returning), so there is
// no leaked sync.Once or other one-shot gate that would make a second Start
// silently do nothing.
var (
	mu      sync.Mutex
	cancel  context.CancelFunc
	stopped chan struct{}
)

// ServerVersion returns the Stremio-compatible API version string
// (settings.serverVersion), e.g. "4.21.0". The returned pointer is static;
// the caller must not free it.
//
//export ServerVersion
func ServerVersion() *C.char {
	defer func() { _ = recover() }()
	return serverVersionCStr
}

// ServerStart runs the server until ServerStop is called or an
// unrecoverable error occurs. It BLOCKS the calling thread for the entire
// server lifetime — callers embed it in a dedicated worker thread. logPath
// receives appended log output (created if missing); a nil/empty logPath
// falls back to the process's stderr. envJSON is a JSON object of string
// env-var values (APP_PATH, HTTP_PORT, RIVULET_*, ...) — the same knobs the
// rivulet-engine executable reads from its real environment; a nil/empty
// envJSON means "use every default".
//
// Returns 0 on a clean stop (ServerStop called, or ctx-equivalent shutdown),
// 1 on an init or runtime error, 2 if a server is already running.
//
//export ServerStart
func ServerStart(logPath *C.char, envJSON *C.char) (ret C.int) {
	defer func() {
		if r := recover(); r != nil {
			logging.For("librivulet-engine").Error("recovered from panic in ServerStart", "panic", r)
			mu.Lock()
			cancel = nil
			stopped = nil
			mu.Unlock()
			ret = 1
		}
	}()

	mu.Lock()
	if cancel != nil {
		mu.Unlock()
		return 2
	}
	runCtx, cancelFn := context.WithCancel(context.Background())
	cancel = cancelFn
	myStopped := make(chan struct{})
	stopped = myStopped
	mu.Unlock()

	// Always clear the shared lifecycle state before returning, however we
	// get there (clean stop, init error, or the recover above), so a later
	// ServerStart is never wrongly told "already running".
	defer func() {
		mu.Lock()
		cancel = nil
		stopped = nil
		mu.Unlock()
		close(myStopped)
		cancelFn()
	}()

	logw, closeLog := openLog(logPath)
	defer closeLog()

	envMap, err := parseEnvJSON(envJSON)
	if err != nil {
		_, _ = fmt.Fprintf(logw, "librivulet-engine: invalid envJSON: %v\n", err)
		return 1
	}
	defer applyOSEnv(envMap)()

	cfg := app.Config{Lookup: app.MapLookup(envMap)}
	if v, ok := envMap["RIVULET_SERVER_VERSION"]; ok && v != "" {
		cfg.Version = v
	}

	if err := app.Run(runCtx, cfg, logw); err != nil {
		_, _ = fmt.Fprintf(logw, "librivulet-engine: server exited with error: %v\n", err)
		return 1
	}
	return 0
}

// ServerStop requests a graceful shutdown (5s per listener, matching
// internal/app.Run) of a running ServerStart call and waits for it to
// return, up to stopGrace. Safe to call when no server is running (returns 0
// immediately) and safe to call more than once.
//
//export ServerStop
func ServerStop() (ret C.int) {
	defer func() {
		if r := recover(); r != nil {
			logging.For("librivulet-engine").Error("recovered from panic in ServerStop", "panic", r)
			ret = 0 // Stop is documented as always "safe"; never surface a Stop failure code
		}
	}()

	mu.Lock()
	c := cancel
	s := stopped
	mu.Unlock()

	if c == nil {
		return 0 // not running
	}
	c()
	if s != nil {
		select {
		case <-s:
		case <-time.After(stopGrace):
			logging.For("librivulet-engine").Warn("ServerStop: timed out waiting for ServerStart to return")
		}
	}
	return 0
}

// parseEnvJSON decodes a JSON object of string env-var values. A nil or
// blank envJSON yields an empty map (every knob defaults).
func parseEnvJSON(envJSON *C.char) (map[string]string, error) {
	if envJSON == nil {
		return map[string]string{}, nil
	}
	raw := strings.TrimSpace(C.GoString(envJSON))
	if raw == "" {
		return map[string]string{}, nil
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("decode envJSON: %w", err)
	}
	return m, nil
}

// openLog opens logPath for appending (creating it if needed) and returns it
// plus a close func. A nil/blank logPath falls back to os.Stderr silently; a
// path that fails to open falls back to os.Stderr and the failure is
// reported on stderr so it is never silent.
func openLog(logPath *C.char) (io.Writer, func()) {
	if logPath == nil {
		return os.Stderr, func() {}
	}
	return openLogFile(C.GoString(logPath), os.Stderr)
}

// openLogFile is the cgo-free core of openLog; warn receives the open
// failure message when the fallback is used.
func openLogFile(path string, warn io.Writer) (io.Writer, func()) {
	p := strings.TrimSpace(path)
	if p == "" {
		return os.Stderr, func() {}
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		_, _ = fmt.Fprintf(warn, "librivulet-engine: cannot open log file %q, logging to stderr: %v\n", p, err)
		return os.Stderr, func() {}
	}
	return f, func() { _ = f.Close() }
}

// osGetenvKeys are the knobs still read via os.Getenv directly in other
// packages (not threaded through Lookup); envJSON values for them must be
// mirrored into the process environment for library mode to honour them.
//
// PATH is here because ffmpeg, ffprobe and yt-dlp are run by bare name
// (exec.Command / exec.LookPath search os.Getenv("PATH")), and on Android
// Go's c-shared entry point (_rt0_*_android_lib) starts the runtime with no
// environment at all: whatever the host setenv()s never reaches os.Getenv,
// so without this the engine cannot find a host-bundled ffmpeg (issue #89).
// The restore is deferred to the end of ServerStart, i.e. the server's whole
// lifetime, so the value holds for every exec made while it runs.
var osGetenvKeys = []string{
	"PATH",
	"RIVULET_LOG_LEVEL",
	"RIVULET_LOG_FORMAT",
	"RIVULET_HWACCEL",
	"RIVULET_ARCHIVE_ALLOW_PRIVATE",
	"RIVULET_ARCHIVE_LOCAL_ROOT",
	"RIVULET_FTP_ALLOW_PRIVATE",
	"LOCAL_FILES_DIR",
}

// applyOSEnv exports the os.Getenv-based keys present in m into the process
// environment and returns a func restoring the previous values.
func applyOSEnv(m map[string]string) func() {
	type prev struct {
		key string
		val string
		set bool
	}
	var saved []prev
	for _, k := range osGetenvKeys {
		v, ok := m[k]
		if !ok {
			continue
		}
		old, was := os.LookupEnv(k)
		saved = append(saved, prev{k, old, was})
		_ = os.Setenv(k, v)
	}
	return func() {
		for _, p := range saved {
			if p.set {
				_ = os.Setenv(p.key, p.val)
			} else {
				_ = os.Unsetenv(p.key)
			}
		}
	}
}

// main is required for a c-shared buildmode package but is never executed;
// the library has no process lifetime of its own.
func main() {}
