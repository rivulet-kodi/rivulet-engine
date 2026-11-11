// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestApplyOSEnv(t *testing.T) {
	t.Setenv("RIVULET_HWACCEL", "orig")
	os.Unsetenv("LOCAL_FILES_DIR")
	restore := applyOSEnv(map[string]string{
		"RIVULET_HWACCEL": "0",
		"LOCAL_FILES_DIR": "/x",
		"HTTP_PORT":       "1",
	})
	if os.Getenv("RIVULET_HWACCEL") != "0" || os.Getenv("LOCAL_FILES_DIR") != "/x" {
		t.Fatal("env not applied")
	}
	if _, ok := os.LookupEnv("HTTP_PORT"); ok {
		t.Fatal("non-allowlisted key leaked into env")
	}
	restore()
	if os.Getenv("RIVULET_HWACCEL") != "orig" {
		t.Fatal("not restored")
	}
	if _, ok := os.LookupEnv("LOCAL_FILES_DIR"); ok {
		t.Fatal("unset key not restored")
	}
}

// TestApplyOSEnvPathReachesLookPath covers issue #89: on Android the c-shared
// runtime starts with an empty environment, so envJSON's PATH is the only way
// a host-bundled ffprobe/ffmpeg can be found by the bare-name exec calls.
func TestApplyOSEnvPathReachesLookPath(t *testing.T) {
	dir := t.TempDir()
	name := "ffprobe"
	if runtime.GOOS == "windows" {
		name += ".exe" // LookPath only matches a PATHEXT extension there
	}
	bin := filepath.Join(dir, name)
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", "")
	if _, err := exec.LookPath("ffprobe"); err == nil {
		t.Fatal("ffprobe found with an empty PATH")
	}
	restore := applyOSEnv(map[string]string{"PATH": dir})
	got, err := exec.LookPath("ffprobe")
	if err != nil || got != bin {
		t.Fatalf("LookPath(ffprobe) = %q, %v; want %q", got, err, bin)
	}
	restore()
	if os.Getenv("PATH") != "" {
		t.Fatal("PATH not restored")
	}
}

func TestOpenLogFileFailureReported(t *testing.T) {
	var warn bytes.Buffer
	w, closeFn := openLogFile(filepath.Join(t.TempDir(), "no", "such", "log"), &warn)
	defer closeFn()
	if w != os.Stderr {
		t.Fatal("expected stderr fallback")
	}
	if !strings.Contains(warn.String(), "cannot open log file") {
		t.Fatalf("failure not reported: %q", warn.String())
	}
}
