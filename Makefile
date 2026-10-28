# SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
#
# SPDX-License-Identifier: MIT

# rivulet-engine

BINARY      := rivulet-engine
MAIN        := ./cmd/rivulet-engine
LIB_BINARY  := librivulet-engine
LIBMAIN     := ./cmd/librivulet-engine
DIST        := dist
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE        := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS     := -s -w -checklinkname=0 \
	-X main.buildVersion=$(VERSION) \
	-X main.buildCommit=$(COMMIT) \
	-X main.buildDate=$(DATE)
# -mod=readonly is exported so EVERY go invocation (build, test, vet, list)
# inherits it. Without it, a toolchain newer than the go.mod directive silently
# rewrites go.mod/go.sum and bumps pinned dependencies out from under you.
export GOFLAGS := -mod=readonly

# Build-only flags. Deliberately NOT part of GOFLAGS: exporting -ldflags would
# leak "-s -w" into `go test`/`go vet` binaries too.
BUILDFLAGS  := -trimpath -ldflags "$(LDFLAGS)"


# CGO is not required for most targets; disabling it makes them cross-compile
# as pure Go. The two exceptions are android/arm and android/arm64: Android
# ships no /etc/resolv.conf, so a pure-Go binary's resolver falls back to
# 127.0.0.1:53 (nothing listens there) and every DNS lookup fails -- only cgo
# linking against bionic's real resolver works. See ANDROID_ARM_CC /
# ANDROID_ARM64_CC in build-all.
export CGO_ENABLED := 0

# android/arm and android/arm64 both need an NDK clang cross-compiler (see
# above). Point ANDROID_ARM_CC / ANDROID_ARM64_CC at one each (or put the NDK
# toolchain bin dir on PATH) and build-all includes them; otherwise each
# missing one is skipped with a notice. ANDROID_LDFLAGS forces external
# linking + PIE (mirroring elgatito/elementum's Makefile:91-103) and statically
# links libstdc++ so the binary doesn't NEED libc++_shared.so -- an NDK
# redistributable apps normally bundle inside their APK that a standalone
# Kodi-addon-dropped binary has no APK to ship.
ANDROID_ARM_CC     ?= armv7a-linux-androideabi21-clang
ANDROID_ARM_CXX    ?= armv7a-linux-androideabi21-clang++
ANDROID_ARM64_CC   ?= aarch64-linux-android21-clang
ANDROID_ARM64_CXX  ?= aarch64-linux-android21-clang++
ANDROID_AMD64_CC   ?= x86_64-linux-android21-clang
ANDROID_AMD64_CXX  ?= x86_64-linux-android21-clang++
ANDROID_386_CC     ?= i686-linux-android21-clang
ANDROID_386_CXX    ?= i686-linux-android21-clang++
ANDROID_LDFLAGS    := -linkmode=external -extldflags "-pie -lm -static-libstdc++"
# librivulet-engine.so (cmd/librivulet-engine, -buildmode=c-shared) reuses the same
# NDK CC/CXX and -static-libstdc++ as the executable above, but MUST NOT pass
# -pie: cmd/link already adds -shared for c-shared, and some gcc/clang
# frontends misinterpret "-shared -pie" together and pull in the executable
# startup object (Scrt1.o, which wants a `main` symbol the .so doesn't
# export as an entry point), failing the link. -pie is an executable-only
# concern; a shared object is already position-independent code.
ANDROID_LIB_LDFLAGS := -linkmode=external -extldflags "-lm -static-libstdc++"

.PHONY: all build run test vet fmt fmt-check lint tidy clean smoke build-all lib-android dup-baseline api-fuzz help

all: fmt-check vet lint test build

build: ## Build the binary for the host platform
	go build $(BUILDFLAGS) -o $(BINARY) $(MAIN)

run: build ## Build and run
	./$(BINARY)

# The race detector requires cgo, so this is the one target that overrides the
# global CGO_ENABLED := 0 above. Builds stay pure Go.
test: export CGO_ENABLED := 1
test: ## Run tests (race detector, serial)
	go test -p 1 -race ./...

vet: ## go vet
	go vet ./...

fmt: ## Format (gofmt -s)
	gofmt -s -w .

fmt-check: ## Fail if not gofmt -s clean
	@test -z "$$(gofmt -s -l .)" || (echo "run 'make fmt'"; gofmt -s -l .; exit 1)

lint: ## golangci-lint (if installed)
	@if command -v golangci-lint >/dev/null; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; skipping"; \
	fi

tidy: ## go mod tidy
	go mod tidy

dup-baseline: ## Re-lock the dupl duplication baseline (.github/dup-baseline.json)
	./scripts/dup-check.sh --update

api-fuzz: ## Fuzz the HTTP API against internal/api/openapi.json with Schemathesis (needs docker/podman)
	./scripts/api-fuzz.sh pr

smoke: build ## Run the end-to-end API smoke test
	./scripts/smoke.sh

clean: ## Remove build artifacts
	rm -rf $(BINARY) $(DIST)

# Cross-compile every published target (pure Go, CGO disabled -- except
# android/arm and android/arm64, which need cgo + an NDK clang; see above).
# android/arm64 needs -checklinkname=0 (already in LDFLAGS) for github.com/wlynxg/anet on Go 1.23+.
build-all: ## Cross-build all release targets into dist/
	@mkdir -p $(DIST)
	GOOS=linux   GOARCH=amd64        go build $(BUILDFLAGS) -o $(DIST)/$(BINARY)_linux_amd64        $(MAIN)
	GOOS=linux   GOARCH=arm64        go build $(BUILDFLAGS) -o $(DIST)/$(BINARY)_linux_arm64        $(MAIN)
	GOOS=linux   GOARCH=arm GOARM=7  go build $(BUILDFLAGS) -o $(DIST)/$(BINARY)_linux_armv7        $(MAIN)
	GOOS=darwin  GOARCH=amd64        go build $(BUILDFLAGS) -o $(DIST)/$(BINARY)_darwin_amd64       $(MAIN)
	GOOS=darwin  GOARCH=arm64        go build $(BUILDFLAGS) -o $(DIST)/$(BINARY)_darwin_arm64       $(MAIN)
	GOOS=windows GOARCH=amd64        go build $(BUILDFLAGS) -o $(DIST)/$(BINARY)_windows_amd64.exe  $(MAIN)
	GOOS=windows GOARCH=arm64        go build $(BUILDFLAGS) -o $(DIST)/$(BINARY)_windows_arm64.exe  $(MAIN)
	@if command -v $(ANDROID_ARM64_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_ARM64_CC) CXX=$(ANDROID_ARM64_CXX) \
		GOOS=android GOARCH=arm64 \
		go build -trimpath -ldflags '$(LDFLAGS) $(ANDROID_LDFLAGS)' -o $(DIST)/$(BINARY)_android_arm64 $(MAIN); \
	else \
		echo "skipping android/arm64: $(ANDROID_ARM64_CC) not found (set ANDROID_ARM64_CC to an NDK clang)"; \
	fi
	@if command -v $(ANDROID_ARM_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_ARM_CC) CXX=$(ANDROID_ARM_CXX) \
		GOOS=android GOARCH=arm GOARM=7 \
		go build -trimpath -ldflags '$(LDFLAGS) $(ANDROID_LDFLAGS)' -o $(DIST)/$(BINARY)_android_armv7 $(MAIN); \
	else \
		echo "skipping android/armv7: $(ANDROID_ARM_CC) not found (set ANDROID_ARM_CC to an NDK clang)"; \
	fi
	@if command -v $(ANDROID_AMD64_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_AMD64_CC) CXX=$(ANDROID_AMD64_CXX) \
		GOOS=android GOARCH=amd64 \
		go build -trimpath -ldflags '$(LDFLAGS) $(ANDROID_LDFLAGS)' -o $(DIST)/$(BINARY)_android_amd64 $(MAIN); \
	else \
		echo "skipping android/amd64: $(ANDROID_AMD64_CC) not found (set ANDROID_AMD64_CC to an NDK clang)"; \
	fi
	@if command -v $(ANDROID_386_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_386_CC) CXX=$(ANDROID_386_CXX) \
		GOOS=android GOARCH=386 \
		go build -trimpath -ldflags '$(LDFLAGS) $(ANDROID_LDFLAGS)' -o $(DIST)/$(BINARY)_android_386 $(MAIN); \
	else \
		echo "skipping android/386: $(ANDROID_386_CC) not found (set ANDROID_386_CC to an NDK clang)"; \
	fi
	@$(MAKE) lib-android

# librivulet-engine.so: the same server, in c-shared mode, for SELinux-
# enforcing Android hosts where a Kodi addon's shell can dlopen() a .so
# dropped next to its own native libs but is blocked from exec()'ing a
# bundled executable. Shipped inside the same Android release archives as
# the rivulet-engine_android_{arm64,armv7} executable (see .goreleaser.yml).
lib-android: ## Cross-build librivulet-engine.so (c-shared) for android/{arm64,armv7,amd64,386} into dist/
	@mkdir -p $(DIST)
	@if command -v $(ANDROID_ARM64_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_ARM64_CC) CXX=$(ANDROID_ARM64_CXX) \
		GOOS=android GOARCH=arm64 \
		go build -trimpath -buildmode=c-shared -ldflags '-s -w -checklinkname=0 $(ANDROID_LIB_LDFLAGS)' \
			-o $(DIST)/$(LIB_BINARY)_android_arm64.so $(LIBMAIN); \
	else \
		echo "skipping librivulet-engine android/arm64: $(ANDROID_ARM64_CC) not found (set ANDROID_ARM64_CC to an NDK clang)"; \
	fi
	@if command -v $(ANDROID_ARM_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_ARM_CC) CXX=$(ANDROID_ARM_CXX) \
		GOOS=android GOARCH=arm GOARM=7 \
		go build -trimpath -buildmode=c-shared -ldflags '-s -w -checklinkname=0 $(ANDROID_LIB_LDFLAGS)' \
			-o $(DIST)/$(LIB_BINARY)_android_armv7.so $(LIBMAIN); \
	else \
		echo "skipping librivulet-engine android/armv7: $(ANDROID_ARM_CC) not found (set ANDROID_ARM_CC to an NDK clang)"; \
	fi
	@if command -v $(ANDROID_AMD64_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_AMD64_CC) CXX=$(ANDROID_AMD64_CXX) \
		GOOS=android GOARCH=amd64 \
		go build -trimpath -buildmode=c-shared -ldflags '-s -w -checklinkname=0 $(ANDROID_LIB_LDFLAGS)' \
			-o $(DIST)/$(LIB_BINARY)_android_amd64.so $(LIBMAIN); \
	else \
		echo "skipping librivulet-engine android/amd64: $(ANDROID_AMD64_CC) not found (set ANDROID_AMD64_CC to an NDK clang)"; \
	fi
	@if command -v $(ANDROID_386_CC) >/dev/null 2>&1; then \
		CGO_ENABLED=1 CC=$(ANDROID_386_CC) CXX=$(ANDROID_386_CXX) \
		GOOS=android GOARCH=386 \
		go build -trimpath -buildmode=c-shared -ldflags '-s -w -checklinkname=0 $(ANDROID_LIB_LDFLAGS)' \
			-o $(DIST)/$(LIB_BINARY)_android_386.so $(LIBMAIN); \
	else \
		echo "skipping librivulet-engine android/386: $(ANDROID_386_CC) not found (set ANDROID_386_CC to an NDK clang)"; \
	fi
	@echo "built -> $(DIST)/"

help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
