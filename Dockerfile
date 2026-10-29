# SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
#
# SPDX-License-Identifier: MIT

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
# The builder runs natively on the build host ($BUILDPLATFORM) and cross-compiles
# to the requested target — pure-Go (CGO disabled), so no QEMU emulation needed.
ARG GO_IMAGE=docker.io/library/golang:1.27.3-alpine3.24@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb
ARG ALPINE_IMAGE=docker.io/library/alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=container
ARG COMMIT=none
ARG DATE=

# TARGETOS/TARGETARCH are auto-populated by BuildKit/Buildah per --platform target.
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-amd64}" go build \
    -trimpath \
    -ldflags "-s -w -checklinkname=0 \
              -X main.buildVersion=${VERSION} \
              -X main.buildCommit=${COMMIT} \
              -X main.buildDate=${DATE}" \
    -o /out/rivulet-engine \
    ./cmd/rivulet-engine

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM ${ALPINE_IMAGE}

LABEL org.opencontainers.image.title="rivulet-engine" \
      org.opencontainers.image.description="IPv6-capable drop-in streaming server for Stremio clients" \
      org.opencontainers.image.source="https://github.com/rivulet-kodi/rivulet-engine" \
      org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache ffmpeg yt-dlp ca-certificates tzdata && \
    adduser -D -u 1000 user

COPY --from=build /out/rivulet-engine /usr/local/bin/rivulet-engine

ENV HTTP_PORT=11470 \
    HTTPS_PORT=0 \
    BT_LISTEN_PORT=0 \
    APP_PATH=/data \
    WEB_UI_LOCATION=https://web.stremio.com/ \
    HOME=/home/user

RUN mkdir -p /data && chown -R user:user /data /home/user

USER user
WORKDIR /home/user

VOLUME ["/data"]

EXPOSE 11470

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD wget -q -O - "http://127.0.0.1:${HTTP_PORT}/heartbeat" >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/usr/local/bin/rivulet-engine"]
