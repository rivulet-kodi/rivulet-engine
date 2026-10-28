// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — GET /get-https?authKey=..&ipAddress=..
//
// Provisions a TLS certificate from api.strem.io (see certprovision.go for the
// shared logic), installs https-cert.pem / https-key.pem under cfg.AppPath, and
// returns {ipAddress, domain, port}. The authKey is cached so the background
// renewer in cmd/rivulet-engine can refresh the cert without user interaction.
//
// NOTE: This endpoint requires a valid Stremio authKey issued by api.strem.io
// after a real Stremio account login. Without it, the API returns no certificate
// and no files are written.
package api

import (
	"errors"
	"net/http"
)

// handleGetHTTPS handles GET /get-https?authKey=..&ipAddress=..
func (s *server) handleGetHTTPS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ipAddress := q.Get("ipAddress")
	// Prefer the request header to avoid the auth key being captured in upstream
	// access logs (which typically record the full request URI including query
	// params). The authKey query param is retained for backwards compatibility.
	authKey := r.Header.Get("X-Stremio-AuthKey")
	if authKey == "" {
		authKey = q.Get("authKey")
	}

	if ipAddress == "" {
		http.Error(w, "missing ipAddress query parameter", http.StatusBadRequest)
		return
	}
	if authKey == "" {
		http.Error(w, "missing authKey (provide via X-Stremio-AuthKey header or authKey query param)", http.StatusBadRequest)
		return
	}

	res, err := ProvisionCert(s.cfg.AppPath, authKey, ipAddress)
	if err != nil {
		status := http.StatusBadGateway
		var pe *provisionError
		if errors.As(err, &pe) {
			status = pe.status
		}
		http.Error(w, err.Error(), status)
		return
	}

	// Remember the authKey so the background renewer can refresh the cert.
	CacheAuthKey(s.cfg.AppPath, authKey)

	// Hot-swap the live HTTPS listener so the freshly installed cert takes effect
	// immediately, with no restart. No-op until cmd wires the reload hook.
	if p := s.certReload.Load(); p != nil {
		(*p)()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ipAddress": ipAddress,
		"domain":    res.Domain,
		"port":      s.cfg.HTTPSPort,
	})
}

// SetCertReloadHook registers a callback invoked after /get-https installs a new
// certificate, so the running HTTPS listener can hot-swap it without a restart.
func (s *server) SetCertReloadHook(fn func()) { s.certReload.Store(&fn) }
