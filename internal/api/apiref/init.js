// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Starts the Scalar API reference. A separate same-origin file so the page
// needs no inline script (CSP script-src 'self').
(function () {
  'use strict'

  Scalar.createApiReference('#app', {
    url: '/openapi.json',
    // Nothing leaves the origin: no telemetry, no hosted proxy, no external
    // fonts, no AI agent or MCP promotion. The API has no authentication.
    telemetry: false,
    proxyUrl: '',
    withDefaultFonts: false,
    agent: { disabled: true },
    mcp: { disabled: true },
    showDeveloperTools: 'never',
  })
})()
