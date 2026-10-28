// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package types

import "errors"

// ErrTooManyEngines is returned by EnsureEngine when the active-engine cap is
// reached and no idle engine can be evicted to make room.
var ErrTooManyEngines = errors.New("too many active engines")

// ErrInvalidInfoHash is returned by EnsureEngine for an info hash the torrent
// client cannot track: the all-zero hash (anacrolix/torrent panics on it, which
// used to leave the manager lock held and wedge every later request).
var ErrInvalidInfoHash = errors.New("invalid info hash")
