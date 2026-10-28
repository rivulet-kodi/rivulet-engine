// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import "testing"

// TestValidateSettingsPatchTrackerAndUpdateKeys: these keys are echoed back by
// GET /settings, whose stremio-core deserializer is strict about their types,
// so a wrongly typed or null value must be refused at write time.
func TestValidateSettingsPatchTrackerAndUpdateKeys(t *testing.T) {
	bad := []map[string]any{
		{"allTranscodeProfiles": nil},
		{"allTranscodeProfiles": "x"},
		{"allTranscodeProfiles": []any{1.0}},
		{"cachedTrackers": nil},
		{"cachedTrackers": []any{"udp://t:1", true}},
		{"trackersSourceUrl": nil},
		{"trackersSourceUrl": 3.0},
		{"updateChannel": nil},
		{"updateChannel": false},
		{"trackersLastUpdated": nil},
		{"trackersLastUpdated": "now"},
		{"trackersLastUpdated": -1.0},
		{"trackersLastUpdated": 1.5},
		{"updateCheckIntervalHours": nil},
		{"updateCheckIntervalHours": -2.0},
		{"updateCheckIntervalHours": "6"},
	}
	for _, p := range bad {
		if msg := validateSettingsPatch(p); msg == "" {
			t.Errorf("patch %v accepted, want rejection", p)
		}
	}

	good := []map[string]any{
		{"allTranscodeProfiles": []any{}},
		{"allTranscodeProfiles": []any{"fast", "slow"}},
		{"cachedTrackers": []any{"udp://t:1"}},
		{"trackersSourceUrl": ""},
		{"trackersSourceUrl": "https://example.test/list.txt"},
		{"updateChannel": "stable"},
		{"trackersLastUpdated": 1761000000.0},
		{"updateCheckIntervalHours": 6.0},
		{"updateCheckIntervalHours": 0.5},
	}
	for _, p := range good {
		if msg := validateSettingsPatch(p); msg != "" {
			t.Errorf("patch %v rejected: %s", p, msg)
		}
	}
}
