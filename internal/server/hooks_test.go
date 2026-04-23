package server

import (
	"testing"
	"time"

	"github.com/rbaliyan/ledger/internal/config"
)

func TestNewHookRunnerValidation(t *testing.T) {
	cases := []struct {
		name    string
		cfg     config.HookConfig
		wantErr bool
	}{
		{
			name:    "missing name",
			cfg:     config.HookConfig{Store: "s", URL: "http://x"},
			wantErr: true,
		},
		{
			name:    "missing store",
			cfg:     config.HookConfig{Name: "h", URL: "http://x"},
			wantErr: true,
		},
		{
			name:    "missing url",
			cfg:     config.HookConfig{Name: "h", Store: "s"},
			wantErr: true,
		},
		{
			name:    "invalid interval",
			cfg:     config.HookConfig{Name: "h", Store: "s", URL: "http://x", Interval: "bad"},
			wantErr: true,
		},
		{
			name:    "valid minimal",
			cfg:     config.HookConfig{Name: "h", Store: "s", URL: "http://x"},
			wantErr: false,
		},
		{
			name:    "valid with interval",
			cfg:     config.HookConfig{Name: "h", Store: "s", URL: "http://x", Interval: "10s"},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newHookRunner(tc.cfg, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("newHookRunner error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestHookRunnerInterval(t *testing.T) {
	h, err := newHookRunner(config.HookConfig{Name: "h", Store: "s", URL: "http://x", Interval: "2s"}, nil)
	if err != nil {
		t.Fatalf("newHookRunner: %v", err)
	}
	if h.interval != 2*time.Second {
		t.Errorf("interval=%v, want 2s", h.interval)
	}
}

func TestHookRunnerDefaultInterval(t *testing.T) {
	h, err := newHookRunner(config.HookConfig{Name: "h", Store: "s", URL: "http://x"}, nil)
	if err != nil {
		t.Fatalf("newHookRunner: %v", err)
	}
	if h.interval != 5*time.Second {
		t.Errorf("interval=%v, want 5s", h.interval)
	}
}

func TestHookRunnerCursorPrune(t *testing.T) {
	h, err := newHookRunner(config.HookConfig{Name: "h", Store: "s", URL: "http://x"}, nil)
	if err != nil {
		t.Fatalf("newHookRunner: %v", err)
	}

	// Pre-populate cursors for three streams.
	h.mu.Lock()
	h.cursors["stream-a"] = "id-1"
	h.cursors["stream-b"] = "id-2"
	h.cursors["stream-gone"] = "id-3"
	h.mu.Unlock()

	// Simulate post-poll pruning: only stream-a and stream-b are active.
	active := map[string]struct{}{"stream-a": {}, "stream-b": {}}
	h.mu.Lock()
	for s := range h.cursors {
		if _, ok := active[s]; !ok {
			delete(h.cursors, s)
		}
	}
	h.mu.Unlock()

	h.mu.Lock()
	_, hasGone := h.cursors["stream-gone"]
	_, hasA := h.cursors["stream-a"]
	h.mu.Unlock()

	if hasGone {
		t.Error("stale cursor for stream-gone should have been pruned")
	}
	if !hasA {
		t.Error("cursor for stream-a should be retained")
	}
}
