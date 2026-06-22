package webhook

import (
	"testing"
	"time"
)

func TestDefaultRetryPolicy(t *testing.T) {
	p := defaultRetryPolicy()
	if p.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", p.MaxAttempts)
	}
	if p.BaseDelay != 200*time.Millisecond {
		t.Errorf("BaseDelay = %v, want 200ms", p.BaseDelay)
	}
	if p.MaxDelay != 30*time.Second {
		t.Errorf("MaxDelay = %v, want 30s", p.MaxDelay)
	}
}

func TestBackoffDelay_ExponentialSchedule(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1600 * time.Millisecond},
	}
	for _, tt := range tests {
		if got := backoffDelay(p, tt.attempt); got != tt.want {
			t.Errorf("backoffDelay(attempt=%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffDelay_CappedAtMaxDelay(t *testing.T) {
	p := RetryPolicy{BaseDelay: 1 * time.Second, MaxDelay: 4 * time.Second}
	// 1s, 2s, 4s, then capped at 4s thereafter.
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 4 * time.Second},
		{10, 4 * time.Second},
	}
	for _, tt := range tests {
		if got := backoffDelay(p, tt.attempt); got != tt.want {
			t.Errorf("backoffDelay(attempt=%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffDelay_MinimumFloor(t *testing.T) {
	// Zero BaseDelay/MaxDelay must not busy-loop: floor is minBackoffDelay.
	p := RetryPolicy{BaseDelay: 0, MaxDelay: 0}
	for attempt := 0; attempt < 5; attempt++ {
		if got := backoffDelay(p, attempt); got < minBackoffDelay {
			t.Errorf("backoffDelay(attempt=%d) = %v, want >= %v", attempt, got, minBackoffDelay)
		}
	}
}

func TestBackoffDelay_SmallBaseFloored(t *testing.T) {
	// A base smaller than the floor is raised to minBackoffDelay on early attempts.
	p := RetryPolicy{BaseDelay: 1 * time.Millisecond, MaxDelay: 1 * time.Second}
	if got := backoffDelay(p, 0); got != minBackoffDelay {
		t.Errorf("backoffDelay(0) with tiny base = %v, want %v", got, minBackoffDelay)
	}
}
