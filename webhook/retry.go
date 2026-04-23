package webhook

import (
	"context"
	"time"
)

// sleep pauses for d, returning early if ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// backoffDelay returns the delay for attempt n (0-indexed) given the policy.
// Delay doubles each attempt, capped at MaxDelay.
func backoffDelay(p RetryPolicy, attempt int) time.Duration {
	d := p.BaseDelay
	for i := 0; i < attempt; i++ {
		d *= 2
		if d > p.MaxDelay {
			return p.MaxDelay
		}
	}
	return d
}
