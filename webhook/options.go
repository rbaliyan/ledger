package webhook

import (
	"net/http"
	"time"
)

// RetryPolicy configures exponential back-off for failed webhook deliveries.
type RetryPolicy struct {
	MaxAttempts int           // total delivery attempts (default 5)
	BaseDelay   time.Duration // initial back-off delay (default 200ms)
	MaxDelay    time.Duration // back-off ceiling (default 30s)
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, BaseDelay: 200 * time.Millisecond, MaxDelay: 30 * time.Second}
}

// Option configures a [Sink].
type Option func(*options)

type options struct {
	headers map[string]string
	retry   RetryPolicy
	client  *http.Client
}

func defaultOptions() options {
	return options{
		headers: make(map[string]string),
		retry:   defaultRetryPolicy(),
		client:  http.DefaultClient,
	}
}

// WithHeader adds a custom HTTP header to every webhook POST.
func WithHeader(key, value string) Option {
	return func(o *options) { o.headers[key] = value }
}

// WithRetryPolicy overrides the default retry / back-off settings.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(o *options) { o.retry = p }
}

// WithHTTPClient replaces the default HTTP client.
func WithHTTPClient(c *http.Client) Option {
	return func(o *options) {
		if c != nil {
			o.client = c
		}
	}
}
