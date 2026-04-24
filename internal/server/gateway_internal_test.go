package server

import "testing"

func TestGatewayDialTarget(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"ipv4 wildcard", "0.0.0.0:50051", "127.0.0.1:50051"},
		{"ipv6 wildcard", "[::]:50051", "127.0.0.1:50051"},
		{"specific ipv4", "127.0.0.1:50051", "127.0.0.1:50051"},
		{"named host", "localhost:50051", "localhost:50051"},
		{"non-loopback ip", "10.0.0.1:50051", "10.0.0.1:50051"},
		{"malformed passthrough", "not-a-hostport", "not-a-hostport"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gatewayDialTarget(tt.input)
			if got != tt.want {
				t.Errorf("gatewayDialTarget(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGatewayHeaderMatcher(t *testing.T) {
	tests := []struct {
		input    string
		wantKey  string
		wantPass bool
	}{
		// x-ledger-store must pass through regardless of case.
		{"x-ledger-store", "x-ledger-store", true},
		{"X-Ledger-Store", "x-ledger-store", true},
		// x-api-key must pass through regardless of case.
		{"x-api-key", "x-api-key", true},
		{"X-Api-Key", "x-api-key", true},
		// Unrelated headers delegate to the default matcher (strips prefix, preserves case).
		{"Grpc-Metadata-Custom", "Custom", true},
		{"X-Random-Header", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			key, pass := gatewayHeaderMatcher(tt.input)
			if pass != tt.wantPass {
				t.Errorf("gatewayHeaderMatcher(%q) pass = %v, want %v", tt.input, pass, tt.wantPass)
			}
			if pass && key != tt.wantKey {
				t.Errorf("gatewayHeaderMatcher(%q) key = %q, want %q", tt.input, key, tt.wantKey)
			}
		})
	}
}
