package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

// BenchmarkFieldMapperUpcast measures a single v(n)->v(n+1) hop: JSON decode
// into a map, apply rename/default/remove ops, re-encode. This runs once per
// version gap for every old-version entry read.
func BenchmarkFieldMapperUpcast(b *testing.B) {
	ctx := context.Background()
	mapper := NewFieldMapper(1, 2).
		RenameField("customer_name", "customerName").
		AddDefault("email", "unknown@example.com").
		RemoveField("legacy_id")
	payload := json.RawMessage(`{"customer_name":"Acme Corp","legacy_id":42,"amount":1234.56,"currency":"USD"}`)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := mapper.Upcast(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpcastChain measures the full chain walk over varying version gaps.
// Each hop renames one field, so a v1->v5 read pays four decode/encode cycles.
func BenchmarkUpcastChain(b *testing.B) {
	ctx := context.Background()
	payload := json.RawMessage(`{"f1":"value","amount":1234.56,"currency":"USD","status":"ok"}`)

	// Build a chain of single-hop field-rename upcasters v1->v2->...->v6.
	const maxVersion = 6
	upcasters := make([]Upcaster[json.RawMessage], 0, maxVersion-1)
	for v := 1; v < maxVersion; v++ {
		upcasters = append(upcasters, NewFieldMapper(v, v+1).
			RenameField(fmt.Sprintf("f%d", v), fmt.Sprintf("f%d", v+1)))
	}

	cases := []struct {
		name   string
		target int
	}{
		{"v1->v2", 2},
		{"v1->v3", 3},
		{"v1->v5", 5},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := upcastChain(ctx, payload, 1, c.target, upcasters); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
