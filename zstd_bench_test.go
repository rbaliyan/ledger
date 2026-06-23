package ledger

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// benchRecord is a realistic domain payload: a mix of scalar fields and a
// variable-length items slice that drives the small/medium/large sizing.
type benchRecord struct {
	ID       string            `json:"id"`
	Customer string            `json:"customer"`
	Amount   float64           `json:"amount"`
	Currency string            `json:"currency"`
	Status   string            `json:"status"`
	Tags     []string          `json:"tags"`
	Metadata map[string]string `json:"metadata"`
	Items    []benchItem       `json:"items"`
}

type benchItem struct {
	SKU   string  `json:"sku"`
	Name  string  `json:"name"`
	Qty   int     `json:"qty"`
	Price float64 `json:"price"`
}

// makeRecord builds a record with n line items. Real-world JSON tends to
// compress well because of repeated field names and value patterns, which is
// exactly what the zstd path exploits.
func makeRecord(n int) benchRecord {
	items := make([]benchItem, n)
	for i := range items {
		items[i] = benchItem{
			SKU:   fmt.Sprintf("SKU-%06d", i),
			Name:  "Widget " + strings.Repeat("x", 8),
			Qty:   i + 1,
			Price: 19.99,
		}
	}
	return benchRecord{
		ID:       "order-123456",
		Customer: "customer-7890",
		Amount:   1234.56,
		Currency: "USD",
		Status:   "confirmed",
		Tags:     []string{"priority", "verified", "retail"},
		Metadata: map[string]string{"region": "us-east", "channel": "web"},
		Items:    items,
	}
}

// payloadSizes maps a label to a line-item count producing roughly
// small/medium/large JSON payloads.
var payloadSizes = []struct {
	name string
	n    int
}{
	{"small", 1},
	{"medium", 20},
	{"large", 200},
}

func BenchmarkCodecMarshal(b *testing.B) {
	for _, sz := range payloadSizes {
		rec := makeRecord(sz.n)
		raw, err := json.Marshal(rec)
		if err != nil {
			b.Fatal(err)
		}
		nbytes := int64(len(raw))

		b.Run("json/"+sz.name, func(b *testing.B) {
			codec := JSONCodec[benchRecord]{}
			b.SetBytes(nbytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := codec.Marshal(rec); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("zstd/"+sz.name, func(b *testing.B) {
			codec, err := NewZstdCodec[benchRecord]()
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(codec.Close)
			b.SetBytes(nbytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := codec.Marshal(rec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCodecUnmarshal(b *testing.B) {
	for _, sz := range payloadSizes {
		rec := makeRecord(sz.n)
		jsonRaw, err := json.Marshal(rec)
		if err != nil {
			b.Fatal(err)
		}
		nbytes := int64(len(jsonRaw))

		zcodec, err := NewZstdCodec[benchRecord]()
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(zcodec.Close)
		// Pre-encode payloads covering the three zstd decode branches:
		//   - zstd-prefixed (current format)
		//   - explicit plain marker (0x00 prefix)
		//   - legacy (no version byte, raw JSON)
		zstdPayload, err := zcodec.Marshal(rec)
		if err != nil {
			b.Fatal(err)
		}
		plainPayload := json.RawMessage(append([]byte{plainVersionByte}, jsonRaw...))
		legacyPayload := json.RawMessage(jsonRaw)

		b.Run("json/"+sz.name, func(b *testing.B) {
			codec := JSONCodec[benchRecord]{}
			b.SetBytes(nbytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var out benchRecord
				if err := codec.Unmarshal(legacyPayload, &out); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("zstd/"+sz.name, func(b *testing.B) {
			b.SetBytes(nbytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var out benchRecord
				if err := zcodec.Unmarshal(zstdPayload, &out); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("zstd_plain/"+sz.name, func(b *testing.B) {
			b.SetBytes(nbytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var out benchRecord
				if err := zcodec.Unmarshal(plainPayload, &out); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("zstd_legacy/"+sz.name, func(b *testing.B) {
			b.SetBytes(nbytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var out benchRecord
				if err := zcodec.Unmarshal(legacyPayload, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
