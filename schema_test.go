package ledger

import (
	"context"
	"encoding/json"
	"testing"
)

func TestFieldMapper_Upcast(t *testing.T) {
	ctx := context.Background()

	mapper := NewFieldMapper(1, 2).
		RenameField("customer_name", "name").
		AddDefault("email", "unknown@example.com").
		RemoveField("legacy_id")

	input := `{"customer_name":"John","legacy_id":"old123","amount":99}`
	result, err := mapper.Upcast(ctx, json.RawMessage(input))
	if err != nil {
		t.Fatalf("Upcast: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	// Renamed
	if m["name"] != "John" {
		t.Errorf("name = %v, want John", m["name"])
	}
	if _, ok := m["customer_name"]; ok {
		t.Error("customer_name should be removed after rename")
	}

	// Default added
	if m["email"] != "unknown@example.com" {
		t.Errorf("email = %v, want unknown@example.com", m["email"])
	}

	// Removed
	if _, ok := m["legacy_id"]; ok {
		t.Error("legacy_id should be removed")
	}

	// Untouched
	if m["amount"] != float64(99) {
		t.Errorf("amount = %v, want 99", m["amount"])
	}
}

func TestFieldMapper_DefaultPreservesExisting(t *testing.T) {
	ctx := context.Background()

	mapper := NewFieldMapper(1, 2).AddDefault("email", "default@example.com")
	input := `{"email":"existing@example.com"}`
	result, err := mapper.Upcast(ctx, json.RawMessage(input))
	if err != nil {
		t.Fatalf("Upcast: %v", err)
	}

	var m map[string]any
	json.Unmarshal(result, &m)
	if m["email"] != "existing@example.com" {
		t.Errorf("email = %v, want existing@example.com (should not override)", m["email"])
	}
}

func TestUpcasterFunc(t *testing.T) {
	ctx := context.Background()

	u := UpcasterFunc(1, 2, func(ctx context.Context, data json.RawMessage) (json.RawMessage, error) {
		var m map[string]any
		json.Unmarshal(data, &m)
		m["version"] = 2
		b, err := json.Marshal(m)
		return json.RawMessage(b), err
	})

	if u.FromVersion() != 1 || u.ToVersion() != 2 {
		t.Errorf("versions = %d→%d, want 1→2", u.FromVersion(), u.ToVersion())
	}

	result, err := u.Upcast(ctx, json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("Upcast: %v", err)
	}

	var m map[string]any
	json.Unmarshal(result, &m)
	if m["version"] != float64(2) {
		t.Errorf("version = %v, want 2", m["version"])
	}
}

func TestUpcastChain(t *testing.T) {
	ctx := context.Background()

	upcasters := []Upcaster[json.RawMessage]{
		NewFieldMapper(1, 2).AddDefault("v2_field", "added_in_v2"),
		NewFieldMapper(2, 3).AddDefault("v3_field", "added_in_v3"),
	}

	input := `{"original":"data"}`
	result, err := upcastChain(ctx, json.RawMessage(input), 1, 3, upcasters)
	if err != nil {
		t.Fatalf("upcastChain: %v", err)
	}

	var m map[string]any
	json.Unmarshal(result, &m)

	if m["original"] != "data" {
		t.Errorf("original = %v, want data", m["original"])
	}
	if m["v2_field"] != "added_in_v2" {
		t.Errorf("v2_field = %v, want added_in_v2", m["v2_field"])
	}
	if m["v3_field"] != "added_in_v3" {
		t.Errorf("v3_field = %v, want added_in_v3", m["v3_field"])
	}
}

func TestUpcastChain_MissingUpcaster(t *testing.T) {
	ctx := context.Background()

	upcasters := []Upcaster[json.RawMessage]{
		NewFieldMapper(1, 2).AddDefault("x", "y"),
		// Missing v2→v3
	}

	_, err := upcastChain(ctx, json.RawMessage(`{}`), 1, 3, upcasters)
	if err == nil {
		t.Fatal("expected error for missing upcaster")
	}
}

func TestUpcastChain_SameVersion(t *testing.T) {
	ctx := context.Background()
	result, err := upcastChain(ctx, json.RawMessage(`{"x":1}`), 2, 2, nil)
	if err != nil {
		t.Fatalf("upcastChain same version: %v", err)
	}
	if string(result) != `{"x":1}` {
		t.Errorf("result = %s, want unchanged", result)
	}
}

func TestApplyReadOptions(t *testing.T) {
	// Defaults
	o := ApplyReadOptions()
	if o.Limit() != 100 {
		t.Errorf("default limit = %d, want 100", o.Limit())
	}
	if o.Order() != Ascending {
		t.Error("default order should be Ascending")
	}
	if o.HasAfter() {
		t.Error("default should have no cursor")
	}

	// With options
	o = ApplyReadOptions(Limit(50), Desc(), WithOrderKey("user-123"), After[int64](42))
	if o.Limit() != 50 {
		t.Errorf("limit = %d, want 50", o.Limit())
	}
	if o.Order() != Descending {
		t.Error("order should be Descending")
	}
	if o.OrderKeyFilter() != "user-123" {
		t.Errorf("orderKey = %q, want user-123", o.OrderKeyFilter())
	}
	if !o.HasAfter() {
		t.Error("should have cursor")
	}
	v, ok := AfterValue[int64](o)
	if !ok || v != 42 {
		t.Errorf("after = %d, ok=%v, want 42/true", v, ok)
	}
}

func TestAfterValue_TypeMismatch(t *testing.T) {
	o := ApplyReadOptions(After[string]("abc"))
	_, ok := AfterValue[int64](o)
	if ok {
		t.Error("AfterValue should return false for type mismatch")
	}
}

func TestLimit_InvalidValues(t *testing.T) {
	o := ApplyReadOptions(Limit(0))
	if o.Limit() != 100 {
		t.Errorf("limit(0) = %d, want 100 (default)", o.Limit())
	}

	o = ApplyReadOptions(Limit(-1))
	if o.Limit() != 100 {
		t.Errorf("limit(-1) = %d, want 100 (default)", o.Limit())
	}
}

func TestOrderString(t *testing.T) {
	if Ascending.String() != "ascending" {
		t.Errorf("Ascending.String() = %q", Ascending.String())
	}
	if Descending.String() != "descending" {
		t.Errorf("Descending.String() = %q", Descending.String())
	}
	if Order(99).String() != "ascending" {
		t.Errorf("Order(99).String() = %q, want ascending (default)", Order(99).String())
	}
}

func TestJSONCodecRoundTrip(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
		N    int    `json:"n"`
	}

	codec := JSONCodec[payload]{}

	data, err := codec.Marshal(payload{Name: "test", N: 42})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded payload
	if err := codec.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Name != "test" || decoded.N != 42 {
		t.Errorf("decoded = %+v", decoded)
	}
}

func TestJSONCodecErrors(t *testing.T) {
	codec := JSONCodec[any]{}

	// Marshal error: channels can't be marshaled
	_, err := codec.Marshal(make(chan int))
	if err == nil {
		t.Error("Marshal(chan) should error")
	}

	// Unmarshal error: invalid JSON
	var v any
	if err := codec.Unmarshal(json.RawMessage(`{invalid`), &v); err == nil {
		t.Error("Unmarshal(invalid) should error")
	}
}

func TestNewStreamPanicsOnNilStore(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewStream with nil store should panic")
		}
	}()
	NewStream[int64, json.RawMessage, any](nil, "test", JSONCodec[any]{})
}

func TestFieldMapper_NilPayload(t *testing.T) {
	mapper := NewFieldMapper(1, 2).AddDefault("x", "y")
	result, err := mapper.Upcast(context.Background(), json.RawMessage(`null`))
	if err != nil {
		t.Fatalf("Upcast null: %v", err)
	}
	// null payload returns unchanged
	if string(result) != "null" {
		t.Errorf("result = %s, want null", result)
	}
}

func TestFieldMapper_InvalidJSON(t *testing.T) {
	mapper := NewFieldMapper(1, 2)
	_, err := mapper.Upcast(context.Background(), json.RawMessage(`{invalid`))
	if err == nil {
		t.Error("Upcast invalid JSON should error")
	}
}

func TestValidateName(t *testing.T) {
	valid := []string{"ledger_entries", "my_table", "_private", "T1"}
	for _, name := range valid {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{"", "1starts_with_digit", "has spaces", "has-dash", "Robert'; DROP TABLE --", "table.name"}
	for _, name := range invalid {
		if err := ValidateName(name); err == nil {
			t.Errorf("ValidateName(%q) = nil, want error", name)
		}
	}
}
