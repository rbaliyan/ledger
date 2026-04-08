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
	result, err := mapper.Upcast(ctx, []byte(input))
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
	result, err := mapper.Upcast(ctx, []byte(input))
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

	u := UpcasterFunc(1, 2, func(ctx context.Context, data []byte) ([]byte, error) {
		var m map[string]any
		json.Unmarshal(data, &m)
		m["version"] = 2
		return json.Marshal(m)
	})

	if u.FromVersion() != 1 || u.ToVersion() != 2 {
		t.Errorf("versions = %d→%d, want 1→2", u.FromVersion(), u.ToVersion())
	}

	result, err := u.Upcast(ctx, []byte(`{"x":1}`))
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

	upcasters := []Upcaster{
		NewFieldMapper(1, 2).AddDefault("v2_field", "added_in_v2"),
		NewFieldMapper(2, 3).AddDefault("v3_field", "added_in_v3"),
	}

	input := `{"original":"data"}`
	result, err := upcastChain(ctx, []byte(input), 1, 3, upcasters)
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

	upcasters := []Upcaster{
		NewFieldMapper(1, 2).AddDefault("x", "y"),
		// Missing v2→v3
	}

	_, err := upcastChain(ctx, []byte(`{}`), 1, 3, upcasters)
	if err == nil {
		t.Fatal("expected error for missing upcaster")
	}
}

func TestUpcastChain_SameVersion(t *testing.T) {
	ctx := context.Background()
	result, err := upcastChain(ctx, []byte(`{"x":1}`), 2, 2, nil)
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
