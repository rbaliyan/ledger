package ledger

import (
	"context"
	"encoding/json"
	"fmt"
)

// Upcaster transforms payload data from one schema version to the next.
// P is the store-native payload type (e.g. json.RawMessage for SQL backends,
// bson.Raw for MongoDB).
//
// Register upcasters in sequence (v1→v2, v2→v3, etc.) to allow chained upgrades.
// When reading entries written with an older schema version, the stream applies
// upcasters in order to transform the payload before decoding into T.
type Upcaster[P any] interface {
	// FromVersion returns the source version this upcaster handles.
	FromVersion() int

	// ToVersion returns the target version this upcaster produces.
	ToVersion() int

	// Upcast transforms the payload from source to target version.
	Upcast(ctx context.Context, payload P) (P, error)
}

// UpcasterFunc creates an Upcaster[P] from a plain function.
func UpcasterFunc[P any](from, to int, fn func(ctx context.Context, payload P) (P, error)) Upcaster[P] {
	return &funcUpcaster[P]{from: from, to: to, fn: fn}
}

type funcUpcaster[P any] struct {
	from, to int
	fn       func(ctx context.Context, payload P) (P, error)
}

func (u *funcUpcaster[P]) FromVersion() int { return u.from }
func (u *funcUpcaster[P]) ToVersion() int   { return u.to }
func (u *funcUpcaster[P]) Upcast(ctx context.Context, payload P) (P, error) {
	return u.fn(ctx, payload)
}

// FieldMapper transforms JSON payloads between schema versions using field operations.
// It implements [Upcaster][json.RawMessage] and is intended for use with SQL backends
// (sqlite, postgres). MongoDB users should write BSON-aware upcasters instead.
//
// Supports three operations applied in order: renames, then defaults, then removals.
//
//	upcaster := ledger.NewFieldMapper(1, 2).
//	    RenameField("customer_name", "customerName").
//	    AddDefault("email", "unknown@example.com").
//	    RemoveField("legacy_id")
type FieldMapper struct {
	from         int
	to           int
	fieldMap     map[string]string // old name → new name
	defaults     map[string]any    // new field → default value
	removeFields []string          // fields to remove
}

// NewFieldMapper creates a new field mapper for the specified version transition.
func NewFieldMapper(from, to int) *FieldMapper {
	return &FieldMapper{
		from:     from,
		to:       to,
		fieldMap: make(map[string]string),
		defaults: make(map[string]any),
	}
}

// FromVersion returns the source version this mapper handles.
func (f *FieldMapper) FromVersion() int { return f.from }

// ToVersion returns the target version this mapper produces.
func (f *FieldMapper) ToVersion() int { return f.to }

// RenameField adds a field rename transformation.
func (f *FieldMapper) RenameField(oldName, newName string) *FieldMapper {
	f.fieldMap[oldName] = newName
	return f
}

// AddDefault sets a default value for a field that may not exist.
func (f *FieldMapper) AddDefault(field string, value any) *FieldMapper {
	f.defaults[field] = value
	return f
}

// RemoveField marks a field for removal during upcasting.
func (f *FieldMapper) RemoveField(field string) *FieldMapper {
	f.removeFields = append(f.removeFields, field)
	return f
}

// Upcast transforms the JSON payload from source to target version.
// Applies operations in order: renames, defaults, removals.
func (f *FieldMapper) Upcast(ctx context.Context, data json.RawMessage) (json.RawMessage, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	if m == nil {
		return data, nil
	}

	for oldName, newName := range f.fieldMap {
		if value, ok := m[oldName]; ok {
			m[newName] = value
			delete(m, oldName)
		}
	}

	for field, value := range f.defaults {
		if _, ok := m[field]; !ok {
			m[field] = value
		}
	}

	for _, field := range f.removeFields {
		delete(m, field)
	}

	return json.Marshal(m)
}

var _ Upcaster[json.RawMessage] = (*FieldMapper)(nil)
var _ Upcaster[json.RawMessage] = (*funcUpcaster[json.RawMessage])(nil)

// upcastChain applies upcasters in sequence from fromVersion to targetVersion.
func upcastChain[P any](ctx context.Context, payload P, fromVersion, targetVersion int, upcasters []Upcaster[P]) (P, error) {
	if fromVersion >= targetVersion {
		return payload, nil
	}

	result := payload
	for v := fromVersion; v < targetVersion; v++ {
		u := findUpcaster(upcasters, v, v+1)
		if u == nil {
			var zero P
			return zero, fmt.Errorf("%w: version %d to %d", ErrNoUpcaster, v, v+1)
		}
		var err error
		result, err = u.Upcast(ctx, result)
		if err != nil {
			var zero P
			return zero, fmt.Errorf("upcast v%d to v%d: %w", v, v+1, err)
		}
	}
	return result, nil
}

func findUpcaster[P any](upcasters []Upcaster[P], from, to int) Upcaster[P] {
	for _, u := range upcasters {
		if u.FromVersion() == from && u.ToVersion() == to {
			return u
		}
	}
	return nil
}
