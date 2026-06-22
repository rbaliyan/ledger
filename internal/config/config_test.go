package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaults(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	if cfg == nil {
		t.Fatal("Defaults returned nil")
	}
	if cfg.Listen != "localhost:50051" {
		t.Errorf("Listen = %q, want localhost:50051", cfg.Listen)
	}
	if cfg.DB.Type != "sqlite" {
		t.Errorf("DB.Type = %q, want sqlite", cfg.DB.Type)
	}
	if cfg.DB.MongoDB.Database != "ledger" {
		t.Errorf("DB.MongoDB.Database = %q, want ledger", cfg.DB.MongoDB.Database)
	}
	// configDir defaults to ~/.ledger; SQLite path resolves under it.
	if cfg.ConfigDir() == "" {
		t.Error("ConfigDir() is empty")
	}
	wantDB := filepath.Join(cfg.ConfigDir(), "ledger.db")
	if cfg.DB.SQLite.Path != wantDB {
		t.Errorf("DB.SQLite.Path = %q, want %q", cfg.DB.SQLite.Path, wantDB)
	}
	wantPID := filepath.Join(cfg.ConfigDir(), "ledger.pid")
	if cfg.PIDFile() != wantPID {
		t.Errorf("PIDFile() = %q, want %q", cfg.PIDFile(), wantPID)
	}
}

func TestLoadFrom_ValidConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
listen: "0.0.0.0:6000"
http_listen: "0.0.0.0:8080"
api_key: "secret-key"
allowed_stores:
  - orders
  - users
db:
  type: sqlite
  sqlite:
    path: /var/lib/ledger/data.db
hooks:
  - name: orders-hook
    store: orders
    url: https://example.test/hook
    secret: hook-secret
    max_retries: 3
    interval: "10s"
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadFrom(path)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.Listen != "0.0.0.0:6000" {
		t.Errorf("Listen = %q, want 0.0.0.0:6000", cfg.Listen)
	}
	if cfg.HTTPListen != "0.0.0.0:8080" {
		t.Errorf("HTTPListen = %q, want 0.0.0.0:8080", cfg.HTTPListen)
	}
	if cfg.APIKey != "secret-key" {
		t.Errorf("APIKey = %q, want secret-key", cfg.APIKey)
	}
	if len(cfg.AllowedStores) != 2 || cfg.AllowedStores[0] != "orders" {
		t.Errorf("AllowedStores = %v, want [orders users]", cfg.AllowedStores)
	}
	if cfg.DB.Type != "sqlite" {
		t.Errorf("DB.Type = %q, want sqlite", cfg.DB.Type)
	}
	if cfg.DB.SQLite.Path != "/var/lib/ledger/data.db" {
		t.Errorf("DB.SQLite.Path = %q, want /var/lib/ledger/data.db", cfg.DB.SQLite.Path)
	}
	if len(cfg.Hooks) != 1 {
		t.Fatalf("Hooks length = %d, want 1", len(cfg.Hooks))
	}
	h := cfg.Hooks[0]
	if h.Name != "orders-hook" || h.Store != "orders" || h.URL != "https://example.test/hook" {
		t.Errorf("hook = %+v, want name=orders-hook store=orders url=https://example.test/hook", h)
	}
	if h.MaxRetries != 3 || h.Interval != "10s" || h.Secret != "hook-secret" {
		t.Errorf("hook delivery fields = %+v", h)
	}
	// configDir is the directory of the loaded file.
	if cfg.ConfigDir() != dir {
		t.Errorf("ConfigDir() = %q, want %q", cfg.ConfigDir(), dir)
	}
}

func TestLoadFrom_DefaultsAppliedWhenOmitted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "minimal.yaml")
	// Only db.type set; listen and sqlite path should default.
	if err := os.WriteFile(path, []byte("db:\n  type: sqlite\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadFrom(path)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.Listen != "localhost:50051" {
		t.Errorf("Listen = %q, want default localhost:50051", cfg.Listen)
	}
	wantDB := filepath.Join(dir, "ledger.db")
	if cfg.DB.SQLite.Path != wantDB {
		t.Errorf("DB.SQLite.Path = %q, want %q", cfg.DB.SQLite.Path, wantDB)
	}
}

func TestLoadFrom_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := LoadFrom(filepath.Join(t.TempDir(), "nope.yaml"))
	if err == nil {
		t.Fatal("LoadFrom on missing file = nil error, want error")
	}
}

func TestLoadFrom_MalformedContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("listen: [unterminated\n  : :"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadFrom(path); err == nil {
		t.Fatal("LoadFrom on malformed YAML = nil error, want error")
	}
}

func TestLoadFrom_InvalidListenAddress(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad-listen.yaml")
	// "not-a-host-port" has no port, so Validate must reject it.
	content := "listen: \"not-a-host-port\"\ndb:\n  type: sqlite\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadFrom(path); err == nil {
		t.Fatal("LoadFrom with invalid listen address = nil error, want validation error")
	}
}

func TestValidate_TLSPairing(t *testing.T) {
	t.Parallel()
	base := func() *Config {
		c := &Config{Listen: "localhost:1", DB: DBConfig{Type: "sqlite"}}
		return c
	}
	t.Run("cert_without_key", func(t *testing.T) {
		t.Parallel()
		c := base()
		c.TLS.Cert = "cert.pem"
		if err := c.Validate(); err == nil {
			t.Error("Validate with cert but no key = nil, want error")
		}
	})
	t.Run("key_without_cert", func(t *testing.T) {
		t.Parallel()
		c := base()
		c.TLS.Key = "key.pem"
		if err := c.Validate(); err == nil {
			t.Error("Validate with key but no cert = nil, want error")
		}
	})
	t.Run("both", func(t *testing.T) {
		t.Parallel()
		c := base()
		c.TLS.Cert = "cert.pem"
		c.TLS.Key = "key.pem"
		if err := c.Validate(); err != nil {
			t.Errorf("Validate with cert+key = %v, want nil", err)
		}
	})
}
