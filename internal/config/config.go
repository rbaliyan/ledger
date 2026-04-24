package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

// Config holds the full ledger daemon configuration.
type Config struct {
	Listen        string       `yaml:"listen"`
	HTTPListen    string       `yaml:"http_listen"`    // optional HTTP/REST gateway address; empty = disabled
	LogFile       string       `yaml:"log_file"`
	APIKey        string       `yaml:"api_key"`
	AllowedStores []string     `yaml:"allowed_stores"` // optional allow-list of store names for the API key
	StartTimeout  string       `yaml:"start_timeout"`  // background-start readiness timeout (default: "5s")
	TLS           TLSConfig    `yaml:"tls"`
	DB            DBConfig     `yaml:"db"`
	Hooks         []HookConfig `yaml:"hooks"`

	// configDir is the directory from which this config was loaded.
	configDir string
}

// HookConfig describes a webhook event hook that delivers new entries to an
// HTTP endpoint whenever they are appended to a store or stream.
//
// Note: hook cursors are stored in memory. If the daemon restarts, each hook
// re-delivers from the beginning of every stream. Avoid stateful side-effects
// in webhook consumers, or use dedup_key to deduplicate re-deliveries.
type HookConfig struct {
	Name               string `yaml:"name"`                 // unique name for this hook (required)
	Store              string `yaml:"store"`                // store name (table/collection) to watch (required)
	Stream             string `yaml:"stream"`               // stream filter; empty = all streams in the store
	URL                string `yaml:"url"`                  // webhook POST URL (required)
	Secret             string `yaml:"secret"`               // HMAC-SHA256 secret; adds X-Ledger-Signature header
	MaxRetries         int    `yaml:"max_retries"`          // delivery attempts (default: 5)
	Interval           string `yaml:"interval"`             // polling interval (default: "5s")
	CA                 string `yaml:"ca"`                   // path to CA cert for TLS verification
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"` // skip TLS certificate verification
}

// TLSConfig holds TLS certificate paths.
type TLSConfig struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"`
}

// DBConfig selects the backend and its per-backend settings.
type DBConfig struct {
	Type       string          `yaml:"type"` // "sqlite", "postgres", "mongodb", "clickhouse"
	SQLite     SQLiteConfig    `yaml:"sqlite"`
	Postgres   PostgresConfig  `yaml:"postgres"`
	MongoDB    MongoDBConfig   `yaml:"mongodb"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
}

// ClickHouseConfig holds ClickHouse connection settings.
type ClickHouseConfig struct {
	DSN string `yaml:"dsn"`
}

// SQLiteConfig holds SQLite-specific settings.
type SQLiteConfig struct {
	Path string `yaml:"path"`
}

// PostgresConfig holds PostgreSQL connection settings.
type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

// MongoDBConfig holds MongoDB connection settings.
type MongoDBConfig struct {
	URI      string `yaml:"uri"`
	Database string `yaml:"database"`
}

// ConfigDir returns the directory from which the config was loaded,
// or the default config directory if no config file was found.
func (c *Config) ConfigDir() string { return c.configDir }

// PIDFile returns the path to the daemon PID file.
func (c *Config) PIDFile() string {
	return filepath.Join(c.configDir, "ledger.pid")
}

// Validate checks that the config values are well-formed. It enforces shape
// only — backend-specific validation (driver availability, DSN correctness) is
// performed by the server registry. Called automatically by LoadFrom.
func (c *Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("ledger: config: invalid listen address %q: %w", c.Listen, err)
	}
	if c.DB.Type == "" {
		return fmt.Errorf("ledger: config: db.type must be set (e.g. sqlite, postgres)")
	}
	if c.HTTPListen != "" {
		if _, _, err := net.SplitHostPort(c.HTTPListen); err != nil {
			return fmt.Errorf("ledger: config: invalid http_listen address %q: %w", c.HTTPListen, err)
		}
	}
	if c.TLS.Cert != "" && c.TLS.Key == "" {
		return fmt.Errorf("ledger: config: tls.cert requires tls.key")
	}
	if c.TLS.Key != "" && c.TLS.Cert == "" {
		return fmt.Errorf("ledger: config: tls.key requires tls.cert")
	}
	return nil
}

// defaults fills in zero-value fields with sensible defaults.
func (c *Config) defaults() {
	if c.Listen == "" {
		c.Listen = "localhost:50051"
	}
	if c.DB.Type == "" {
		c.DB.Type = "sqlite"
	}
	if c.DB.SQLite.Path == "" {
		c.DB.SQLite.Path = filepath.Join(c.configDir, "ledger.db")
	}
	if c.DB.MongoDB.Database == "" {
		c.DB.MongoDB.Database = "ledger"
	}
}

// defaultConfigDir returns the default directory for daemon state files
// (PID file, SQLite database, generated config.yaml).
func defaultConfigDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".ledger")
}

// Defaults returns a Config with all fields set to sensible built-in values.
// The configDir is set to the first search path (~/.ledger) so PID files and
// database paths resolve to a stable location even without a config file.
func Defaults() *Config {
	cfg := &Config{configDir: defaultConfigDir()}
	cfg.defaults()
	return cfg
}

// LoadFrom parses a config file at an explicit path.
func LoadFrom(path string) (*Config, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is an explicit user-supplied config file
	if err != nil {
		return nil, fmt.Errorf("ledger: read config %s: %w", path, err)
	}
	cfg := &Config{configDir: filepath.Dir(path)}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("ledger: parse config %s: %w", path, err)
	}
	cfg.defaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
