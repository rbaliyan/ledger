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
	Listen  string    `yaml:"listen"`
	LogFile string    `yaml:"log_file"`
	APIKey  string    `yaml:"api_key"`
	TLS     TLSConfig `yaml:"tls"`
	DB      DBConfig  `yaml:"db"`

	// configDir is the directory from which this config was loaded.
	configDir string
}

// TLSConfig holds TLS certificate paths.
type TLSConfig struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"`
}

// DBConfig selects the backend and its per-backend settings.
type DBConfig struct {
	Type     string        `yaml:"type"` // "sqlite", "postgres", "mongodb"
	SQLite   SQLiteConfig  `yaml:"sqlite"`
	Postgres PostgresConfig `yaml:"postgres"`
	MongoDB  MongoDBConfig  `yaml:"mongodb"`
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
	data, err := os.ReadFile(path)
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
