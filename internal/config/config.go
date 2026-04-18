package config

import (
	"errors"
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

// Validate checks that the config values are well-formed.
// It is called automatically by Load and LoadFrom.
func (c *Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("ledger: config: invalid listen address %q: %w", c.Listen, err)
	}
	switch c.DB.Type {
	case "sqlite", "postgres", "mongodb":
	default:
		return fmt.Errorf("ledger: config: unknown db type %q (want sqlite|postgres|mongodb)", c.DB.Type)
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

// searchPaths returns candidate directories in order of preference.
func searchPaths() []string {
	home, _ := os.UserHomeDir()
	return []string{
		filepath.Join(home, ".ledger"),
		filepath.Join(home, ".config", "ledger"),
	}
}

// defaultConfigDir returns the first search path (used when no config is found).
func defaultConfigDir() string {
	paths := searchPaths()
	if len(paths) > 0 {
		return paths[0]
	}
	return "."
}

// Load discovers and parses the config file. If no file is found, sensible
// defaults are returned. The config file must be named "config.yaml".
func Load() (*Config, error) {
	for _, dir := range searchPaths() {
		path := filepath.Join(dir, "config.yaml")
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("ledger: read config %s: %w", path, err)
		}
		cfg := &Config{configDir: dir}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("ledger: parse config %s: %w", path, err)
		}
		cfg.defaults()
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return cfg, nil
	}
	// No config file found — use all defaults.
	cfg := &Config{configDir: defaultConfigDir()}
	cfg.defaults()
	return cfg, nil
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
