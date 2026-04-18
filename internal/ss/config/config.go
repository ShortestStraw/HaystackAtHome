// Package config defines the top-level SS configuration.
// Fields absent from the file keep their Default() values.
package config

import (
	"log/slog"

	"HaystackAtHome/internal/ss/server"
	"HaystackAtHome/internal/ss/service"
	"HaystackAtHome/internal/ss/storage"
)

type Config struct {
	Server  server.Config  `toml:"server"`
	Storage storage.Config `toml:"storage"`
	Service service.Config `toml:"service"`
	// LogLevel controls logging verbosity: 0=Error, 1=Warn, 2=Info, 3=Debug.
	LogLevel int    `toml:"log_level"`
	LogPath  string `toml:"log_path"`
}

func Default() Config {
	return Config{
		Server:   server.DefaultConfig(),
		Storage:  storage.DefaultConfig(),
		Service:  service.DefaultConfig(),
		LogLevel: 2, // Info
		LogPath:  "",
	}
}

func (cfg Config) SlogLevel() slog.Level {
	switch cfg.LogLevel {
	case 0:
		return slog.LevelError
	case 1:
		return slog.LevelWarn
	case 3:
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

func (cfg *Config) Validate() error {
	if err := cfg.Service.Validate(); err != nil {
		return err
	}

	if err := cfg.Storage.Validate(); err != nil {
		return err
	}

	if err := cfg.Server.Validate(); err != nil {
		return err
	}

	return nil
}
