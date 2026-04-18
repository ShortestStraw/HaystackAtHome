package service

import (
	"HaystackAtHome/internal/ss/models"
)

// Config holds parameters for the SS gRPC service.
type Config struct {
	VolNum        int    `toml:"vol_num"`
	VolMaxSize    uint64 `toml:"vol_max_size"`
	MemLimit      uint32 `toml:"mem_limit"`
}

func DefaultConfig() Config {
	return Config{
		VolNum:       20,
		VolMaxSize:   107374182400, // 100 GiB
	}
}

func (cfg *Config) Validate() error {
	if cfg.MemLimit < 1024 {
		return models.NewErrInvalidParams("service mem_limit must be grater thnn 1024")
	}

	if cfg.VolNum <= 0 {
		return models.NewErrInvalidParams("service vol_num must be greater than 0")
	}

	if cfg.VolMaxSize < 1024 * 1024 {
		return models.NewErrInvalidParams("service vol_max_size must be more than 1 Mib")
	}

	return nil
}