package storage

import "HaystackAtHome/internal/ss/models"

// Config holds parameters for opening a Storage instance.
type Config struct {
	RootDir      string `toml:"root_dir"`
	Checksumming bool   `toml:"checksumming"`
	Buffering    int    `toml:"buffering"`
}

func DefaultConfig() Config {
	return Config{
		RootDir:      "",
		Checksumming: true,
		Buffering:    0,
	}
}

func (cfg *Config) Validate() error {
	if cfg.RootDir == "" {
		return models.NewErrInvalidParams("storage root_dir must be specified")
	}

	if cfg.Checksumming == false {
		return models.NewErrInvalidParams("storage without checksumming is not supported yet")
	}

	if cfg.Buffering != 0 {
		return models.NewErrInvalidParams("storage with buffering is not supported yet")
	}

	return nil
}