package server

import (
	"HaystackAtHome/internal/ss/models"
	"fmt"
	"net/netip"
)

// Config holds parameters for the gRPC and prometheus servers.
type Config struct {
	Addr          string `toml:"address"` // ip:port
	PromAddr      string `toml:"prometheus"` // ip:port
}

func DefaultConfig() Config {
	return Config{
		Addr: "",
		PromAddr: "",
	}
}

func (cfg *Config) Validate() error {
	if cfg.Addr == "" {
		return models.NewErrInvalidParams("service address must be specified")
	}

	if _, err := netip.ParseAddrPort(cfg.Addr); err != nil {
		return models.NewErrInvalidParams(fmt.Sprintf("service address parsing error: %s", err.Error()))
	}

	if cfg.PromAddr == "" {
		return nil
	}

	if _, err := netip.ParseAddrPort(cfg.PromAddr); err != nil {
		return models.NewErrInvalidParams(fmt.Sprintf("service prometheus address parsing error: %s", err.Error()))
	}

	return nil
}