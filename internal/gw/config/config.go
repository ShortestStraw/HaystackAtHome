package config

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/BurntSushi/toml"
)

type StorageServerConfig struct {
	Endpoint string `toml:"endpoint"`
	name     string
}

type ApiServerConfig struct {
	Endpoint string `toml:"endpoint"`
	name     string
}

type ClusterConfig struct {
	StorageServers map[string]StorageServerConfig `toml:"SS"`
	ApiServers     map[string]ApiServerConfig     `toml:"GW"`
}

func New(path string) *ClusterConfig {
	cfgData, err := os.ReadFile(path)
	if err != nil {
		slog.Error("Create new config", "Error reading config file:", err)
		return nil
	}
	cfg := new(ClusterConfig)
	// Decode the TOML data into the struct
	if _, err := toml.Decode(string(cfgData), cfg); err != nil {
		slog.Error("Create new config", "Error decoding TOML:", err)
		return nil
	}
	for k, v := range cfg.StorageServers {
		v.name = k
	}

	for k, v := range cfg.ApiServers {
		v.name = k
	}

	return cfg
}

func (c *ClusterConfig) String() string {
	s := "Cfg:\n{\n"
	for k, v := range c.StorageServers {
		s += fmt.Sprintf("storage %s:%s-%s\n", k, v.name, v.Endpoint)
	}
	for k, v := range c.ApiServers {
		s += fmt.Sprintf("api %s:%s-%s\n", k, v.name, v.Endpoint)
	}
	s += "}"
	return s
}
