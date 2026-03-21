package config

import (
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/BurntSushi/toml"
)

type HashFunction func(path string) int

type StorageServerConfig struct {
	Endpoint string `toml:"endpoint"`
	name     string
}

type ApiServerConfig struct {
	Endpoint string `toml:"endpoint"`
	name     string
}

type DummyConfig struct {
	StorageServers map[string]StorageServerConfig `toml:"servers"`
	ApiServers     map[string]ApiServerConfig     `toml:"api"`
}

type ClusterConfig struct {
	storageServers map[string]StorageServerConfig
	apiServers     map[string]ApiServerConfig
}

func New(path string) *ClusterConfig {
	cfgData, err := os.ReadFile(path)
	if err != nil {
		slog.Error("Create new config", "Error reading config file:", err)
		return nil
	}
	cfg := new(ClusterConfig)
	tmp := new(DummyConfig)
	// Decode the TOML data into the struct
	if _, err := toml.Decode(string(cfgData), tmp); err != nil {
		slog.Error("Create new config", "Error decoding TOML:", err)
		return nil
	}
	cfg.apiServers = make(map[string]ApiServerConfig, len(tmp.ApiServers))
	cfg.storageServers = make(map[string]StorageServerConfig, len(tmp.StorageServers))
	for k, v := range tmp.StorageServers {
		v.name = k
		cfg.storageServers[k] = v
	}

	for k, v := range tmp.ApiServers {
		v.name = k
		cfg.apiServers[k] = v
	}

	return cfg
}

func (c *ClusterConfig) String() string {
	s := "Cfg:\n{\n"
	for k, v := range c.storageServers {
		s += fmt.Sprintf("storage %s:%s-%s\n", k, v.name, v.Endpoint)
	}
	for k, v := range c.apiServers {
		s += fmt.Sprintf("api %s:%s-%s\n", k, v.name, v.Endpoint)
	}
	s += "}"
	return s
}

func (c *ClusterConfig) GetStorageByName(name string) (string, bool) {
	s, ok := c.storageServers[name]
	if !ok {
		return "", false
	}
	return s.Endpoint, true
}

func (c *ClusterConfig) GetApiByName(name string) (string, bool) {
	s, ok := c.apiServers[name]
	if !ok {
		return "", false
	}
	return s.Endpoint, true
}

type HashRing struct {
	servers  map[int]StorageServerConfig
	keys     []int
	hashFunc HashFunction
}

func (r *HashRing) String() string {
	s := "Hash Ring:\n{\n"
	for k, v := range r.servers {
		s += fmt.Sprintf("servers map %d:%s-%s\n", k, v.name, v.Endpoint)
	}
	for _, v := range r.keys {
		s += fmt.Sprintf("keys %d\n", v)
	}
	s += "}"
	return s
}

func newGenericHashRing(cfg *ClusterConfig, hashFunc HashFunction) *HashRing {
	if cfg == nil || hashFunc == nil {
		return nil
	}
	serversNum := len(cfg.storageServers)
	ring := &HashRing{
		servers:  make(map[int]StorageServerConfig, serversNum),
		keys:     make([]int, serversNum),
		hashFunc: hashFunc,
	}
	i := 0
	for k, v := range cfg.storageServers {
		h := hashFunc(k)
		ring.keys[i] = h
		ring.servers[h] = v
		i++
	}
	sort.Ints(ring.keys)
	return ring
}

func NewMd5Ring(cfg *ClusterConfig) *HashRing {
	return newGenericHashRing(cfg, md5Hash)
}
