package main

import (
	"HaystackAtHome/internal/build_version"
	"HaystackAtHome/internal/gw/config"
	hashring "HaystackAtHome/internal/gw/hash_ring"
	"HaystackAtHome/internal/gw/server"
	"flag"
	"fmt"
	"log/slog"
	"os"
)

const (
	LogErr = iota
	LogWarn
	LogInfo
	LogDebug
)

var (
	name       = flag.String("name", "", "Name of the server to search in config")
	isSecure   = flag.Bool("secure", false, "Connection uses TLS if true, else plain TCP")
	configFile = flag.String("config", "./config.toml", "Path to config file, default ./config.toml")
	logLevel   = flag.Int("log-level", 2, "0-3: 0 -- minimum, LogErr; 3 -- maximum, Log")
)

func main() {
	flag.Parse()
	var ll slog.Level
	switch *logLevel {
	case LogErr:
		ll = slog.LevelError
	case LogWarn:
		ll = slog.LevelWarn
	case LogInfo:
		ll = slog.LevelInfo
	case LogDebug:
		ll = slog.LevelDebug
	default:
		fmt.Printf("Unknown log level")
		return
	}
	opts := &slog.HandlerOptions{Level: ll}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
	slog.Info("GW started", "version", build_version.Get())
	cfg := config.New(*configFile)
	connMap, err := hashring.ConnMapFromConfig(cfg)
	if err != nil {
		slog.Error("Starting", "failed to create connection map", err)
		return
	}
	ring := hashring.NewMd5Ring(connMap)
	if ring == nil {
		slog.Error("Starting failed to create hashring")
		return
	}
	slog.Debug("Starting", "Config", cfg.String())
	serv, ok := cfg.ApiServers[*name]
	if !ok {
		slog.Error("Starting", "No such service in configuration", name)
		return
	}
	/* Dump service start params */
	srv := server.New(serv.Endpoint, ring)
	srv.RunServer()
}
