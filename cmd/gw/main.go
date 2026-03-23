package main

import (
	"flag"
	"fmt"
	"HaystackAtHome/internal/config"
	"HaystackAtHome/internal/server"
	"HaystackAtHome/internal/build_version"
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
	logLevel   = flag.Int("log-level", 2, "")
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
	slog.Info("Service started", "version", build_version.Get())
	cfg := config.New(*configFile)
	ring := config.NewMd5Ring(cfg)
	slog.Debug("Starting", "Config", cfg.String())
	slog.Debug("Starting", "HashRing", ring.String())
	endpoint, ok := cfg.GetApiByName(*name)
	if !ok {
		slog.Error("Starting", "No such service in configuration", name)
		return
	}
	/* Dump service start params */
	srv := server.New(endpoint)
	srv.RunServer()
}
