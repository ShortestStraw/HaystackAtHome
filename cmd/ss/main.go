package main

import (
	"HaystackAtHome/internal/build_version"
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
	configFile = flag.String("config", "./config.toml", "Path to config file, default ./config.toml")
	logLevel   = flag.Int("log-level", 2, "0-4: 0 -- minimum, LogErr; 4 -- maximum, Log")
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
	logger.Info("SS started", "version", build_version.Get())
}