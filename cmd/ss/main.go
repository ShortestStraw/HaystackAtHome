package main

import (
	"HaystackAtHome/internal/build_version"
	app "HaystackAtHome/internal/ss"
	"HaystackAtHome/internal/ss/config"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
)

const (
	LogErr = iota
	LogWarn
	LogInfo
	LogDebug
)

var (
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
		fmt.Printf("Unknown log level\n")
		os.Exit(1)
	}

	opts := &slog.HandlerOptions{Level: ll}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	logger.Info("SS started", "version", build_version.Get())

	var cfg config.Config
	if _, err := toml.DecodeFile(*configFile, &cfg); err != nil {
		logger.Error("failed to decode config file", "path", *configFile, "err", err)
		os.Exit(1)
	}

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Create and run the App
	srv, err := app.FromConfig(ctx, logger, &cfg)
	if err != nil {
		logger.Error("failed to create app from config", "err", err)
		os.Exit(1)
	}

	logger.Info("starting StorageService", "addr", cfg.Server.Addr, "storage", cfg.Storage.RootDir)

	if err := srv.Run(ctx); err != nil {
		logger.Error("app run failed", "err", err)
		os.Exit(1)
	}

	logger.Info("SS shutdown complete")
}
