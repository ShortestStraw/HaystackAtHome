package app

import (
	conf "HaystackAtHome/internal/ss/config"
	"HaystackAtHome/internal/ss/models"
	"HaystackAtHome/internal/ss/server"
	"HaystackAtHome/internal/ss/service"
	"HaystackAtHome/internal/ss/storage"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

type App struct {
	logger   *slog.Logger
	stor     models.Storage
	service  models.Service
	server   models.Server
}

func FromConfig(ctx context.Context, logger *slog.Logger, cfg *conf.Config) (*App, error) {
	if logger == nil {
		return nil, models.NewErrInvalidParams("app must be started with configured logger")	
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %v", err)	
	}

	opts := make([]storage.Option, 0, 4)
	if cfg.Storage.Checksumming {
		opts = append(opts, storage.WithObjectChecksumming())
		logger.Info("Using storage checksumming")
	}

	if cfg.Storage.Buffering > 0 {
		opts = append(opts, storage.WithVolumeWriteBuffering(uint64(cfg.Storage.Buffering)))
		logger.Info("Using storage write buggering", "bufSize", cfg.Storage.Buffering)
	}

	opts = append(opts, storage.WithLogger(logger.With("storage", cfg.Storage.RootDir)))

	storageMetrics := storage.NewDefaultStorageMetrics()
	if cfg.Server.PromAddr != "" {
		opts = append(opts, storage.WithMetrics(storageMetrics))
		logger.Info("Using storage metrics reporting")
	}

	stor, err := storage.Open(ctx, cfg.Storage.RootDir, opts...)
	if err != nil {
		logger.Error("Failed to open storage", "err", err)
		return nil, fmt.Errorf("failed to open storage: %v", err)
	}

	if cfg.Service.VolNum > 0 {
		existing, err := stor.ListVolumes(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list volumes: %v", err)
		}
		have := make(map[uint64]struct{}, len(existing))
		for _, v := range existing {
			have[v.Key] = struct{}{}
		}
		for i := 0; i < cfg.Service.VolNum; i++ {
			if _, ok := have[uint64(i)]; ok {
				continue
			}
			if _, err := stor.AddVolume(ctx, strconv.Itoa(i), cfg.Service.VolMaxSize); err != nil {
				return nil, fmt.Errorf("failed to create volume %d: %v", i, err)
			}
			logger.Info("Created volume", "id", i, "maxSize", cfg.Service.VolMaxSize)
		}
	}

	svcOpts := make([]service.Option, 0, 4)

	if cfg.Service.Uid != 0 {
		svcOpts = append(svcOpts, service.WithUID(cfg.Service.Uid))
	}

	svcOpts = append(svcOpts, service.WithLogger(logger.With("svc", "run")))
	svcOpts = append(svcOpts, service.WithServiceFeatures(
		models.ServiceFeatures{Checksum: cfg.Storage.Checksumming},
	))
	svcOpts = append(svcOpts, service.WithMemoryLimit(cfg.Service.MemLimit))
	svcOpts = append(svcOpts, service.WithAllocatorRR())

	svc, err := service.New(ctx, stor, svcOpts...)
	if err != nil {
		logger.Error("failed to create service", "err", err)
		return nil, fmt.Errorf("failed to create service: %v", err)
	}

	if err := svc.InitObjTable(ctx); err != nil {
		logger.Error("failed to initialise object table", "err", err)
		return nil, fmt.Errorf("failed to init object table: %v", err)
	}

	srv, err := server.New(cfg.Server, svc, logger.With("server", cfg.Server.Addr))
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %v", err)
	}

	return &App{
		logger:  logger,
		stor:    stor,
		service: svc,
		server:  srv,
	}, nil
}

func (app *App) Run(ctx context.Context) error {
	exit_ctx, cancel := context.WithTimeout(context.Background(), time.Second * 30)
	defer cancel()
	err := app.server.ListenAndServe(ctx)
	if err != nil {
		app.logger.Error("failed shutdown server", "err", err)
	}

	if err1 := app.service.Stop(exit_ctx); err1 != nil {
		app.logger.Error("failed shutdown service", "err", err1)
	}

	if	err2 := app.stor.Close(exit_ctx); err2 != nil {
		app.logger.Error("failed shutdown storage", "err", err2)
	}

	return err
}