package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"HaystackAtHome/internal/ss/models"
	"HaystackAtHome/internal/transport"

	"google.golang.org/grpc"
)

// Server implements the gRPC SS service and models.Server.
type Server struct {
	transport.UnimplementedSSServer
	svc  models.Service
	cfg  Config
	logg *slog.Logger
	gs   *grpc.Server
}

func New(cfg Config, svc models.Service, logg *slog.Logger) (*Server, error) {
	if svc == nil {
		return nil, models.NewErrInvalidParams("svc must not be nil")
	}
	s := &Server{
		svc:  svc,
		cfg:  cfg,
		logg: logg,
		gs:   grpc.NewServer(),
	}
	transport.RegisterSSServer(s.gs, s)
	return s, nil
}

// ListenAndServe binds, registers the gRPC service, and blocks until ctx is
// cancelled (which triggers GracefulStop) or the listener is closed.
func (s *Server) ListenAndServe(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.Addr, err)
	}

	go func() {
		<-ctx.Done()
		s.gs.GracefulStop()
	}()

	if s.logg != nil {
		s.logg.Info("gRPC server listening", "addr", s.cfg.Addr)
	}

	// Serve returns nil after GracefulStop; any other return is a real error.
	return s.gs.Serve(lis)
}
