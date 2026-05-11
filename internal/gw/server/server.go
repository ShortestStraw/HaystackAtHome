package server

import (
	"HaystackAtHome/internal/gw/api"
	hashring "HaystackAtHome/internal/gw/hash_ring"
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type TaskHandler func(t *Task, w http.ResponseWriter, r *http.Request)

type HaystackServer struct {
	endpoint string
	httpSrv  *http.Server
	hashRing *hashring.HashRing
}

func New(endpoint string, hashRing *hashring.HashRing) *HaystackServer {
	if endpoint == "" || hashRing == nil {
		return nil
	}
	return &HaystackServer{endpoint: endpoint, hashRing: hashRing}
}

func (srv *HaystackServer) ReqHandler(w http.ResponseWriter, r *http.Request) {
	task, err := srv.CreateTask(r)
	switch err {
	case ErrNoSuchTask:
		w.WriteHeader(http.StatusNotImplemented)
		return
	case api.ErrBadRequest:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	task.Handler(task, w, r)
}

func (srv *HaystackServer) RunServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.ReqHandler)
	srv.httpSrv = &http.Server{
		Addr:         srv.endpoint,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		slog.Info("RunServer", "starting server on", srv.endpoint)
		if err := srv.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	slog.Info("Running server", "Received signal", sig)

	slog.Info("Running server: starting graceful shutdown...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("HTTP server forced shutdown: %v", err)
	}
}

func CheckSignature(task *Task, r *http.Request) error {
	sign, err := api.SignReq(r, task.SecretKey)
	if err != nil {
		slog.Debug("Check signature", "Fail to sign req", err)
		return err
	}
	key := r.Header.Values("Signature")
	if len(key) != 1 {
		slog.Debug("Failed to get signature header")
		return api.ErrBadRequest
	}
	if sign != key[0] {
		slog.Debug("Signature mismatch for", "req", r)
		slog.Debug("Signature is", "sign", sign)
		return api.ErrSignMismatch
	}
	return nil
}

func getSecret(srv *HaystackServer, accessKey string) string {
	return "admin"
}
