package server

import (
	"HaystackAtHome/internal/gw/api"
	hashring "HaystackAtHome/internal/gw/hash_ring"
	"errors"
	"log"
	"log/slog"
	"net/http"
)

/* request processing */
/*	Firstly handle request, and call parse request function,
	that will create task name from request.
	After parsing request check, if task have handler
	if so process it, else answer is method not implemented
*/
var (
	ErrNoSuchTask = errors.New("No such task")
)

type TaskHandler func(t *Task, w http.ResponseWriter, r *http.Request)

type HaystackServer struct {
	endpoint string
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

/* TODO: add more endpoints to process */
func (srv *HaystackServer) RunServer() {
	http.HandleFunc("/", srv.ReqHandler)
	log.Fatal(http.ListenAndServe(srv.endpoint, nil))
}

func CheckSignature(task *Task, r *http.Request) error {
	sign, err := api.SignReq(r, task.SecretKey)
	if err != nil {
		slog.Debug("Fail to sign req")
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
