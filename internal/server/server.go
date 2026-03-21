package server

import (
	"errors"
	"fmt"
	"HaystackAtHome/internal/api"
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

	hsHandlers = map[string]TaskHandler{
		"PUT":    PutTask,
		"DELETE": DeleteTask,
		"GET":    GetTask,
	}
)

type TaskHandler func(t *Task, w http.ResponseWriter, r *http.Request)

type HaystackServer struct {
	endpoint string
}

/* http.ListenAndServe(eng.endpoint, nil) */

func (srv *HaystackServer) CreateTask(r *http.Request) (*Task, error) {
	/*	Naive implementation expects only 3 type of tasks:
		put key, get key, delete key (simple crud for test)
	*/
	taskName := fmt.Sprintf("%s", r.Method)
	slog.Debug("Build task handler", "task", taskName)
	handler, ok := hsHandlers[taskName]
	if !ok {
		return nil, ErrNoSuchTask
	}
	key := r.Header.Values("AccessKey")
	if len(key) != 1 {
		return nil, api.ErrBadRequest
	}

	task := Task{
		Handler:   handler,
		AccessKey: key[0],
		TaskType:  taskName,
		Server:    srv,
	}
	return &task, nil
}

/* TODO: add more params */
func New(endpoint string) *HaystackServer {
	if endpoint == "" {
		return nil
	}
	return &HaystackServer{endpoint: endpoint}
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

func PutTask(task *Task, w http.ResponseWriter, r *http.Request) {
	task.SecretKey = getSecret(task.Server, task.AccessKey)
	err := CheckSignature(task, r)
	switch err {
	case api.ErrBadRequest:
		w.WriteHeader(http.StatusBadRequest)
		return
	case api.ErrSignMismatch:
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	TestTask(task.Server, w, r)
}

func GetTask(task *Task, w http.ResponseWriter, r *http.Request) {
	task.SecretKey = getSecret(task.Server, task.AccessKey)
	err := CheckSignature(task, r)
	switch err {
	case api.ErrBadRequest:
		w.WriteHeader(http.StatusBadRequest)
		return
	case api.ErrSignMismatch:
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	TestTask(task.Server, w, r)
}

func DeleteTask(task *Task, w http.ResponseWriter, r *http.Request) {
	task.SecretKey = getSecret(task.Server, task.AccessKey)
	err := CheckSignature(task, r)
	switch err {
	case api.ErrBadRequest:
		w.WriteHeader(http.StatusBadRequest)
		return
	case api.ErrSignMismatch:
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	TestTask(task.Server, w, r)
}

func getSecret(srv *HaystackServer, accessKey string) string {
	return "admin"
}

func TestTask(srv *HaystackServer, w http.ResponseWriter, r *http.Request) {
	fmt.Printf("req is %v srv %p writer %p\n", r, srv, w)
	w.WriteHeader(http.StatusOK)
}
