package server

import (
	"HaystackAtHome/internal/gw/api"
	"HaystackAtHome/internal/transport"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TaskType uint8

const (
	PutObjectTask   TaskType = 0
	GetObjectTask   TaskType = 1
	DeletObjectTask TaskType = 2
	UnknownTask     TaskType = 255

	ResponseTout = 30 * time.Second
)

var (
	hsHandlers = map[TaskType]TaskHandler{
		PutObjectTask:   PutTask,
		DeletObjectTask: DeleteTask,
		GetObjectTask:   GetTask,
	}
)

type Task struct {
	AccessKey string
	SecretKey string
	Type      TaskType
	Handler   TaskHandler
	Server    *HaystackServer
}

/* Naive implementation of parsing task name from uri, change */
func GetTaskType(r *http.Request) TaskType {

	switch r.Method {
	case http.MethodPut:
		return PutObjectTask
	case http.MethodGet:
		return GetObjectTask
	case http.MethodDelete:
		return DeletObjectTask
	}
	return UnknownTask
}

func (self *TaskType) String() string {
	switch *self {
	case PutObjectTask:
		return "PutObjectTask"
	case GetObjectTask:
		return "GetObjectTask"
	case DeletObjectTask:
		return "DeleteObjectTask"
	}
	return "UnknownTask"
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("PutTask", "Read body error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	defer r.Body.Close()

	path := parseObjectPath(r.RequestURI)
	key := task.Server.hashRing.GetKey(path)
	client := task.Server.hashRing.ChooseServer(path)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.PutObj(ctx)
	if err != nil {
		slog.Error("PutTask", "Put object stream error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := stream.Send(&transport.PutObjReq{
		Data: &transport.PutObjReq_Meta{Meta: &transport.PutObjMeta{
			Key:  uint64(key),
			Size: uint64(len(body)),
		}},
	}); err != nil {
		slog.Error("PutTask", "Send initial msg error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := stream.Send(&transport.PutObjReq{
		Data: &transport.PutObjReq_Chunk{Chunk: body},
	}); err != nil {
		slog.Error("PutTask", "Send data error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		slog.Error("PutTask", "Close stream error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
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

	path := parseObjectPath(r.RequestURI)
	key := task.Server.hashRing.GetKey(path)
	client := task.Server.hashRing.ChooseServer(path)
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTout)
	defer cancel()

	stream, err := client.GetObj(ctx, &transport.GetObjReq{Key: uint64(key)})
	if err != nil {
		slog.Error("GetTask", "Get object stream error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var buf bytes.Buffer
	var size uint64
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				switch st.Code() {
				case codes.NotFound:
					slog.Error("GetTask", "Get object stream error", "Object not found")
					w.WriteHeader(http.StatusNotFound)
					return
				case codes.Unavailable:
					slog.Error("GetTask", "Get object stream error", "Unavailable")
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				default:
					slog.Error("GetTask", "Get object stream error", st.Message(), "error code", st.Code())
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
			slog.Error("GetTask", "Get object stream error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		switch d := msg.Data.(type) {
		case *transport.GetObjResp_Size:
			size = d.Size
		case *transport.GetObjResp_Chunk:
			buf.Write(d.Chunk)
		}
	}
	if size != uint64(len(buf.Bytes())) {
		slog.Error("GetTask", "Data length mismatch on", path, "key", key)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(buf.Bytes())
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

func TestTask(srv *HaystackServer, w http.ResponseWriter, r *http.Request) {
	fmt.Printf("req is %v srv %p writer %p\n", r, srv, w)
	fmt.Printf("choose storage server %p\n", srv.hashRing.ChooseServer(r.Method))
	w.WriteHeader(http.StatusOK)
}

func (srv *HaystackServer) CreateTask(r *http.Request) (*Task, error) {
	/*	Naive implementation expects only 3 type of tasks:
		put key, get key, delete key (simple crud for test)
	*/
	taskType := GetTaskType(r)
	slog.Debug("Create Task", "task type", taskType.String())
	handler, ok := hsHandlers[taskType]
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
		Type:      taskType,
		Server:    srv,
	}
	return &task, nil
}

func parseObjectPath(uri string) string {
	return strings.TrimPrefix(uri, "/")
}
