package server

import (
	"HaystackAtHome/internal/gw/api"
	"HaystackAtHome/internal/transport"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TaskType uint8

const (
	PutObjectTask    TaskType = 0
	GetObjectTask    TaskType = 1
	DeleteObjectTask TaskType = 2
	ListObjectTask   TaskType = 3
	UnknownTask      TaskType = 255

	ResponseTout = 30 * time.Second
	MaxSize      = 5 * 1024 * 1024
)

var (
	hsHandlers = map[TaskType]TaskHandler{
		PutObjectTask:    PutTask,
		DeleteObjectTask: DeleteTask,
		GetObjectTask:    GetTask,
		ListObjectTask:   ListTask,
	}

	ErrNoSuchTask = errors.New("No such task")
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
	path := parseObjectPath(r.URL.Path)
	if path != "" {
		_, err := strconv.ParseUint(path, 10, 64)
		if err != nil {
			slog.Error("GetTaskType", "Error while parsing object path", err)
			return UnknownTask
		}
	} else {
		if r.Method != http.MethodGet {
			return UnknownTask
		}
		return ListObjectTask
	}

	switch r.Method {
	case http.MethodPut:
		return PutObjectTask
	case http.MethodGet:
		return GetObjectTask
	case http.MethodDelete:
		return DeleteObjectTask
	}
	return UnknownTask
}

func (self *TaskType) String() string {
	switch *self {
	case PutObjectTask:
		return "PutObjectTask"
	case GetObjectTask:
		return "GetObjectTask"
	case DeleteObjectTask:
		return "DeleteObjectTask"
	case ListObjectTask:
		return "ListObjectTask"
	}
	return "UnknownTask"
}

func PutTask(task *Task, w http.ResponseWriter, r *http.Request) {
	if r.ContentLength <= 0 {
		http.Error(w, "Content-Length header is missing or must be greater than 0", http.StatusBadRequest)
		return
	}

	if r.ContentLength > MaxSize {
		http.Error(w, "Object is too large", http.StatusRequestEntityTooLarge)
		return
	}

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
	if int64(len(body)) != r.ContentLength {
		slog.Error("PutTask", "Read body error", "Data size mismatch")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	path := parseObjectPath(r.RequestURI)
	key, _ := strconv.ParseUint(path, 10, 64)
	client := task.Server.hashRing.ChooseServer(int(key))
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTout)
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
	key, _ := strconv.ParseUint(path, 10, 64)
	client := task.Server.hashRing.ChooseServer(int(key))
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

	ctx, cancel := context.WithTimeout(context.Background(), ResponseTout)
	defer cancel()

	path := parseObjectPath(r.RequestURI)
	key, _ := strconv.ParseUint(path, 10, 64)
	client := task.Server.hashRing.ChooseServer(int(key))
	if _, err := client.DelObj(ctx, &transport.DelObjReq{Key: uint64(key)}); err != nil {
		st, ok := status.FromError(err)
		if ok {
			switch st.Code() {
			case codes.NotFound:
				slog.Info("DeleteTask", "object not found", key)
				w.WriteHeader(http.StatusNotFound)
				return
			default:
				slog.Error("DeleteTask", "error message", st.Message(), "error code", st.Code())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		slog.Error("DeleteTask", "Delete object error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
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

func ListTask(task *Task, w http.ResponseWriter, r *http.Request) {
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

	ctx, cancel := context.WithTimeout(context.Background(), ResponseTout)
	defer cancel()

	idx := 0
	var keys []uint64
	for client := task.Server.hashRing.GetServer(idx); client != nil; client = task.Server.hashRing.GetServer(idx) {
		stream, err := client.GetObjsMap(ctx, &transport.GetObjsMapReq{})
		if err != nil {
			slog.Error("List task", "service unavailable", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				slog.Error("List task", "service unavailable", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			for _, obj := range resp.Obj {
				keys = append(keys, obj.Key)
			}
		}
		idx++
	}
	jsonData, err := json.Marshal(keys)
	if err != nil {
		slog.Error("List task", "error while converting data", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
}
