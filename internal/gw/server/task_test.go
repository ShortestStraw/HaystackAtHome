package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"HaystackAtHome/internal/gw/api"
	hashtest "HaystackAtHome/internal/gw/hash_ring"
	"HaystackAtHome/internal/transport"
)

// setupTestServer создаёт тестовый HaystackServer с моковым хэш-кольцом
func setupTestServer(t *testing.T) *HaystackServer {
	mockClient := transport.NewMockSSClient("127.0.0.1:9999")
	conMap := hashtest.ConnectionMap{
		"mock-ss": hashtest.StorageServer{
			Endpoint: mockClient.Endpoint,
			Client:   mockClient,
		},
	}
	ring := hashtest.NewMd5Ring(&conMap)
	return New("127.0.0.1:0", ring) // порт 0 для тестов
}

// createTestRequest создаёт тестовый HTTP-запрос с необходимыми заголовками
func createTestRequest(method, path, accessKey string, body []byte) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("AccessKey", accessKey)
	req.Header.Set("x-date", time.Now().Format(time.RFC3339))
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Etag", "test-etag")
		req.ContentLength = int64(len(body))
	}
	sign, _ := api.SignReq(req, "admin")
	req.Header.Add("Signature", sign)
	return req
}

func TestPutTask_Success(t *testing.T) {
	srv := setupTestServer(t)
	key := uint64(12345)
	data := []byte("test object content")

	req := createTestRequest(http.MethodPut, "/"+strconv.FormatUint(key, 10), "admin", data)
	// Подпись не проверяется в тесте, т.к. getSecret возвращает "admin"
	w := httptest.NewRecorder()

	task := &Task{
		AccessKey: "admin",
		SecretKey: "admin", // совпадает с getSecret
		Type:      PutObjectTask,
		Handler:   PutTask,
		Server:    srv,
	}

	task.Handler(task, w, req)

	if w.Code != http.StatusOK {
		t.Errorf("PutTask: expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestGetTask_Success(t *testing.T) {
	srv := setupTestServer(t)
	key := uint64(67890)
	originalData := []byte("retrieved content")

	// Сначала PUT для "записи" данных в мок
	putReq := createTestRequest(http.MethodPut, "/"+strconv.FormatUint(key, 10), "admin", originalData)
	putW := httptest.NewRecorder()
	putTask := &Task{AccessKey: "admin", SecretKey: "admin", Type: PutObjectTask, Handler: PutTask, Server: srv}
	putTask.Handler(putTask, putW, putReq)

	// Затем GET
	req := createTestRequest(http.MethodGet, "/"+strconv.FormatUint(key, 10), "admin", nil)
	w := httptest.NewRecorder()

	getTask := &Task{AccessKey: "admin", SecretKey: "admin", Type: GetObjectTask, Handler: GetTask, Server: srv}
	getTask.Handler(getTask, w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetTask: expected status %d, got %d", http.StatusOK, w.Code)
	}
	if !bytes.Equal(w.Body.Bytes(), originalData) {
		t.Errorf("GetTask: data mismatch. expected %q, got %q", originalData, w.Body.Bytes())
	}
}

func TestDeleteTask_Success(t *testing.T) {
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
	srv := setupTestServer(t)
	key := uint64(11111)
	data := []byte("to be deleted")

	// PUT
	putReq := createTestRequest(http.MethodPut, "/"+strconv.FormatUint(key, 10), "admin", data)
	putW := httptest.NewRecorder()
	putTask := &Task{AccessKey: "admin", SecretKey: "admin", Type: PutObjectTask, Handler: PutTask, Server: srv}
	putTask.Handler(putTask, putW, putReq)

	// DELETE
	delReq := createTestRequest(http.MethodDelete, "/"+strconv.FormatUint(key, 10), "admin", nil)
	delW := httptest.NewRecorder()
	delTask := &Task{AccessKey: "admin", SecretKey: "admin", Type: DeleteObjectTask, Handler: DeleteTask, Server: srv}
	delTask.Handler(delTask, delW, delReq)

	if delW.Code != http.StatusOK {
		t.Errorf("DeleteTask: expected status %d, got %d", http.StatusOK, delW.Code)
	}

	// Проверяем, что объект действительно удалён — следующий GET должен вернуть 404
	// (в моке мы эмулируем 404 через grpc NotFound)
	getReq := createTestRequest(http.MethodGet, "/"+strconv.FormatUint(key, 10), "admin", nil)
	getW := httptest.NewRecorder()
	getTask := &Task{AccessKey: "admin", SecretKey: "admin", Type: GetObjectTask, Handler: GetTask, Server: srv}
	getTask.Handler(getTask, getW, getReq)

	if getW.Code != http.StatusNotFound {
		t.Errorf("After delete, GetTask should return 404, got %d", getW.Code)
	}
}

func TestListTask_Success(t *testing.T) {
	srv := setupTestServer(t)
	keys := []uint64{100, 200, 300}

	// Записываем несколько объектов
	for _, k := range keys {
		req := createTestRequest(http.MethodPut, "/"+strconv.FormatUint(k, 10), "admin", []byte("data"))
		w := httptest.NewRecorder()
		task := &Task{AccessKey: "admin", SecretKey: "admin", Type: PutObjectTask, Handler: PutTask, Server: srv}
		task.Handler(task, w, req)
	}

	// Запрашиваем список
	req := createTestRequest(http.MethodGet, "/", "admin", nil)
	w := httptest.NewRecorder()
	listTask := &Task{AccessKey: "admin", SecretKey: "admin", Type: ListObjectTask, Handler: ListTask, Server: srv}
	listTask.Handler(listTask, w, req)

	if w.Code != http.StatusOK {
		t.Errorf("ListTask: expected status %d, got %d", http.StatusOK, w.Code)
	}

	var returnedKeys []uint64
	if err := json.Unmarshal(w.Body.Bytes(), &returnedKeys); err != nil {
		t.Fatalf("ListTask: failed to unmarshal response: %v", err)
	}

	// Проверяем, что все ключи присутствуют (порядок может отличаться)
	keySet := make(map[uint64]bool)
	for _, k := range returnedKeys {
		keySet[k] = true
	}
	for _, expected := range keys {
		if !keySet[expected] {
			t.Errorf("ListTask: expected key %d not found in response", expected)
		}
	}
}

func TestCreateTask_Valid(t *testing.T) {
	srv := setupTestServer(t)

	tests := []struct {
		name     string
		method   string
		path     string
		wantType TaskType
	}{
		{"PUT object", http.MethodPut, "/123", PutObjectTask},
		{"GET object", http.MethodGet, "/456", GetObjectTask},
		{"DELETE object", http.MethodDelete, "/789", DeleteObjectTask},
		{"LIST objects", http.MethodGet, "/", ListObjectTask},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := createTestRequest(tt.method, tt.path, "admin", nil)
			task, err := srv.CreateTask(req)
			if err != nil {
				t.Errorf("CreateTask() error = %v", err)
				return
			}
			if task.Type != tt.wantType {
				t.Errorf("CreateTask() type = %v, want %v", task.Type, tt.wantType)
			}
			if task.Handler == nil {
				t.Error("CreateTask() handler is nil")
			}
		})
	}
}

func TestCreateTask_Invalid(t *testing.T) {
	srv := setupTestServer(t)

	// POST метод не поддерживается
	req := httptest.NewRequest(http.MethodPost, "/123", nil)
	req.Header.Set("AccessKey", "admin")
	_, err := srv.CreateTask(req)
	if err != ErrNoSuchTask {
		t.Errorf("CreateTask(POST) should return ErrNoSuchTask, got %v", err)
	}

	// Отсутствует AccessKey
	req = httptest.NewRequest(http.MethodGet, "/123", nil)
	_, err = srv.CreateTask(req)
	if err == nil {
		t.Error("CreateTask without AccessKey should return error")
	}
}
