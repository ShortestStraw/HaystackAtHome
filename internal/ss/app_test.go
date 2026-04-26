package app_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	app "HaystackAtHome/internal/ss"
	conf "HaystackAtHome/internal/ss/config"
	serverconf "HaystackAtHome/internal/ss/server"
	serviceconf "HaystackAtHome/internal/ss/service"
	storageconf "HaystackAtHome/internal/ss/storage"
	"HaystackAtHome/internal/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// freePort picks a free TCP port by binding to :0 and immediately releasing it.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func makeConfig(t *testing.T, storRoot string, addr string) *conf.Config { //nolint:unparam
	t.Helper()
	cfg := conf.Default()
	cfg.Storage = storageconf.Config{
		RootDir:      storRoot,
		Checksumming: true,
		Buffering:    0,
	}
	cfg.Service = serviceconf.Config{
		VolNum:     2,
		VolMaxSize: 100 * 1024 * 1024, // 100 MiB per volume — enough for tests
		MemLimit:   1024 * 1024,        // 1 MiB
		Uid:        42,
	}
	cfg.Server = serverconf.Config{
		Addr:     addr,
		PromAddr: "",
	}
	cfg.LogLevel = 0 // Error — keep test output clean
	return &cfg
}

// putObject sends metadata + data via PutObj streaming RPC and returns on success.
func putObject(t *testing.T, client transport.SSClient, key, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.PutObj(ctx)
	if err != nil {
		t.Fatalf("PutObj stream: %v", err)
	}

	objKey := uint64(key[0])<<8 | uint64(key[1])
	if err := stream.Send(&transport.PutObjReq{
		Data: &transport.PutObjReq_Meta{Meta: &transport.PutObjMeta{
			Key:  objKey,
			Size: uint64(len(data)),
		}},
	}); err != nil {
		t.Fatalf("send meta: %v", err)
	}

	if err := stream.Send(&transport.PutObjReq{
		Data: &transport.PutObjReq_Chunk{Chunk: data},
	}); err != nil {
		t.Fatalf("send chunk: %v", err)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
}

// putObjectKey is a convenience wrapper that takes a plain uint64 key.
func putObjectKey(t *testing.T, client transport.SSClient, key uint64, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.PutObj(ctx)
	if err != nil {
		t.Fatalf("PutObj stream: %v", err)
	}
	if err := stream.Send(&transport.PutObjReq{
		Data: &transport.PutObjReq_Meta{Meta: &transport.PutObjMeta{
			Key:  key,
			Size: uint64(len(data)),
		}},
	}); err != nil {
		t.Fatalf("send meta: %v", err)
	}
	if err := stream.Send(&transport.PutObjReq{
		Data: &transport.PutObjReq_Chunk{Chunk: data},
	}); err != nil {
		t.Fatalf("send chunk: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
}

// getObject retrieves an object and returns its data.
func getObject(t *testing.T, client transport.SSClient, key uint64) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.GetObj(ctx, &transport.GetObjReq{Key: key})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch d := msg.Data.(type) {
		case *transport.GetObjResp_Size:
			// size hint — ignore for now
			_ = d.Size
		case *transport.GetObjResp_Chunk:
			buf.Write(d.Chunk)
		}
	}
	return buf.Bytes(), nil
}

// listKeys drains a GetObjsMap stream and returns all keys.
func listKeys(t *testing.T, client transport.SSClient) []uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.GetObjsMap(ctx, &transport.GetObjsMapReq{})
	if err != nil {
		t.Fatalf("GetObjsMap: %v", err)
	}
	var keys []uint64
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("GetObjsMap recv: %v", err)
		}
		for _, obj := range resp.Obj {
			keys = append(keys, obj.Key)
		}
	}
	return keys
}

func containsKey(keys []uint64, k uint64) bool {
	for _, v := range keys {
		if v == k {
			return true
		}
	}
	return false
}

func TestAppLifecycle(t *testing.T) {
	storRoot := t.TempDir()
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := makeConfig(t, storRoot, addr)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	a, err := app.FromConfig(appCtx, logger, cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	appErrCh := make(chan error, 1)
	go func() { appErrCh <- a.Run(appCtx) }()

	// Wait for the gRPC server to be ready by probing with GetServiceInfo.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	probe := transport.NewSSClient(conn)
	var ready bool
	for i := 0; i < 50; i++ {
		pCtx, pCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, pErr := probe.GetServiceInfo(pCtx, &transport.GetServiceInfoReq{})
		pCancel()
		if pErr == nil {
			ready = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		t.Fatal("gRPC server did not become ready in time")
	}

	client := transport.NewSSClient(conn)

	const (
		key1 uint64 = 1001
		key2 uint64 = 1002
	)
	data1 := []byte("hello haystack")
	data2 := []byte("second object payload")

	// 1. Put object 1.
	putObjectKey(t, client, key1, data1)

	// 2. List — must contain key1.
	keys := listKeys(t, client)
	if !containsKey(keys, key1) {
		t.Fatalf("after first put: key1 not in list %v", keys)
	}

	// 3. Put object 2 and read it back.
	putObjectKey(t, client, key2, data2)

	got, err := getObject(t, client, key2)
	if err != nil {
		t.Fatalf("GetObj key2: %v", err)
	}
	if !bytes.Equal(got, data2) {
		t.Fatalf("GetObj key2: got %q want %q", got, data2)
	}

	// 4. List — must contain both keys.
	keys = listKeys(t, client)
	if !containsKey(keys, key1) || !containsKey(keys, key2) {
		t.Fatalf("after second put: keys %v missing key1 or key2", keys)
	}

	// 5. Delete key1.
	delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer delCancel()
	if _, err := client.DelObj(delCtx, &transport.DelObjReq{Key: key1}); err != nil {
		t.Fatalf("DelObj key1: %v", err)
	}

	// 6. List — key1 must be gone, key2 still present.
	keys = listKeys(t, client)
	if containsKey(keys, key1) {
		t.Fatalf("after delete: key1 still in list %v", keys)
	}
	if !containsKey(keys, key2) {
		t.Fatalf("after delete: key2 missing from list %v", keys)
	}

	// 7. Reading deleted key1 must return NotFound.
	_, err = getObject(t, client, key1)
	if err == nil {
		t.Fatal("expected error reading deleted key1, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Fatalf("reading deleted key1: want NotFound, got %v", err)
	}

	// 8. Graceful shutdown: close the client connection first so GracefulStop
	// does not wait for the idle client-side transport to drain.
	_ = conn.Close()
	cancelApp()
	select {
	case err := <-appErrCh:
		if err != nil {
			t.Fatalf("app.Run returned error on shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("app did not shut down within 10s\n\nGoroutines:\n%s", buf[:n])
	}
}
