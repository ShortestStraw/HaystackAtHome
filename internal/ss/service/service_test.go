package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"HaystackAtHome/internal/ss/models"
)

const (
	testVolKey  = uint64(0)
	testVolSize = uint64(1 << 30) // 1 GiB
)

// newTestSvcN creates a ready-to-use Service backed by n volumes keyed 0..n-1.
func newTestSvcN(t *testing.T, n int) (*Service, *mockStorage) {
	t.Helper()
	stor := newMockStorage()
	for i := 0; i < n; i++ {
		stor.addVol(uint64(i), testVolSize)
	}
	svc, err := New(
		context.Background(), stor,
		WithServiceFeatures(models.ServiceFeatures{Checksum: false}),
		WithUID(42),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.InitObjTable(context.Background()); err != nil {
		t.Fatalf("InitObjTable: %v", err)
	}
	return svc, stor
}

// newTestSvc creates a ready-to-use Service backed by a single in-memory volume.
func newTestSvc(t *testing.T) (*Service, *mockStorage) {
	t.Helper()
	stor := newMockStorage()
	stor.addVol(testVolKey, testVolSize)

	svc, err := New(
		context.Background(), stor,
		WithServiceFeatures(models.ServiceFeatures{Checksum: false}),
		WithUID(42),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.InitObjTable(context.Background()); err != nil {
		t.Fatalf("InitObjTable: %v", err)
	}
	return svc, stor
}

// putAndClose writes data as a complete object and returns on Close success.
func putAndClose(t *testing.T, svc *Service, key uint64, data []byte) {
	t.Helper()
	w, err := svc.PutObj(context.Background(), key, uint64(len(data)))
	if err != nil {
		t.Fatalf("PutObj key=%d: %v", key, err)
	}
	if _, err := w.Write(data); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Write key=%d: %v", key, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close key=%d: %v", key, err)
	}
}

// readAll reads and returns all bytes from GetObj.
func readAll(t *testing.T, svc *Service, key uint64) []byte {
	t.Helper()
	rc, _, err := svc.GetObj(context.Background(), key)
	if err != nil {
		t.Fatalf("GetObj key=%d: %v", key, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll key=%d: %v", key, err)
	}
	return got
}

// ── basic CRUD ────────────────────────────────────────────────────────────────

func TestPutGetRoundtrip(t *testing.T) {
	svc, _ := newTestSvc(t)
	data := []byte("hello world")
	putAndClose(t, svc, 1, data)

	rc, sz, err := svc.GetObj(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetObj: %v", err)
	}
	defer rc.Close()

	if sz != uint64(len(data)) {
		t.Errorf("size: want %d got %d", len(data), sz)
	}
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("data: want %q got %q", data, got)
	}
}

func TestDelObj(t *testing.T) {
	svc, _ := newTestSvc(t)
	putAndClose(t, svc, 1, []byte("x"))

	if err := svc.DelObj(context.Background(), 1); err != nil {
		t.Fatalf("DelObj: %v", err)
	}

	_, _, err := svc.GetObj(context.Background(), 1)
	if !errors.Is(err, &models.ErrNotFound{}) {
		t.Fatalf("after delete GetObj: want ErrNotFound, got %v", err)
	}
}

func TestGetObjsMap(t *testing.T) {
	svc, _ := newTestSvc(t)
	putAndClose(t, svc, 1, []byte("aaa"))
	putAndClose(t, svc, 2, []byte("bbbbbb"))

	metas, err := svc.GetObjsMap(context.Background())
	if err != nil {
		t.Fatalf("GetObjsMap: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("len: want 2, got %d", len(metas))
	}

	byKey := make(map[uint64]models.ObjMeta, len(metas))
	for _, m := range metas {
		byKey[m.Key] = m
	}
	if byKey[1].Size != 3 {
		t.Errorf("key 1 size: want 3, got %d", byKey[1].Size)
	}
	if byKey[2].Size != 6 {
		t.Errorf("key 2 size: want 6, got %d", byKey[2].Size)
	}
}

func TestGetServiceInfo(t *testing.T) {
	svc, _ := newTestSvc(t)

	info, err := svc.GetServiceInfo(context.Background())
	if err != nil {
		t.Fatalf("GetServiceInfo: %v", err)
	}
	if info.UID != 42 {
		t.Errorf("UID: want 42, got %d", info.UID)
	}
	if info.Space.Total == 0 {
		t.Error("Space.Total should be non-zero")
	}
}

// ── error cases ───────────────────────────────────────────────────────────────

func TestPutDuplicateReturnsErrExists(t *testing.T) {
	svc, _ := newTestSvc(t)
	putAndClose(t, svc, 1, []byte("x"))

	_, err := svc.PutObj(context.Background(), 1, 1)
	if !errors.Is(err, &models.ErrExists{}) {
		t.Fatalf("want ErrExists, got %v", err)
	}
}

func TestPutAfterDeleteSucceeds(t *testing.T) {
	svc, _ := newTestSvc(t)
	putAndClose(t, svc, 1, []byte("original"))

	if err := svc.DelObj(context.Background(), 1); err != nil {
		t.Fatalf("DelObj: %v", err)
	}

	putAndClose(t, svc, 1, []byte("replaced"))

	got := readAll(t, svc, 1)
	if string(got) != "replaced" {
		t.Errorf("want 'replaced', got %q", got)
	}
}

func TestGetNotFoundReturnsErrNotFound(t *testing.T) {
	svc, _ := newTestSvc(t)

	_, _, err := svc.GetObj(context.Background(), 999)
	if !errors.Is(err, &models.ErrNotFound{}) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestGetDeletedReturnsErrNotFound(t *testing.T) {
	svc, _ := newTestSvc(t)
	putAndClose(t, svc, 1, []byte("x"))
	svc.DelObj(context.Background(), 1)

	_, _, err := svc.GetObj(context.Background(), 1)
	if !errors.Is(err, &models.ErrNotFound{}) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestDelNotFoundReturnsErrNotFound(t *testing.T) {
	svc, _ := newTestSvc(t)

	err := svc.DelObj(context.Background(), 999)
	if !errors.Is(err, &models.ErrNotFound{}) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestPutBeforeInitTableReturnsErrInvalidParams(t *testing.T) {
	stor := newMockStorage()
	stor.addVol(testVolKey, testVolSize)

	svc, err := New(
		context.Background(), stor,
		WithServiceFeatures(models.ServiceFeatures{}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// InitObjTable intentionally NOT called.

	_, err = svc.PutObj(context.Background(), 1, 1)
	if !errors.Is(err, &models.ErrInvalidParams{}) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}
}

// ── InitObjTable ──────────────────────────────────────────────────────────────

func TestInitObjTableRestoresIndex(t *testing.T) {
	stor := newMockStorage()
	stor.addVol(testVolKey, testVolSize)
	stor.addObj(testVolKey, models.ObjInfo{Key: 10, DataSize: 5, Offset: 0}, []byte("alpha"))
	stor.addObj(testVolKey, models.ObjInfo{Key: 20, DataSize: 5, Offset: 5}, []byte("bravo"))

	svc, err := New(
		context.Background(), stor,
		WithServiceFeatures(models.ServiceFeatures{}),
		WithUID(7),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.InitObjTable(context.Background()); err != nil {
		t.Fatalf("InitObjTable: %v", err)
	}

	if got := readAll(t, svc, 10); string(got) != "alpha" {
		t.Errorf("key 10: want 'alpha', got %q", got)
	}
	if got := readAll(t, svc, 20); string(got) != "bravo" {
		t.Errorf("key 20: want 'bravo', got %q", got)
	}
}

// ── partial write ─────────────────────────────────────────────────────────────

func TestPartialWriteNotIndexed(t *testing.T) {
	svc, _ := newTestSvc(t)

	w, err := svc.PutObj(context.Background(), 1, 10)
	if err != nil {
		t.Fatalf("PutObj: %v", err)
	}
	w.Write([]byte("hello")) // 5 of 10 bytes
	w.Close()

	_, _, err = svc.GetObj(context.Background(), 1)
	if !errors.Is(err, &models.ErrNotFound{}) {
		t.Fatalf("want ErrNotFound for partial write, got %v", err)
	}
}

func TestPartialWriteKeyIsReusable(t *testing.T) {
	svc, _ := newTestSvc(t)

	// partial write
	w, _ := svc.PutObj(context.Background(), 1, 10)
	w.Write([]byte("hello"))
	w.Close()

	// full write with same key must succeed
	putAndClose(t, svc, 1, []byte("worldX"))

	got := readAll(t, svc, 1)
	if string(got) != "worldX" {
		t.Errorf("want 'worldX', got %q", got)
	}
}

// ── concurrent writes ─────────────────────────────────────────────────────────

func TestConcurrentWritesSameVolume(t *testing.T) {
	svc, _ := newTestSvc(t)
	ctx := context.Background()

	const N = 8
	writers := make([]io.WriteCloser, N)
	payloads := make([][]byte, N)
	for i := range writers {
		payloads[i] = bytes.Repeat([]byte{byte(i + 1)}, 16)
		w, err := svc.PutObj(ctx, uint64(i+1), 16)
		if err != nil {
			t.Fatalf("PutObj %d: %v", i, err)
		}
		writers[i] = w
	}

	var wg sync.WaitGroup
	for i, w := range writers {
		w, payload := w, payloads[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Write(payload)
			w.Close()
		}()
	}
	wg.Wait()

	for i := range writers {
		key := uint64(i + 1)
		got := readAll(t, svc, key)
		if !bytes.Equal(got, payloads[i]) {
			t.Errorf("key %d: data mismatch", key)
		}
	}
}

// TestWritesFIFOSameVolume verifies that writer 2 does not start writing until
// writer 1 has been closed. We observe this by issuing both writers while
// holding tickets in order, then confirming writer 2's Write blocks.
func TestWritesFIFOSameVolume(t *testing.T) {
	svc, _ := newTestSvc(t)
	ctx := context.Background()

	w1, _ := svc.PutObj(ctx, 1, 5)
	w2, _ := svc.PutObj(ctx, 2, 5)

	w2Wrote := make(chan struct{})
	go func() {
		w2.Write([]byte("22222")) // blocks until w1 is closed
		close(w2Wrote)
		w2.Close()
	}()

	// Let the goroutine reach waitTurn before we close w1.
	time.Sleep(10 * time.Millisecond)

	select {
	case <-w2Wrote:
		t.Fatal("writer 2 wrote before writer 1 was closed")
	default:
	}

	w1.Write([]byte("11111"))
	w1.Close()

	select {
	case <-w2Wrote:
		// correct: w2 unblocked after w1 closed
	case <-time.After(time.Second):
		t.Fatal("writer 2 never unblocked after writer 1 closed")
	}

	if got := readAll(t, svc, 1); string(got) != "11111" {
		t.Errorf("obj 1: want '11111', got %q", got)
	}
	if got := readAll(t, svc, 2); string(got) != "22222" {
		t.Errorf("obj 2: want '22222', got %q", got)
	}
}

// ── Stop ─────────────────────────────────────────────────────────────────────

func TestStopWaitsForWriters(t *testing.T) {
	svc, _ := newTestSvc(t)

	w, err := svc.PutObj(context.Background(), 1, 5)
	if err != nil {
		t.Fatalf("PutObj: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		svc.Stop(context.Background())
		close(stopped)
	}()

	// Stop must be blocked while the writer is open.
	select {
	case <-stopped:
		t.Fatal("Stop returned before the writer was closed")
	case <-time.After(20 * time.Millisecond):
	}

	w.Write([]byte("hello"))
	w.Close()

	select {
	case <-stopped:
		// correct
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after writer was closed")
	}
}

// ── multi-volume: parallel I/O ────────────────────────────────────────────────

// TestParallelIOAcrossVolumes verifies that writes to different volumes proceed
// independently and are not serialised by each other's per-volume FIFO queues.
//
// Layout with 2 volumes and a round-robin allocator (max=2):
//
//	PutObj(key=1) → vol 0, ticket 0
//	PutObj(key=2) → vol 1, ticket 0
//	PutObj(key=3) → vol 0, ticket 1  ← blocked until key=1's writer closes
//
// The test confirms that key=2's writer (vol 1) completes while key=3's writer
// (vol 0) is still blocked, then unblocks key=3 by closing key=1's writer.
func TestParallelIOAcrossVolumes(t *testing.T) {
	svc, _ := newTestSvcN(t, 2)
	ctx := context.Background()

	w1, _ := svc.PutObj(ctx, 1, 5) // vol 0, ticket 0
	w2, _ := svc.PutObj(ctx, 2, 5) // vol 1, ticket 0  — independent
	w3, _ := svc.PutObj(ctx, 3, 5) // vol 0, ticket 1  — blocked by w1

	w3Done := make(chan struct{})
	go func() {
		w3.Write([]byte("33333")) // waitTurn(1) blocks here until w1 closes
		w3.Close()
		close(w3Done)
	}()

	w2Done := make(chan struct{})
	go func() {
		w2.Write([]byte("22222")) // waitTurn(0) on vol 1: passes immediately
		w2.Close()
		close(w2Done)
	}()

	// Give goroutines time to reach their waitTurn calls.
	time.Sleep(10 * time.Millisecond)

	// w3 must still be blocked (vol 0 queue held by w1).
	select {
	case <-w3Done:
		t.Fatal("w3 completed before w1 was closed: vol 0 FIFO ordering broken")
	default:
	}

	// w2 must be done: vol 1 queue is independent of vol 0.
	select {
	case <-w2Done:
	case <-time.After(time.Second):
		t.Fatal("w2 did not complete: writes to vol 1 should not be serialised by vol 0")
	}

	// Unblock w3 by closing w1.
	w1.Write([]byte("11111"))
	w1.Close()

	select {
	case <-w3Done:
	case <-time.After(time.Second):
		t.Fatal("w3 did not complete after w1 was closed")
	}

	if got := readAll(t, svc, 1); string(got) != "11111" {
		t.Errorf("key 1: want '11111', got %q", got)
	}
	if got := readAll(t, svc, 2); string(got) != "22222" {
		t.Errorf("key 2: want '22222', got %q", got)
	}
	if got := readAll(t, svc, 3); string(got) != "33333" {
		t.Errorf("key 3: want '33333', got %q", got)
	}
}

// ── read before write completion ──────────────────────────────────────────────

// TestReadBeforeWriteCompletion verifies two related invariants:
//
//  1. An object is invisible to GetObj / GetObjsMap until its writer is closed
//     (AddObj is deferred to writer.Close after fdatasync).
//
//  2. Concurrent readers during a blocked write (waiting in the per-volume FIFO
//     queue) all see ErrNotFound — the object must not appear in the index
//     before its data is durable.
func TestReadBeforeWriteCompletion(t *testing.T) {
	svc, _ := newTestSvc(t)
	ctx := context.Background()

	// ── Part 1: sequential ───────────────────────────────────────────────────
	w, _ := svc.PutObj(ctx, 1, 5)
	w.Write([]byte("hello"))

	if _, _, err := svc.GetObj(ctx, 1); !errors.Is(err, &models.ErrNotFound{}) {
		t.Fatalf("GetObj while writer is open: want ErrNotFound, got %v", err)
	}
	if metas, _ := svc.GetObjsMap(ctx); len(metas) != 0 {
		t.Fatalf("GetObjsMap while writer is open: want 0 entries, got %d", len(metas))
	}

	w.Close()

	if _, _, err := svc.GetObj(ctx, 1); err != nil {
		t.Fatalf("GetObj after writer closed: %v", err)
	}

	// ── Part 2: concurrent reads during a blocked write ──────────────────────
	//
	// vol 0 queue state after the key=1 write: issued=1, serving=1.
	//   PutObj(key=2) → w_gate:    ticket=1  (serving==1, passes waitTurn immediately)
	//   PutObj(key=3) → w_blocked: ticket=2  (serving==1, blocks in waitTurn)
	//
	// While w_blocked is stuck, 16 concurrent readers must all see ErrNotFound
	// for key=3 because AddObj has not been called yet.
	w_gate, _ := svc.PutObj(ctx, 2, 5)
	w_blocked, _ := svc.PutObj(ctx, 3, 5)

	writerDone := make(chan struct{})
	go func() {
		w_blocked.Write([]byte("33333")) // blocks in waitTurn(2) until w_gate closes
		w_blocked.Close()
		close(writerDone)
	}()

	time.Sleep(5 * time.Millisecond) // let the goroutine reach waitTurn

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := svc.GetObj(ctx, 3)
			if !errors.Is(err, &models.ErrNotFound{}) {
				t.Errorf("concurrent GetObj of in-flight write: want ErrNotFound, got %v", err)
			}
		}()
	}
	wg.Wait()

	// Release w_blocked by completing w_gate.
	w_gate.Write([]byte("22222"))
	w_gate.Close()

	<-writerDone

	if got := readAll(t, svc, 3); string(got) != "33333" {
		t.Errorf("key 3 after write complete: want '33333', got %q", got)
	}
}
