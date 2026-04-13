package storage

import (
	"HaystackAtHome/internal/ss/models"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	testcase = "testcase/"
	rootInit = testcase + ".stor.init"
)

func testOpen(t *testing.T, want error, opts... Option) {
	openCtx, openCancel := context.WithTimeout(t.Context(), time.Second * 3)
	defer openCancel()

	os.RemoveAll(rootInit)
	os.Mkdir(rootInit, 0o777)
	defer os.RemoveAll(rootInit)
	stor, err := Open(openCtx, rootInit, opts...)
	if diff := cmp.Diff(want, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to open storage (-want +got):\n%s", diff)
	}

	closeCtx, closeCancel := context.WithTimeout(t.Context(), time.Second * 1)
	defer closeCancel()
	err = stor.Close(closeCtx)

	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to close storage (-want +got):\n%s", diff)
	}
}

func addMetrics(reg prom.Registerer) *models.StorageMetrics {
	m := NewDefaultStorageMetrics()
	reg.MustRegister(m.TotalOps)
	reg.MustRegister(m.TotalReadBytes)
	reg.MustRegister(m.TotalWriteBytes)
	reg.MustRegister(m.Errors)
	reg.MustRegister(m.Latencies)
	reg.MustRegister(m.Compaction)
	reg.MustRegister(m.Sizes)
	return m
}

func testPromServer(t *testing.T) *models.StorageMetrics {
	t.Helper()
	reg := prom.NewRegistry()
	m := addMetrics(reg)
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
  go func() { http.ListenAndServe(":8080", nil) }()
	
	return m
}

func TestInit(t *testing.T) {	
	// no options
	testOpen(t, nil)
	// logger
	testOpen(t, nil, WithLogger(slog.Default().With("stor", rootInit)))
	// unsupproted buffering
	testOpen(t, &models.ErrUnimplemented{}, WithVolumeWriteBuffering(1024))
	// checksum
	testOpen(t, nil, WithObjectChecksumming())
	// metrics
	testOpen(t, nil, WithMetrics(NewDefaultStorageMetrics()))
}

func TestOpenExisting(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	stor, err := Open(ctx, fixturePath, WithObjectChecksumming())
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("Open (-want +got):\n%s", diff)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := stor.Close(closeCtx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	// check volumes
	vols, err := stor.ListVolumes(ctx)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("ListVolumes (-want +got):\n%s", diff)
	}
	if len(vols) != len(storDesc) {
		t.Fatalf("ListVolumes: got %d volumes, want %d", len(vols), len(storDesc))
	}

	// build path->volKey map from fixture for lookup
	pathToVolKey := make(map[string]uint64, len(storDesc))
	for volKey := range storDesc {
		path := fixturePath + fmt.Sprintf("/.volume.%d", volKey)
		pathToVolKey[path] = volKey
	}
	for _, v := range vols {
		if _, ok := pathToVolKey[v.Path]; !ok {
			t.Errorf("unexpected volume path %q", v.Path)
		}
	}

	// check objects per volume
	for volKey, needles := range storDesc {
		objs, err := stor.ListObjects(ctx, volKey)
		if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("ListObjects vol=%d (-want +got):\n%s", volKey, diff)
			continue
		}
		if len(objs) != len(needles) {
			t.Errorf("ListObjects vol=%d: got %d objects, want %d", volKey, len(objs), len(needles))
			continue
		}
		for j, obj := range objs {
			fix := needles[j]
			if obj.Key != fix.Key {
				t.Errorf("vol=%d obj[%d]: Key=%d want %d", volKey, j, obj.Key, fix.Key)
			}
			if obj.DataSize != fix.DataSize {
				t.Errorf("vol=%d obj[%d]: DataSize=%d want %d", volKey, j, obj.DataSize, fix.DataSize)
			}
			if obj.Offset != fix.Off {
				t.Errorf("vol=%d obj[%d]: Offset=0x%X want 0x%X", volKey, j, obj.Offset, fix.Off)
			}
			if obj.Flags.Deleted {
				t.Errorf("vol=%d obj[%d]: unexpectedly marked deleted", volKey, j)
			}
			if obj.Flags.CsMismatched {
				t.Errorf("vol=%d obj[%d]: checksum mismatch", volKey, j)
			}
		}
	}
}

func TestVolumeAPI(t *testing.T) {
	openCtx, openCancel := context.WithTimeout(t.Context(), time.Second * 3)
	defer openCancel()

	root := testcase + ".root.1"
	os.RemoveAll(root)
	os.Mkdir(root, 0o777)
	defer os.RemoveAll(root)

	m := testPromServer(t)

	stor, err := Open(openCtx, root,
		WithLogger(slog.Default().With("root", root)),
		WithObjectChecksumming(),
		WithMetrics(m),
	)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to open storage (-want +got):\n%s", diff)
	}

	listCtx, listCancel := context.WithTimeout(t.Context(), time.Second * 10)
	defer listCancel()

	list, err := stor.ListVolumes(listCtx)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to list volumes (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]models.Volume{}, list); diff != "" {
		t.Fatalf("sotrage volume list mismatch (-want +got):\n%s", diff)
	}

	addCtx, addCancel := context.WithTimeout(t.Context(), time.Second * 10)
	defer addCancel()

	_, err = stor.AddVolume(addCtx, "abvgd", 0)
	if diff := cmp.Diff(&models.ErrInvalidParams{}, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("unexpected error duting creating volume (-want +got):\n%s", diff)
	}
	_, err = stor.AddVolume(addCtx, "123", 20)
	if diff := cmp.Diff(&models.ErrInvalidParams{}, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("unexpected error duting creating volume (-want +got):\n%s", diff)
	}

	maxSz := uint64(1024 * 1024)
	volKey1, err := stor.AddVolume(addCtx, "123", maxSz)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("unexpected error duting creating volume 123 (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(uint64(123), volKey1); diff != "" {
		t.Fatalf("volume 123 key mismatch (-want +got):\n%s", diff)
	}

	list, err = stor.ListVolumes(listCtx)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to list volumes (-want +got):\n%s", diff)
	}
	want := []models.Volume{
		{
			Path: root + "/.volume.123",
			Space: models.VolumeSpaceUsage{
				Free: maxSz - 40,
				Used: 40,
			},
		},
	}
	if diff := cmp.Diff(want, list); diff != "" {
		t.Fatalf("sotrage volume list mismatch (-want +got):\n%s", diff)
	}

	rmCtx, rmCancel := context.WithTimeout(t.Context(), time.Second * 5)
	defer rmCancel()

	err = stor.RemoveVolume(rmCtx, 5)
	if diff := cmp.Diff(&models.ErrInvalidParams{}, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to list volumes (-want +got):\n%s", diff)
	}

	err = stor.RemoveVolume(rmCtx, 123)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to list volumes (-want +got):\n%s", diff)
	}

	list, err = stor.ListVolumes(listCtx)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to list volumes (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]models.Volume{}, list); diff != "" {
		t.Fatalf("sotrage volume list mismatch (-want +got):\n%s", diff)
	}

	closeCtx, closeCancel := context.WithTimeout(t.Context(), time.Second * 1)
	defer closeCancel()

	err = stor.Close(closeCtx)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("failed to open storage (-want +got):\n%s", diff)
	}
}

func TestTwoWritesParallel(t *testing.T) {
	root := testcase + ".root.twp"
	os.RemoveAll(root)
	os.Mkdir(root, 0o777)
	defer os.RemoveAll(root)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	stor, err := Open(ctx, root, WithObjectChecksumming())
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("Open (-want +got):\n%s", diff)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := stor.Close(closeCtx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	volKey, err := stor.AddVolume(ctx, "1", 64*1024*1024)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("AddVolume (-want +got):\n%s", diff)
	}

	data1 := bytes.Repeat([]byte{0xAA}, 4*1024)
	data2 := bytes.Repeat([]byte{0xBB}, 8*1024)

	wr1, off1, err := stor.PutObjectWriter(volKey, 1, uint64(len(data1)))
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("PutObjectWriter 1 (-want +got):\n%s", diff)
	}
	wr2, off2, err := stor.PutObjectWriter(volKey, 2, uint64(len(data2)))
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("PutObjectWriter 2 (-want +got):\n%s", diff)
	}

	doWrite := func(wg *sync.WaitGroup, wr io.WriteCloser, data []byte) {
		defer wg.Done()
		if _, err := wr.Write(data); err != nil && err != io.EOF {
			t.Errorf("Write: %v", err)
			return
		}
		if err := wr.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go doWrite(&wg, wr1, data1)
	go doWrite(&wg, wr2, data2)
	wg.Wait()

	checkRead := func(objKey, off uint64, want []byte) {
		t.Helper()
		rd, err := stor.GetObjectReader(volKey, objKey, off)
		if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("GetObjectReader key=%d (-want +got):\n%s", objKey, diff)
			return
		}
		defer rd.Close()
		got, err := io.ReadAll(rd)
		if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("ReadAll key=%d (-want +got):\n%s", objKey, diff)
			return
		}
		if !bytes.Equal(got, want) {
			t.Errorf("key=%d: data mismatch (len got=%d want=%d)", objKey, len(got), len(want))
		}
	}

	checkRead(1, off1, data1)
	checkRead(2, off2, data2)
}

// TestOneWriteManyReadsParallel verifies that a single large in-flight write
// does not corrupt concurrent reads of already-committed objects. Objects are
// sized at several MiB so that the scheduler can preempt goroutines mid-read
// and mid-write, exercising the volume's concurrent-access paths.
func TestOneWriteManyReadsParallel(t *testing.T) {
	const (
		numReaders  = 5
		preWriteSz  = 2 * 1024 * 1024  // 2 MiB per pre-written object
		writeSz     = 4 * 1024 * 1024  // 4 MiB for the concurrent write
		writeChunk  = 4 * 1024         // 4 KiB chunks — keeps the writer in-flight longer
		readChunk   = 4 * 1024         // 4 KiB chunks for readers
		volMaxSz    = 256 * 1024 * 1024
	)

	root := testcase + ".root.owmr"
	os.RemoveAll(root)
	os.Mkdir(root, 0o777)
	defer os.RemoveAll(root)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	stor, err := Open(ctx, root, WithObjectChecksumming())
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("Open (-want +got):\n%s", diff)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := stor.Close(closeCtx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	volKey, err := stor.AddVolume(ctx, "1", volMaxSz)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("AddVolume (-want +got):\n%s", diff)
	}

	// Pre-write numReaders objects that will be read concurrently.
	type preObj struct {
		objKey uint64
		off    uint64
		data   []byte
	}
	preObjs := make([]preObj, numReaders)
	for i := range preObjs {
		data := bytes.Repeat([]byte{byte(i + 1)}, preWriteSz)
		wr, off, err := stor.PutObjectWriter(volKey, uint64(i+1), uint64(len(data)))
		if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
			t.Fatalf("PutObjectWriter pre[%d] (-want +got):\n%s", i, diff)
		}
		if _, err := wr.Write(data); err != nil && err != io.EOF {
			t.Fatalf("Write pre[%d]: %v", i, err)
		}
		if err := wr.Close(); err != nil {
			t.Fatalf("Close pre[%d]: %v", i, err)
		}
		preObjs[i] = preObj{uint64(i + 1), off, data}
	}

	// Start the single large write in a goroutine, writing in small chunks so
	// it stays in-flight while the readers run.
	writeData := bytes.Repeat([]byte{0xFF}, writeSz)
	writerKey := uint64(numReaders + 1)
	wr, wrOff, err := stor.PutObjectWriter(volKey, writerKey, uint64(len(writeData)))
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("PutObjectWriter writer (-want +got):\n%s", diff)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		src := writeData
		for len(src) > 0 {
			chunk := writeChunk
			if chunk > len(src) {
				chunk = len(src)
			}
			n, err := wr.Write(src[:chunk])
			src = src[n:]
			if err == io.EOF {
				break // all DataSize bytes consumed
			}
			if err != nil {
				t.Errorf("writer Write: %v", err)
				return
			}
		}
		if err := wr.Close(); err != nil {
			t.Errorf("writer Close: %v", err)
		}
	}()

	// Launch numReaders readers concurrently, each reading their pre-written
	// object in small chunks.
	for _, o := range preObjs {
		o := o
		wg.Add(1)
		go func() {
			defer wg.Done()
			rd, err := stor.GetObjectReader(volKey, o.objKey, o.off)
			if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("GetObjectReader key=%d (-want +got):\n%s", o.objKey, diff)
				return
			}
			defer rd.Close()

			got := make([]byte, 0, preWriteSz)
			buf := make([]byte, readChunk)
			for {
				n, err := rd.Read(buf)
				got = append(got, buf[:n]...)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Errorf("reader key=%d Read: %v", o.objKey, err)
					return
				}
			}

			if !bytes.Equal(got, o.data) {
				t.Errorf("key=%d: data mismatch (got %d bytes, want %d)", o.objKey, len(got), len(o.data))
			}
		}()
	}

	wg.Wait()

	// Verify the written object is also readable and correct after all goroutines finish.
	rd, err := stor.GetObjectReader(volKey, writerKey, wrOff)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("GetObjectReader writer (-want +got):\n%s", diff)
	}
	defer rd.Close()
	got, err := io.ReadAll(rd)
	if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
		t.Fatalf("ReadAll writer (-want +got):\n%s", diff)
	}
	if !bytes.Equal(got, writeData) {
		t.Fatalf("writer: data mismatch (got %d bytes, want %d)", len(got), len(writeData))
	}
}