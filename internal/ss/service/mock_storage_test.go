package service

import (
	"bytes"
	"context"
	"io"
	"sync"

	"HaystackAtHome/internal/ss/models"
)

// mockStorage is a thread-safe in-memory implementation of models.Storage used
// exclusively in service tests. It stores object data in a plain map and does
// not emulate needle framing, checksums, or fdatasync.
type mockStorage struct {
	mu   sync.Mutex
	vols map[uint64]*mockVol
}

type mockVol struct {
	maxSz   uint64
	nextOff uint64
	objs    map[uint64]*mockObj
}

type mockObj struct {
	info models.ObjInfo
	data []byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{vols: make(map[uint64]*mockVol)}
}

// addVol registers a volume before the service is constructed so that
// InitObjTable finds it via ListVolumes.
func (m *mockStorage) addVol(key, maxSz uint64) {
	m.vols[key] = &mockVol{maxSz: maxSz, objs: make(map[uint64]*mockObj)}
}

// addObj pre-populates an object so that InitObjTable rebuilds it in the index.
func (m *mockStorage) addObj(volKey uint64, info models.ObjInfo, data []byte) {
	d := make([]byte, len(data))
	copy(d, data)
	m.vols[volKey].objs[info.Key] = &mockObj{info: info, data: d}
}

// ── models.Storage interface ──────────────────────────────────────────────────

func (m *mockStorage) Stats(_ context.Context) *models.StorageStats {
	return &models.StorageStats{}
}

func (m *mockStorage) AddVolume(_ context.Context, _ string, _ uint64) (uint64, error) {
	return 0, &models.ErrUnimplemented{}
}

func (m *mockStorage) RemoveVolume(_ context.Context, _ uint64) error {
	return &models.ErrUnimplemented{}
}

func (m *mockStorage) CompactVolume(_ context.Context, _, _ uint64) error {
	return &models.ErrUnimplemented{}
}

func (m *mockStorage) ListVolumes(_ context.Context) ([]models.Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	vols := make([]models.Volume, 0, len(m.vols))
	for key, v := range m.vols {
		vols = append(vols, models.Volume{
			Key:   key,
			Space: models.VolumeSpaceUsage{Free: v.maxSz, Used: 0},
		})
	}
	return vols, nil
}

func (m *mockStorage) ListObjects(_ context.Context, volKey uint64) ([]models.ObjInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vols[volKey]
	if !ok {
		return nil, models.NewErrNotFound("no such volume")
	}
	out := make([]models.ObjInfo, 0, len(v.objs))
	for _, obj := range v.objs {
		out = append(out, obj.info)
	}
	return out, nil
}

// mockWriter buffers written bytes; Close commits them to the mock's vol map.
type mockWriter struct {
	stor   *mockStorage
	volKey uint64
	key    uint64
	size   uint64
	off    uint64
	buf    bytes.Buffer
}

func (w *mockWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

func (w *mockWriter) Close() error {
	// Store exactly size bytes (zero-pad if service padToFull wrote fewer than size
	// because buf may contain real bytes padded by the service layer).
	data := make([]byte, w.size)
	copy(data, w.buf.Bytes())

	w.stor.mu.Lock()
	defer w.stor.mu.Unlock()
	v := w.stor.vols[w.volKey]
	v.objs[w.key] = &mockObj{
		info: models.ObjInfo{Key: w.key, DataSize: w.size, Offset: w.off},
		data: data,
	}
	return nil
}

func (m *mockStorage) PutObjectWriter(volKey, objKey, dataSize uint64) (io.WriteCloser, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vols[volKey]
	if !ok {
		return nil, 0, models.NewErrNotFound("no such volume")
	}
	off := v.nextOff
	v.nextOff += dataSize
	return &mockWriter{stor: m, volKey: volKey, key: objKey, size: dataSize, off: off}, off, nil
}

func (m *mockStorage) GetObjectReader(volKey, objKey, _ uint64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vols[volKey]
	if !ok {
		return nil, models.NewErrNotFound("no such volume")
	}
	obj, ok := v.objs[objKey]
	if !ok {
		return nil, models.NewErrNotFound("no such object")
	}
	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

func (m *mockStorage) MarkDeleteObject(_ context.Context, volKey, objKey, _ uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vols[volKey]
	if !ok {
		return models.NewErrNotFound("no such volume")
	}
	obj, ok := v.objs[objKey]
	if !ok {
		return models.NewErrNotFound("no such object")
	}
	obj.info.Flags.Deleted = true
	return nil
}

func (m *mockStorage) Close(_ context.Context) error {
	return nil
}
