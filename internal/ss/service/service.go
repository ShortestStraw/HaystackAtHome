package service

import (
	"HaystackAtHome/internal/ss/models"
	"HaystackAtHome/internal/ss/service/accumulator"
	"HaystackAtHome/internal/ss/service/allocator"
	"HaystackAtHome/internal/ss/service/otable"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"time"
	"unsafe"
)

const (
	memLimitDefault   = 8 * 1024 * 1024 // 8 GiB in KiB
	memLimitCorrector = 256              // KiB reserved for OTable map overhead
)

type alloc_t int

const (
	ALLOCATOR_UNKNOWN alloc_t = iota
	ALLOCATOR_RR

	ALLOCATOR_DEF = ALLOCATOR_RR
)

// Service implements models.Service. It owns the object index, volume allocator,
// and I/O accumulators; all storage I/O is delegated to models.Storage.
type Service struct {
	stor      models.Storage
	logg      *slog.Logger
	memLimit  uint32 // in KiB
	objsLock  *sync.RWMutex
	objs      *otable.OTable
	uid       uint64
	features  *models.ServiceFeatures
	raccum    *accumulator.Accumulator
	waccum    *accumulator.Accumulator
	oalloc    allocator.Allocator
	oalloct   alloc_t
	volQueues sync.Map            // uint64 -> *volQueue
	pending   map[uint64]uint64   // volKey -> bytes reserved but not yet durable; protected by objsLock
	wg        sync.WaitGroup      // counts writers returned to callers but not yet closed
}

type Option func(s *Service)

func WithLogger(logg *slog.Logger) Option {
	return func(s *Service) {
		s.logg = logg
	}
}

// WithMemoryLimit sets the in-memory index budget in KiB.
func WithMemoryLimit(memLimit uint32) Option {
	return func(s *Service) {
		s.memLimit = memLimit
	}
}

func WithUID(uid uint64) Option {
	return func(s *Service) {
		s.uid = uid
	}
}

func WithServiceFeatures(features models.ServiceFeatures) Option {
	return func(s *Service) {
		s.features = &models.ServiceFeatures{
			Checksum: features.Checksum,
		}
	}
}

func WithAllocatorRR() Option {
	return func(s *Service) {
		s.oalloct = ALLOCATOR_RR
	}
}

// New constructs a Service. Call InitObjTable to populate the object index
// before serving requests. WithServiceFeatures is mandatory.
// If WithUID is not provided a random UID is generated.
func New(ctx context.Context, stor models.Storage, opts ...Option) (*Service, error) {
	s := &Service{
		stor:     stor,
		objsLock: &sync.RWMutex{},
		oalloct:  ALLOCATOR_DEF,
		memLimit: memLimitDefault,
		pending:  make(map[uint64]uint64),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.objs = otable.New()

	if s.uid == 0 {
		val, err := rand.Int(rand.Reader, big.NewInt(1<<62))
		if err != nil {
			return nil, fmt.Errorf("failed to generate uid: %v", err)
		}
		s.uid = val.Uint64()
	}

	if err := s.validate(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Service) validate(ctx context.Context) error {
	if s.features == nil {
		return models.NewErrInvalidParams("features must be specified with WithServiceFeatures Option")
	}

	if s.memLimit < 1024 {
		return models.NewErrInvalidParams("memLimit must be greater than 1024 KiB")
	}

	if s.stor == nil {
		return models.NewErrInvalidParams("stor must be non-nil")
	}

	if s.stor.Stats(ctx) == nil {
		return models.NewErrInvalidParams("failed to query storage stats as storage test")
	}

	return nil
}

// InitObjTable scans storage, rebuilds the object index, and initialises the
// allocator and I/O accumulators. Must be called once before serving requests.
func (s *Service) InitObjTable(ctx context.Context) error {
	s.objsLock.Lock()
	defer s.objsLock.Unlock()

	vols, err := s.stor.ListVolumes(ctx)
	if err != nil {
		return fmt.Errorf("failed to list storage volumes: %v", err)
	}

	for _, vol := range vols {
		objsInfo, err := s.stor.ListObjects(ctx, vol.Key)
		if err != nil {
			return fmt.Errorf("failed to list volume key=%d: %v", vol.Key, err)
		}
		s.objs.AddVolume(vol)
		for _, objInfo := range objsInfo {
			s.objs.AddObj(vol.Key, objInfo)
		}
	}

	s.waccum = accumulator.New(1 * time.Second)
	s.raccum = accumulator.New(1 * time.Second)

	switch s.oalloct {
	case ALLOCATOR_RR:
		s.oalloc = allocator.NewRR(uint64(len(vols)))
	case ALLOCATOR_UNKNOWN:
		return models.NewErrInvalidParams("unknown allocator is specified")
	}

	return nil
}

func (s *Service) GetServiceInfo(ctx context.Context) (models.ServiceInfo, error) {
	vols, err := s.stor.ListVolumes(ctx)
	if err != nil {
		return models.ServiceInfo{}, fmt.Errorf("failed to list storage volumes: %v", err)
	}

	usage := models.SpaceUsage{}
	for _, vol := range vols {
		usage.Total += vol.Space.Used + vol.Space.Free
		usage.Used += vol.Space.Used
	}

	return models.ServiceInfo{
		UID:      s.uid,
		Space:    usage,
		Features: *s.features,
	}, nil
}

// freeMemSize returns the remaining index budget in KiB (0 if over limit).
func (s *Service) freeMemSize() uint32 {
	usedKiB := s.objs.ObjNum() * uint32(unsafe.Sizeof(otable.OInfo{})) / 1024
	if s.memLimit < usedKiB+memLimitCorrector {
		if s.logg != nil {
			s.logg.Error("no free memory left")
		}
		return 0
	}
	free := s.memLimit - usedKiB - memLimitCorrector
	if free < s.memLimit/10 && s.logg != nil {
		s.logg.Warn("less than 10% of memory limit left", "free_kib", free, "limit_kib", s.memLimit)
	}
	return free
}

func (s *Service) GetObjsMap(ctx context.Context) ([]models.ObjMeta, error) {
	s.objsLock.RLock()
	defer s.objsLock.RUnlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	list := s.objs.List()
	metas := make([]models.ObjMeta, 0, len(list))
	for _, oi := range list {
		metas = append(metas, models.ObjMeta{
			Key:  oi.Key,
			Size: oi.DataSize,
		})
	}

	if s.logg != nil {
		s.logg.Info("GetObjsMap", "num", len(metas))
	}

	return metas, nil
}

func (s *Service) PutObj(ctx context.Context, objKey, dataSize uint64) (io.WriteCloser, error) {
	s.objsLock.Lock()

	if s.oalloc == nil {
		s.objsLock.Unlock()
		return nil, models.NewErrInvalidParams("service not initialized, call InitObjTable first")
	}

	// Reject live duplicates; allow re-put of soft-deleted keys.
	if ext, err := s.objs.LookupExt(objKey); err == nil && !ext.Flags.Deleted {
		s.objsLock.Unlock()
		if s.logg != nil {
			s.logg.Error("PutObj object exists", "key", objKey)
		}
		return nil, models.NewErrExists(fmt.Sprintf("object with key=%d already exists", objKey))
	}

	if s.freeMemSize() == 0 {
		s.objsLock.Unlock()
		return nil, models.NewErrNoMem("no memory left for object index")
	}

	// Account for in-flight writes when checking free space.
	volKey := s.oalloc.Next(dataSize)
	used := s.objs.CurrentSize(volKey) + s.pending[volKey]
	maxSize := s.objs.MaxSize(volKey)
	if maxSize < used || maxSize-used < dataSize {
		s.objsLock.Unlock()
		if s.logg != nil {
			s.logg.Error("no free space", "vol", volKey, "need", dataSize)
		}
		return nil, models.NewErrNoMem("no free space left")
	}

	// Reserve space and capture ticket before releasing the lock so that ticket
	// order matches PutObj call order (both are serialised by objsLock).
	s.pending[volKey] += dataSize
	vq := s.getVolQueue(volKey)
	ticket := vq.issue()
	s.objsLock.Unlock()

	// PutObjectWriter writes the needle header and must not run under objsLock
	// — it would block reads for the duration of a syscall.
	fd, off, err := s.stor.PutObjectWriter(volKey, objKey, dataSize)
	if err != nil {
		s.objsLock.Lock()
		s.pending[volKey] -= dataSize
		s.objsLock.Unlock()
		return nil, fmt.Errorf("failed to get object writer: %v", err)
	}

	s.wg.Add(1)
	return &writer{
		s:      s,
		fd:     fd,
		key:    objKey,
		volKey: volKey,
		off:    off,
		size:   dataSize,
		vq:     vq,
		ticket: ticket,
	}, nil
}

func (s *Service) GetObj(ctx context.Context, objKey uint64) (io.ReadCloser, uint64, error) {
	s.objsLock.RLock()
	defer s.objsLock.RUnlock()

	ext, err := s.objs.LookupExt(objKey)
	if err != nil {
		return nil, 0, models.NewErrNotFound(fmt.Sprintf("object with key=%d not found", objKey))
	}
	if ext.Flags.Deleted {
		return nil, 0, models.NewErrNotFound(fmt.Sprintf("object with key=%d is deleted", objKey))
	}

	rc, err := s.stor.GetObjectReader(ext.VolKey, objKey, ext.Off)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get object reader: %v", err)
	}

	return &reader{s: s, rc: rc, volKey: ext.VolKey}, ext.DataSize, nil
}

func (s *Service) DelObj(ctx context.Context, objKey uint64) error {
	s.objsLock.Lock()
	defer s.objsLock.Unlock()

	ext, err := s.objs.LookupExt(objKey)
	if err != nil {
		return models.NewErrNotFound(fmt.Sprintf("object with key=%d not found", objKey))
	}

	if err := s.stor.MarkDeleteObject(ctx, ext.VolKey, objKey, ext.Off); err != nil {
		return fmt.Errorf("failed to mark object deleted in storage: %v", err)
	}

	return s.objs.MarkDeleted(objKey)
}

func (s *Service) Stop(ctx context.Context) error {
	// Wait for all returned writers to be closed before shutting storage down.
	s.wg.Wait()
	return s.stor.Close(ctx)
}

func (s *Service) getVolQueue(volKey uint64) *volQueue {
	if raw, ok := s.volQueues.Load(volKey); ok {
		return raw.(*volQueue)
	}
	actual, _ := s.volQueues.LoadOrStore(volKey, newVolQueue())
	return actual.(*volQueue)
}

// ── writer ────────────────────────────────────────────────────────────────────

type writer struct {
	s       *Service
	fd      io.WriteCloser
	key     uint64
	volKey  uint64
	off     uint64
	size    uint64
	written uint64
	vq      *volQueue
	ticket  uint64
	started bool
}

// Write blocks on the first call until all writers issued before this one on
// the same volume have been closed, then holds the turn for all subsequent
// Write calls until Close.
func (w *writer) Write(b []byte) (int, error) {
	if !w.started {
		w.vq.waitTurn(w.ticket)
		w.started = true
	}
	n, err := w.fd.Write(b)
	if n > 0 {
		w.written += uint64(n)
		if w.s.waccum != nil {
			w.s.waccum.Account(w.volKey, uint64(n))
		}
	}
	return n, err
}

// padToFull fills the needle's remaining data bytes with zeros.
// This keeps the volume layout consistent when the caller abandons a write
// (e.g. network cancellation): the needle on disk gets a valid footer so the
// volume scanner can traverse past it. The object is not added to the index,
// so GetObj can never return the corrupted data.
func (w *writer) padToFull() {
	const chunkSize = 32 * 1024
	var buf [chunkSize]byte
	remaining := w.size - w.written
	for remaining > 0 {
		n := remaining
		if n > chunkSize {
			n = chunkSize
		}
		written, err := w.fd.Write(buf[:n])
		remaining -= uint64(written)
		if err != nil { // includes io.EOF when the needle is exactly full
			return
		}
	}
}

// Close flushes the write to storage, adds the object to the index (so GetObj
// only sees it after fdatasync), releases the volume queue slot, and signals
// the service WaitGroup. If Write was never called, Close still waits for its
// turn before releasing so the queue always advances correctly.
func (w *writer) Close() error {
	if !w.started {
		w.vq.waitTurn(w.ticket)
	}

	// Pad with zeros if the caller did not write all declared bytes.
	// The needle must be complete on disk for the volume scanner to advance
	// past it correctly; an incomplete needle corrupts all subsequent offsets.
	if w.written < w.size {
		w.padToFull()
	}

	// fd.Close triggers fdatasync. Run without objsLock so concurrent reads
	// are not blocked for the full duration of disk flush.
	err := w.fd.Close()

	w.s.objsLock.Lock()
	// Index the object only when all declared bytes were written and flushed.
	// A partial write (e.g. cancelled RPC) is padded with zeros for volume
	// consistency but must not appear in the index — the key stays reusable.
	if err == nil && w.written == w.size {
		_ = w.s.objs.AddObj(w.volKey, models.ObjInfo{
			Key:      w.key,
			DataSize: w.size,
			Offset:   w.off,
		})
	}
	w.s.pending[w.volKey] -= w.size
	w.s.objsLock.Unlock()

	w.vq.release()
	w.s.wg.Done()
	return err
}

// ── reader ────────────────────────────────────────────────────────────────────

type reader struct {
	s      *Service
	rc     io.ReadCloser
	volKey uint64
}

func (r *reader) Read(b []byte) (int, error) {
	n, err := r.rc.Read(b)
	if n > 0 && r.s.raccum != nil {
		r.s.raccum.Account(r.volKey, uint64(n))
	}
	return n, err
}

func (r *reader) Close() error {
	return r.rc.Close()
}

// ── volQueue ──────────────────────────────────────────────────────────────────

// volQueue enforces FIFO write ordering for a single volume.
//
// Lifecycle per writer:
//  1. issue()    — called at PutObj time; reserves a slot in creation order.
//  2. waitTurn() — called on the first Write (or in Close if Write was skipped);
//                  blocks until all earlier writers have closed.
//  3. release()  — called in Close; advances the counter and wakes the next waiter.
//
// Writers for different volumes are completely independent.
type volQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	serving uint64
	issued  uint64
}

func newVolQueue() *volQueue {
	vq := &volQueue{}
	vq.cond = sync.NewCond(&vq.mu)
	return vq
}

// issue returns the next ticket in PutObj call order. Must be called at PutObj
// time so that the ticket sequence matches the allocation sequence.
func (vq *volQueue) issue() uint64 {
	vq.mu.Lock()
	t := vq.issued
	vq.issued++
	vq.mu.Unlock()
	return t
}

// waitTurn blocks until all writers issued before this ticket have called release.
func (vq *volQueue) waitTurn(ticket uint64) {
	vq.mu.Lock()
	for vq.serving != ticket {
		vq.cond.Wait()
	}
	vq.mu.Unlock()
}

// release signals the next waiter that it may proceed.
func (vq *volQueue) release() {
	vq.mu.Lock()
	vq.serving++
	vq.cond.Broadcast()
	vq.mu.Unlock()
}
