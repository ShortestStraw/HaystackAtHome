package storage

import (
	"HaystackAtHome/internal/ss/models"
	"HaystackAtHome/internal/ss/storage/volume"
	"container/list"
	"context"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"HaystackAtHome/internal/ss/storage/needle"
	"bytes"
	"io/fs"
	"log/slog"
	"os"

	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	volPrefix = ".volume."
	timeoutVolsClose = 30 * time.Second
	timeoutWriteWaiter = 20 * time.Second
)

type volCtx struct {
	lock   *sync.RWMutex // for cuncurrent ops with volume and volume context
	                     // it is very hot RWMutex, so may need research for other 
											 // implementations if fairness will not match our workload
	v      *volume.Volume
	wq     *list.List // list of chans for notification of enqueued goroutines
	off    uint64 // volume offset with enqueued write requests
	buf    *struct {
		vol    bytes.Buffer  // indexed by volume Keys
		sz     uint64   // if set to 0 then buffering is off
	}
}

type waiter struct {
	ch    <-chan error
}

// must be called not under volCtx lock
func (w *waiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-w.ch:
		return err
	}
}

// must be called under volCtx Wlock
func enqueueSelf(wq *list.List) (*waiter) {
	ch := make(chan error, 1)
	w := &waiter{ ch: ch }
	wq.PushFront(ch)
	return w
}

// must be called under volCtx Wlock
func notifyNext(wq *list.List, err error) {
	if wq.Len() == 0 {
		return
	}

	e := wq.Back()
	ch := wq.Remove(e).(chan error)
	ch <- err
	close(ch)
}

// must be called under volCtx Wlock
func (vc *volCtx) enqueueSelf(writeSize uint64) *waiter {
	written, _ := vc.v.Size()
	immediateNotify := vc.off == written
	vc.off += writeSize
	w := enqueueSelf(vc.wq)
	if immediateNotify {
		notifyNext(vc.wq, nil)
	}
	return w
}

// must be called under volCtx Wlock
func (vc *volCtx) notifyNext(err error) {
	notifyNext(vc.wq, err)
}

/*
	On disk storage organized as directory on filesystem
	storage_root/
	- .index
	- .volume.0
	- .volume.1
	...

	Storage have slow scan. It scanned storage_root
	for .volume.$i and .index files, then scan index file to retrive [object -> volume] mapping
	to memory. But .index file may be delayed for volume files, so .volume.$i will be scanned from
	last offset, indexed in .index, to the end and update [object -> volume] mapping.

	TODO lsm .index file for fast start. Maybe it will be better to have own .index for each volume
	since reading and iterating over index may be paralleled with consequetive maps merge. see sqlite

	TODO write recovery on partial or cancelled writes to volume. BLOCKER

	TODO stats fields

	TODO buffering, after write recovery. With buffers for every volume we will be able:
		1) call Sync once for many little objects. low prio
		2) call Sync on timeout
		3) call Sync after unsynced bypass some threshold
	This optimization will increase a little latency of writes but may noticeable increase bandwidth.
	May be too hard to implement with current architecture
*/
type Storage struct {
	root       *os.Root
	logger     *slog.Logger
	metrics    *models.StorageMetrics
	csOn       bool
	buffering  uint64 // if non null buffering is on and size of buffers is equal to this value

	// for addition and deletion elements from vol maps
	// for modification of volumes can be read locked but
	// volCtx-s must be locked with it RWLock for modification
	volsMtx    *sync.RWMutex 
	vol        map[uint64]*volCtx
}

type Option func (*Storage)

func WithLogger(logger *slog.Logger) Option {
	return func (stor *Storage) {
		stor.logger = logger
	}
}

// metrics should be inited by caller
func WithMetrics(metrics *models.StorageMetrics) Option {
	return func (stor *Storage) {
		stor.metrics = metrics
	}
}

// turn on checksum validation on reading and calculation on writing
// objects can still be read with checksum mismatch
func WithObjectChecksumming() Option {
	return func (stor *Storage) {
		stor.csOn = true
	}
}

func WithVolumeWriteBuffering(bufSize uint64) Option {
	return func(stor *Storage) {
		stor.buffering = bufSize
	}
}

func validateOpts(stor *Storage) error {
	if stor.buffering != 0 {
		return fmt.Errorf("buffering is not implemented yet: %w", &models.ErrUnimplemented{})
	}
	if stor.csOn == false {
		if stor.logger != nil {
			stor.logger.Warn("storage does not support no disabled checksumming yet, turn it implicitly")
		}
		stor.csOn = true
	}
	return nil
}

/*
Open file directory with volumes and index file which is the working 
for this storage implementation. 

On success returns the Storage instance which implements the models.Storage
interface and nil error.

On error returns nil *Storage and non nil error which is either *os.PathError, 
one of errors defined in models or io errors.

Open also opens all files as volumes for storing objects which match the prefix ".volume."
*/
func Open(ctx context.Context, storRoot string, opts... Option) (*Storage, error) {
	root, err := os.OpenRoot(storRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage root: %w", err)
	}

	stor := &Storage{
		root: root,
		volsMtx: &sync.RWMutex{},
		vol: make(map[uint64]*volCtx),
	}

	for _, opt := range opts {
		opt(stor)
	}

	if err := validateOpts(stor); err != nil {
		_ = stor.root.Close()
		return nil, fmt.Errorf("params validation failed: %w", err)
	}

	enrties, err := fs.ReadDir(stor.root.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("failed to list files in storage root: %w", err)
	}

	for _, e := range enrties {
		if e.Type().IsRegular() && strings.HasPrefix(e.Name(), volPrefix) {
			// since there cannot be concurrent users of this structure we allowed to call unsafe methods 
			_, err := stor.openVolUnsafe(ctx, stor.root.Name() + "/" + e.Name())
			if err != nil {
				if stor.logger != nil {
					stor.logger.Error("failed to open vol", "path", e.Name(), "err", err)
				}
				closeCtx, cancel := context.WithTimeout(ctx, timeoutVolsClose)
				defer cancel()
				if err := stor.close(closeCtx); err != nil {
					if stor.logger != nil {
						stor.logger.Error("failed to close opened vols", "err", err)
					}
				}
				return nil, fmt.Errorf("failed to open vol '%s': %w", e.Name(), err)
			}
		}
	}
	return stor, nil
}

// absolute or relative to cwd
func (stor *Storage) volPath(volKey uint64) string {
	return stor.root.Name() + "/" + fmt.Sprintf("%s%d", volPrefix, volKey)
}

func (stor *Storage) metricsAddVol(volKey, size uint64) {
	if stor.metrics != nil {
		if stor.metrics.Sizes != nil {
			label := stor.volPath(volKey)
			stor.metrics.Sizes.WithLabelValues(label).Set(float64(size))
		}
	}
}

func (stor *Storage) metricsRemoveVol(volKey uint64) {
	if stor.metrics != nil {
		if stor.metrics.Sizes != nil {
			label := stor.volPath(volKey)
			stor.metrics.Sizes.DeleteLabelValues(label)
		}
	}
}

func (stor *Storage) metricsAccountWriter(volKey uint64) {
	if stor.metrics != nil {
		if stor.metrics.TotalOps != nil {
			volLabel := stor.volPath(volKey)
			stor.metrics.TotalOps.WithLabelValues(volLabel, "write").Inc()
		}
	}
}

func (stor *Storage) metricsAccountReader(volKey uint64) {
	if stor.metrics != nil {
		if stor.metrics.TotalOps != nil {
			volLabel := stor.volPath(volKey)
			stor.metrics.TotalOps.WithLabelValues(volLabel, "read").Inc()
		}
	}
}

func (stor *Storage) metricsAccountSync(volKey uint64) {
	if stor.metrics != nil {
		if stor.metrics.TotalOps != nil {
			volLabel := stor.volPath(volKey)
			stor.metrics.TotalOps.WithLabelValues(volLabel, "sync").Inc()
		}
	}
}

func (stor *Storage) metricsAccountDelete(volKey uint64) {
	if stor.metrics != nil {
		if stor.metrics.TotalOps != nil {
			volLabel := stor.volPath(volKey)
			stor.metrics.TotalOps.WithLabelValues(volLabel, "delete").Inc()
		}
	}
}

func (stor *Storage) metricsWriteLatencyObserver(volKey uint64) prom.Observer {
	if stor.metrics != nil {
		if stor.metrics.Latencies != nil {
			volLabel := stor.volPath(volKey)
			return stor.metrics.Latencies.WithLabelValues(volLabel, "write")
		}
	}
	return nil
}

func (stor *Storage) metricsWriteSizeObserver(volKey uint64) prom.Gauge {
	if stor.metrics != nil {
		if stor.metrics.Sizes != nil {
			label := stor.volPath(volKey)
			return stor.metrics.Sizes.WithLabelValues(label)
		}
	}
	return nil
}

func (stor *Storage) metricsWriteBytesCounter(volKey uint64) prom.Counter {
	if stor.metrics != nil {
		if stor.metrics.TotalWriteBytes != nil {
			label := stor.volPath(volKey)
			return stor.metrics.TotalWriteBytes.WithLabelValues(label)
		}
	}
	return nil
}

func (stor *Storage) metricsReadLatencyObserver(volKey uint64) prom.Observer {
	if stor.metrics != nil {
		if stor.metrics.Latencies != nil {
			volLabel := stor.volPath(volKey)
			return stor.metrics.Latencies.WithLabelValues(volLabel, "read")
		}
	}
	return nil
}

func (stor *Storage) metricsReadBytesCounter(volKey uint64) prom.Counter {
	if stor.metrics != nil {
		if stor.metrics.TotalReadBytes != nil {
			label := stor.volPath(volKey)
			return stor.metrics.TotalReadBytes.WithLabelValues(label)
		}
	}
	return nil
}

func (stor *Storage) metricsDeleteLatencyObserver(volKey uint64) prom.Observer {
	if stor.metrics != nil {
		if stor.metrics.Latencies != nil {
			volLabel := stor.volPath(volKey)
			return stor.metrics.Latencies.WithLabelValues(volLabel, "delete")
		}
	}
	return nil
}

func (stor *Storage) metricsSyncLatencyObserver(volKey uint64) prom.Observer {
	if stor.metrics != nil {
		if stor.metrics.Latencies != nil {
			volLabel := stor.volPath(volKey)
			return stor.metrics.Latencies.WithLabelValues(volLabel, "sync")
		}
	}
	return nil
}

func (stor *Storage) metricsAccountError(vec *prom.CounterVec, volKey uint64, op string, err error) {
	if vec == nil || err == nil {
		return
	}
	volLabel := stor.volPath(volKey)
	var errLabel string
	if s, ok := err.(fmt.Stringer); ok == true {
		errLabel = s.String()
	} else {
		errLabel = fmt.Sprintf("%T", err)
	}
	vec.WithLabelValues(volLabel, op, errLabel).Inc()
}

func (stor *Storage) metricsAccountWriteError(volKey uint64, err error) {
	if stor.metrics != nil {
		stor.metricsAccountError(stor.metrics.Errors, volKey, "write", err)
	}
}

func (stor *Storage) metricsAccountReadError(volKey uint64, err error) {
	if stor.metrics != nil {
		stor.metricsAccountError(stor.metrics.Errors, volKey, "read", err)
	}
}

func (stor *Storage) metricsAccountDeleteError(volKey uint64, err error) {
	if stor.metrics != nil {
		stor.metricsAccountError(stor.metrics.Errors, volKey, "delete", err)
	}
}

func (stor *Storage) metricsAccountSyncError(volKey uint64, err error) {
	if stor.metrics != nil {
		stor.metricsAccountError(stor.metrics.Errors, volKey, "sync", err)
	}
}
type open_co_ctx struct{
	vol  *volume.Volume
	err  error
}

// in case of error closes volume implicitelly. return ready 
func (stor *Storage) openOrCreateVolUnsafe(ctx context.Context, create bool, relpath string, id, maxSize uint64) (*volCtx, error) {
	var logg *slog.Logger = nil
	if stor.logger != nil {
		logg = stor.logger.With("vol", relpath)
	}
	
	ch := make(chan open_co_ctx, 1)
	open_co := func(name string, logg_ *slog.Logger, ch_ chan open_co_ctx) {
		var (
			vol *volume.Volume
			err error
		)
		if create {
			vol, err = volume.CreateAndOpen(relpath, id, maxSize, logg_)
		} else {
			vol, err = volume.Open(name, logg_)
		}
		ch_<- open_co_ctx{ vol, err }
		close(ch_)
	}

	go open_co(relpath, logg, ch)

	select {
	case <-ctx.Done():
		err := ctx.Err()
		return nil, err
	case res := <-ch:
		if res.err != nil {
			if res.vol != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), timeoutVolsClose)
				defer cancel()
				_ = stor.closeVolUnsafe(closeCtx, res.vol)
				return nil, res.err
			}
			return nil, res.err
		}
		_, sz := res.vol.Size()
		vol := &volCtx{
			lock: &sync.RWMutex{},
			v:    res.vol,
			wq:   list.New(),
			off:  sz,
			buf:  nil, // TODO implement
		}
		return vol, nil
	}
}

func (stor *Storage) openVolUnsafe(ctx context.Context, relpath string) (uint64, error) {
	vol, err := stor.openOrCreateVolUnsafe(ctx, false, relpath, 0, 0)
	if err != nil {
		return 0, err
	}
	stor.vol[vol.v.Header().Id] = vol
	stor.metricsAddVol(vol.v.Header().Id, vol.off)
	return vol.v.Header().Id, nil
}

func (stor *Storage) createVol(ctx context.Context, id, maxSize uint64) (uint64, error) {
	stor.volsMtx.Lock()
	defer stor.volsMtx.Unlock()

	if _, ok := stor.vol[id]; ok {
		return 0, models.NewErrExists(fmt.Sprintf("volume with id '%d' already exists", id))
	}

	relpath := stor.volPath(id)

	volctx, err := stor.openOrCreateVolUnsafe(ctx, true, relpath, id, maxSize)
	if err != nil {
		return 0, err
	}

	stor.vol[id] = volctx
	stor.metricsAddVol(volctx.v.Header().Id, volctx.off)
	return volctx.v.Header().Id, nil
}

// must be called under write locked stor.volMtx
func (stor *Storage) closeVolUnsafe(ctx context.Context, vol *volume.Volume) error {
	if vol == nil {
		return models.NewErrInvalidParams("close of nil volume.Volume")
	}
	ch := make(chan error, 1)
	close_co := func(vol_ *volume.Volume, ch_ chan error) {
		err := vol_.Close()
		ch_ <- err
		close(ch_)
	}

	go close_co(vol, ch)
	select {
	case <-ctx.Done():
		return fmt.Errorf("vol '%d' close cancelled: %w", vol.Header().Id, ctx.Err())
	case err := <- ch:
		if err != nil {
			return fmt.Errorf("failed to close vol '%d': %w", vol.Header().Id, err)
		} else {
			return nil
		}
	}
}

// must be called under write locked stor.volMtx
func (stor *Storage) closeVolCtxUnsafe(ctx context.Context, volctx *volCtx) error {
	if volctx == nil {
		return models.NewErrInvalidParams("close of nil volCtx")
	}
	volctx.lock.Lock()
	defer volctx.lock.Unlock()
	// TODO add buffers flush if any after buffering implementation

	for volctx.wq.Len() != 0 {
		volctx.notifyNext(os.ErrClosed)
	}
	// after async notification writers could delay their closes
	err := stor.closeVolUnsafe(ctx, volctx.v)

	return err
}

func (stor *Storage) closeVol(ctx context.Context, volKey uint64) error {
	stor.volsMtx.Lock()
	defer stor.volsMtx.Unlock()

	volctx, ok := stor.vol[volKey]
	if ok == false {
		return models.NewErrNotFound(fmt.Sprintf("no such volume '%d' for close", volKey))
	}
	err := stor.closeVolCtxUnsafe(ctx, volctx)
	delete(stor.vol, volKey)
	stor.metricsRemoveVol(volKey)
	return err
}

// perform volumes Close, may block for a while. call and wait close on all
// volumes but return the first occured error
func (stor *Storage) close(ctx context.Context) error {
	stor.volsMtx.Lock()
	defer stor.volsMtx.Unlock()

	if stor.logger != nil {
		stor.logger.Info("Closing volumes")
	}

	var err error = nil
	for k, vol := range stor.vol {
		_err := stor.closeVolCtxUnsafe(ctx, vol)
		if err == nil {
			err = _err
		}
		delete(stor.vol, k)
	}

	if stor.logger != nil {
		if err != nil {
			stor.logger.Info("Volumes closed with errors")
		} else {
			stor.logger.Info("Volumes successfully closed")
		}
	}

	return err
}


// -------------------------------------------------------
// --------- Implementation of models.Storage ------------
// -------------------------------------------------------

func (stor *Storage) AddVolume(ctx context.Context, name string, maxSz uint64) (uint64, error) {
	// check naming and create a path for vilume
	if name == "" {
		return 0, models.NewErrInvalidParams("volume name cannot be empty")
	}

	if strings.ContainsFunc(name, func(r rune) bool {
		return !(r >= '0' && r <= '9')
	}) {
		return 0, models.NewErrInvalidParams(fmt.Sprintf("volume name can contain only digits but it %s", name))
	}

	// actually it is the same as volKey
	id, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, models.NewErrInvalidParams(fmt.Sprintf("failed to parse volume name '%s' as uint64: %v", name, err))
	}

	volKey, err := stor.createVol(ctx, id, maxSz)

	if err != nil {
		return 0, fmt.Errorf("failed to create volume '%s': %w", name, err)
	}

	return volKey, nil
}

func (stor *Storage) RemoveVolume(ctx context.Context, volKey uint64) error {
	path := fmt.Sprintf("%s%d", volPrefix, volKey)
	err := stor.closeVol(ctx, volKey)
	if err != nil {
		return fmt.Errorf("failed to close volume '%d' for remove: %w", volKey, err)
	}
	if err := stor.root.Remove(path); err != nil {
		return fmt.Errorf("failed to remove volume file '%s': %w", path, err)
	}
	return nil
}

func (stor *Storage) ListVolumes(ctx context.Context) ([]models.Volume, error) {
	stor.volsMtx.RLock()
	defer stor.volsMtx.RUnlock()

	vols := make([]models.Volume, 0, len(stor.vol))
	for _, vol := range stor.vol {
		vol.lock.RLock()
		header := vol.v.Header()
		used, _ := vol.v.Size()
		vol.lock.RUnlock()
		vols = append(vols, models.Volume{
			Key:   header.Id,
			Space: models.VolumeSpaceUsage{
				Used: used,
				Free: header.MaxSize - used,
			},
		})
	}
	return vols, nil
}

func (stor *Storage) ListObjects(ctx context.Context, volKey uint64) ([]models.ObjInfo, error) {
	stor.volsMtx.RLock()
	defer stor.volsMtx.RUnlock()

	vol, ok := stor.vol[volKey]
	if !ok {
		return nil, models.NewErrNotFound(fmt.Sprintf("no such volume '%d' for list objects", volKey))
	}

	vol.lock.RLock()
	defer vol.lock.RUnlock()

	objs := make([]models.ObjInfo, 0)
	opts := []needle.WithOption{}
	if stor.csOn {
		// add checksum checker for scaner
		withCs := needle.WithChecksumAlg(crc64.New(crc64.MakeTable(crc64.ISO)))
		opts = append(opts, withCs)
	}
	// add readahed for better performance
	opts = append(opts, needle.WithReadahead())
	if stor.logger != nil {
		// add logger if it exist
		scanner_logger := stor.logger.With("scanner", stor.volPath(volKey))
		opts = append(opts, needle.WithLogger(scanner_logger))
	}
	// start with offset to skip superblock
	opts = append(opts, needle.WithStartOffset(uint64(vol.v.HeaderEnd())))

	// actual iterator creation
	rd := vol.v.Reader()
	it, err := needle.NewIter(ctx, rd, opts...)

	checkErr := func(err error) bool {
		return err == nil ||
			errors.Is(err, &models.ErrObjCSMismatch{}) ||
			errors.Is(err, &models.ErrObjValidation{})
	}

	if err != nil {
		if stor.logger != nil {
			stor.logger.Error("failed to start needle iter", "vol", stor.volPath(volKey), "err", err)
		}
		if err := rd.Close(); err != nil && stor.logger != nil {
			stor.logger.Error("failed to close volume read fd", "vol", stor.volPath(volKey), "err", err)
		}
		return nil, fmt.Errorf("failed to list objects in volume '%s': %w", stor.volPath(volKey), err)
	}

	for h, err := it.Next(ctx); checkErr(err); h, err = it.Next(ctx) {
		if h == nil {
			if stor.logger != nil {
				stor.logger.Error("failed to read needle header", "vol", stor.volPath(volKey), "err", err)
			}
			continue
		}
		obj := models.ObjInfo{
			Key: h.Key,
			MetaSize: 0, // TODO
			DataSize: h.DataSize,
			Offset: it.Offset(),
		}
		if errors.Is(err, &models.ErrObjCSMismatch{}) {
			obj.Flags.CsMismatched = true
		} else if err != nil { // only ErrObjValidation case
			// hard to deside what to do with validation errors
			// for now just return with CsMismatched so user can decide what to do with it
			obj.Flags.CsMismatched = true
		}
		obj.Flags.Deleted = needle.FlagsDeleted(h.Flags)
		objs = append(objs, obj)
	}

	err = it.Close() // always nil in current impl
	err = rd.Close()
	if stor.logger != nil {
		stor.logger.Error("failed to close volume read fd", "vol", stor.volPath(volKey), "err", err)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to close iter over objects in volume '%s': %w", stor.volPath(volKey), err)
	}

	return objs, nil
}

func (stor *Storage) Stats(ctx context.Context) *models.StorageStats {
	ss := &models.StorageStats{}
	stor.volsMtx.RLock()
	defer stor.volsMtx.RUnlock()

	for _, vol := range stor.vol {
		vol.lock.RLock()
		used, _ := vol.v.Size()
		ss.Volumes = append(ss.Volumes, models.VolumeStat{
			Info: models.Volume{
				Key: vol.v.Header().Id,
				Space: models.VolumeSpaceUsage{
					Used: used,
					Free: vol.v.Header().MaxSize - used,
				},
			},
			ObjectsCount: 0, // we do not track it
			PendingReads: 0, // reads are never pending
			RunningReads: 0, // TODO track
			PendingWrites: vol.wq.Len(),
			PendingKiB: int(vol.off - used)/1024,
		})
		ss.PendingDeletes += 0 // TODO track
		ss.PendingReads = 0 // reads are never pending
		ss.PendingWrites += vol.wq.Len()
		if stor.buffering == 0 {
			ss.WriteBufferSzs += int(vol.off - used)/1024 // just report pending writes data size
		} else {
			// TODO
		}
		vol.lock.RUnlock()
	}
	return ss
}

func (stor *Storage) CompactVolume(ctx context.Context, fromKey, toKey uint64) error {
	if stor.logger != nil {
		stor.logger.Error("CompactVolume() is not implemented yet")
	}
	return &models.ErrUnimplemented{}
}

func (stor *Storage) PutObjectWriter(volKey, objKey, dataSize uint64) (io.WriteCloser, uint64, error) {
	stor.volsMtx.RLock()
	defer stor.volsMtx.RUnlock()

	var (
		vol *volCtx
		ok  bool
	)
	if vol, ok = stor.vol[volKey]; ok == false {
		return nil, 0, models.NewErrNotFound(fmt.Sprintf("No volume with key '%d'", volKey))
	}

	vol.lock.Lock()
	defer vol.lock.Unlock()

	maxSz := vol.v.Header().MaxSize
	if maxSz - vol.off < needle.CalcNeedleSize(dataSize) {
		return nil, 0, io.EOF
	}
	
	var cs hash.Hash64 = nil
	if stor.csOn {
		cs = crc64.New(crc64.MakeTable(crc64.ISO))
	}

	flags := needle.DefaultFlags
	needleFd, objSz, err := needle.NewWriter(vol.v.Writer(), objKey, flags, dataSize, cs)

	if err != nil {
		if stor.logger != nil {
			stor.logger.Error("failed to create needle writer for PutObjectWriter", "objKey", objKey, 
			                  "vol", stor.volPath(volKey),"err", err)
		}
		return nil, 0, fmt.Errorf("failed to create needle.Writer: %w", err)
	}

	w := vol.enqueueSelf(objSz)
	stor.metricsAccountWriter(volKey)
	var logger *slog.Logger = nil
	if stor.logger != nil {
		logger = stor.logger.With("needleWriter", objKey)
	}
	return &objWriter{
		vol: vol,
		fd: needleFd,
		w: w,
		logger: logger,
		mLat: stor.metricsWriteLatencyObserver(volKey),
		mSz:  stor.metricsWriteSizeObserver(volKey),
		mWr:  stor.metricsWriteBytesCounter(volKey),
		stor: stor,
	}, vol.off - objSz, nil
}

type objWriter struct {
	vol       *volCtx
	fd        io.WriteCloser
	notified  bool
	fdClosed  bool      // fd already closed via auto-close on fatal Write error
	w         *waiter
	logger    *slog.Logger
	mLat      prom.Observer
	mWr       prom.Counter // bytes written
	mSz       prom.Gauge
	stor      *Storage // ugly, need only for prometheus error counter
	timeSt    time.Time
}

// notifyAndCloseFd handles the write-queue notification and closes the needle
// writer (releasing the VolumeWriter refcnt).  Idempotent: safe to call from
// both Write (on fatal error) and Close.
func (ow *objWriter) notifyAndCloseFd(err error) error {
	if !ow.notified {
		ow.vol.notifyNext(err)
		ow.notified = true
	}
	if ow.fdClosed {
		return nil
	}
	ow.fdClosed = true
	cerr := ow.fd.Close()
	if cerr != nil && ow.logger != nil {
		ow.logger.Error("Close", "err", cerr)
	}
	ow.stor.metricsAccountWriteError(ow.vol.v.Header().Id, cerr)
	return cerr
}

func (ow *objWriter) Write(b []byte) (int, error) {
	// wait when previous queued writes will complete
	if ow.w != nil {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutWriteWaiter)
		defer cancel()
		err := ow.w.Wait(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, err
		}
		ow.w = nil
		ow.timeSt = time.Now()
	}
	// RLock is sufficient here: write ordering is guaranteed by the volume's
	// internal wrLock and the wait-queue mechanism (enqueueSelf/notifyNext).
	// We only need to prevent concurrent RemoveVolume/Close which take Lock().
	ow.vol.lock.RLock()
	defer ow.vol.lock.RUnlock()

	n, err := ow.fd.Write(b)

	if n != 0 {
		if ow.mSz != nil {
			ow.mSz.Add(float64(n))
		}
		if ow.mWr != nil {
			ow.mWr.Add(float64(n))
		}
	}
	if err != nil {
		if err == io.EOF {
			if ow.mLat != nil {
				ow.mLat.Observe(float64(time.Since(ow.timeSt).Milliseconds()))
			}
			ow.mLat = nil
			if !ow.notified {
				ow.vol.notifyNext(nil)
				ow.notified = true
			}
			return n, err
		}
		if ow.logger != nil {
			ow.logger.Error("Write", "err", err)
		}
		ow.stor.metricsAccountWriteError(ow.vol.v.Header().Id, err)
		// Unrecoverable write error: notify the queue and auto-close the
		// descriptor so the caller is not required to call Close() to avoid
		// a VolumeWriter leak.
		_ = ow.notifyAndCloseFd(err)
		return n, fmt.Errorf("failed to write needle Writer: %w", err)
	}

	return n, err
}

func (ow *objWriter) Close() error {
	ow.vol.lock.Lock()
	defer ow.vol.lock.Unlock()
	if ow.mLat != nil {
		ow.mLat.Observe(float64(time.Since(ow.timeSt).Milliseconds()))
	}
	err := ow.notifyAndCloseFd(nil)
	if err != nil {
		return fmt.Errorf("failed to close needle Writer: %w", err)
	}

	if ow.vol.buf != nil {
		// TODO
		// for buffered write should enqueue here to some wq like for write and imediatelly wait
		// to optimize writes of short objects (like less then a 100Kb) since volume sync for them
		// is often slower then write due to page cache
	} else {
		// for unbuffered writes just sync on every obj write completion
		timeSt := time.Now()
		err = ow.vol.v.Sync()
		ow.stor.metricsAccountSync(ow.vol.v.Header().Id)
		if err != nil {
			ow.stor.metricsAccountSyncError(ow.vol.v.Header().Id, err)
		} else {
			if obs := ow.stor.metricsSyncLatencyObserver(ow.vol.v.Header().Id); obs != nil {
				obs.Observe(float64(time.Since(timeSt).Milliseconds()))
			}
		}
	}

	return err
}

func (stor *Storage) GetObjectReader(volKey, objKey, off uint64) (io.ReadCloser, error) {
	stor.volsMtx.RLock()
	defer stor.volsMtx.RUnlock()

	var (
		vol *volCtx
		ok  bool
	)
	if vol, ok = stor.vol[volKey]; ok == false {
		return nil, models.NewErrNotFound(fmt.Sprintf("No volume with key '%d'", volKey))
	}

	// we will not modify anything except volume, but volume is thread safe itself so just lock for read here
	vol.lock.RLock()
	defer vol.lock.RUnlock()

	_, curSynced := vol.v.Size()
	if curSynced < needle.DataShift || curSynced - needle.DataShift < off {
		return nil, models.NewErrInvalidParams(fmt.Sprintf("cannot read unsynced data after '%d'", curSynced))
	}

	var cs hash.Hash64 = nil
	if stor.csOn {
		cs = crc64.New(crc64.MakeTable(crc64.ISO))
	}

	timeSt := time.Now()
	// implicitly read the header
	needleFd, err := needle.NewReader(vol.v.Reader(), off, cs)

	if err != nil {
		return nil, fmt.Errorf("failed to create needle Reader: %w", err)
	}

	h := needleFd.Header()
	if h.Key != objKey {
		if stor.logger != nil {
			stor.logger.Error("Invalid obj key", "got", objKey, "want", h.Key)
		}
		_ = needleFd.Close()
		return nil, models.NewErrInvalidParams(fmt.Sprintf("obj key mismatch with ondisk value: got '%d' want '%d'", objKey, h.Key))
	}

	if needle.FlagsDeleted(h.Flags) {
		if stor.logger != nil {
			stor.logger.Error("Deleted object", "key", objKey)
		}
		_ = needleFd.Close()
		return nil, models.NewErrNotFound(fmt.Sprintf("obj '%d' is deleted", objKey))
	}

	var logg *slog.Logger = nil
	if stor.logger != nil {
		logg = stor.logger.With("needleReader", objKey)
	}
	stor.metricsAccountReader(volKey)
	or := &objReader{
		vol: vol,
		fd:  needleFd,
		logger: logg,
		objKey: objKey,
		mLat: stor.metricsReadLatencyObserver(volKey),
		mRd:  stor.metricsReadBytesCounter(volKey),
		stor: stor,
		timeSt: timeSt,
	}

	return or, nil
}

type objReader struct {
	vol       *volCtx
	fd        io.ReadCloser
	objKey    uint64  // stored for onflight validation
	logger    *slog.Logger
	mLat      prom.Observer
	mRd       prom.Counter // bytes read
	stor      *Storage // ugly, need only for prometheus error counter
	timeSt    time.Time
	closeOnce sync.Once // ensures fd is closed exactly once
}

// closeFd releases the underlying needle.Reader (and its VolumeReader) exactly
// once.  Safe to call concurrently from Read and Close.
func (or *objReader) closeFd() error {
	var err error
	or.closeOnce.Do(func() {
		err = or.fd.Close()
		if err != nil && or.logger != nil {
			or.logger.Error("Close", "err", err)
		}
		or.stor.metricsAccountReadError(or.vol.v.Header().Id, err)
	})
	return err
}

func (or *objReader) Read(b []byte) (int, error) {
	or.vol.lock.RLock()
	defer or.vol.lock.RUnlock()

	n, err := or.fd.Read(b)

	if n != 0 {
		if or.mRd != nil {
			or.mRd.Add(float64(n))
		}
	}
	if err != nil {
		if err == io.EOF {
			if or.mLat != nil {
				or.mLat.Observe(float64(time.Since(or.timeSt).Milliseconds()))
			}
			or.mLat = nil
			return n, err
		}
		if or.logger != nil {
			or.logger.Error("Read", "err", err)
		}
		or.stor.metricsAccountReadError(or.vol.v.Header().Id, err)
		// Unrecoverable read error: auto-release the VolumeReader so the caller
		// is not required to call Close() to avoid a descriptor leak.
		_ = or.closeFd()
		return n, fmt.Errorf("failed to read needle Reader: %w", err)
	}

	return n, err
}

func (or *objReader) Close() error {
	or.vol.lock.RLock()
	defer or.vol.lock.RUnlock()
	if or.mLat != nil {
		or.mLat.Observe(float64(time.Since(or.timeSt).Milliseconds()))
	}
	return or.closeFd()
}

func (stor *Storage) Close(ctx context.Context) error {
	if stor == nil {
		return nil
	}
	return stor.close(ctx)
}

// in current implementation we actually dont need objKey here but it is 
// needed for storage interface and probably will be used in future
func (stor *Storage) MarkDeleteObject(ctx context.Context, volKey, objKey, off uint64) error {
	var (
		vol  *volCtx
		ok   bool
	)
	stor.volsMtx.RLock()
	defer stor.volsMtx.RUnlock()
	if vol, ok = stor.vol[volKey]; ok == false {
		return models.NewErrNotFound(fmt.Sprintf("No volume with key '%d'", volKey))
	}

	vol.lock.Lock()
	defer vol.lock.Unlock()

	_, szSynced := vol.v.Size()
	if szSynced < needle.DataShift || szSynced - needle.DataShift < off {
		return models.NewErrInvalidParams(fmt.Sprintf("Cannot write unsynced data after '%d'", szSynced))
	}

	fd := vol.v.Rewriter()
	flags := needle.DefaultFlags

	timeSt := time.Now()
	err := needle.MarkDeleted(fd, flags, off)
	if err != nil {
		stor.metricsAccountDeleteError(volKey, err)
		return fmt.Errorf("failed to mark obj as deleted: %w", err)
	}
	stor.metricsAccountDelete(volKey)
	if obs := stor.metricsDeleteLatencyObserver(volKey); obs != nil {
		obs.Observe(float64(time.Since(timeSt).Milliseconds()))
	}

	timeSt = time.Now()
	err = vol.v.Sync()
	stor.metricsAccountSync(volKey)
	if err != nil {
		stor.metricsAccountSyncError(volKey, err)
		return fmt.Errorf("failed to sync delete marker: %w", err)
	} else {
		if obs := stor.metricsSyncLatencyObserver(volKey); obs != nil {
			obs.Observe(float64(time.Since(timeSt).Milliseconds()))
		}
	}
	return nil
}

func NewDefaultStorageMetrics() *models.StorageMetrics {
	m := &models.StorageMetrics{
		TotalOps: prom.NewCounterVec(prom.CounterOpts{
				Subsystem: "storage",
				Name: "total_ops",
				Help: "total number of io operations over volumes",
			}, []string{ "volKey", "io" }),
		TotalReadBytes: prom.NewCounterVec(prom.CounterOpts{
				Subsystem: "storage",
				Name: "read_bytes",
				Help: "total bytes read from all volumes",
			}, []string{ "volKey"}),
		TotalWriteBytes: prom.NewCounterVec(prom.CounterOpts{
			Subsystem: "storage",
			Name: "written_bytes",
			Help: "total bytes written from all volumes",
		}, []string{ "volKey" }),
		Errors: prom.NewCounterVec(prom.CounterOpts{
			Subsystem: "storage",
			Name: "io_errors",
			Help: "all errors encountered during read, writes, syncs and deletes",
		}, []string{ "volKey", "io", "error" }),
		Sizes: prom.NewGaugeVec(prom.GaugeOpts{
			Subsystem: "storage",
			Name: "vol_sizes",
			Help: "volume sizes current value",
		}, []string{ "volKey" }),
		Compaction: prom.NewGaugeVec(prom.GaugeOpts{
			Subsystem: "storage",
			Name: "compaction",
			Help: "0-1 table indexed by <volId> and compation direction (\"from\" and \"to\")",
		}, []string{ "volKey", "direction" }),
	}
	buckets := prom.ExponentialBuckets(0.1, 1.2, 60)
	m.Latencies = prom.NewHistogramVec(prom.HistogramOpts{
		Subsystem: "storage",
		Name: "io_latency",
		Help: "latencies of read, writes, syncs and deletes, if there was no io errors",
		Buckets: buckets,
	}, []string{ "volKey", "io" })
	return m
}
