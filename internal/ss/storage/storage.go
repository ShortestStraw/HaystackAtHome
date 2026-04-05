package storage

import (
	"HaystackAtHome/internal/ss/models"
	"HaystackAtHome/internal/ss/storage/volume"
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"strconv"
	"strings"
	"sync"
	"time"

	"HaystackAtHome/internal/ss/storage/needle"
	"bytes"
	"io/fs"
	"log/slog"
	"os"
	// prom "github.com/prometheus/client_golang/prometheus"
)

const (
	volPrefix = ".volume."
	timeoutVolsClose = 30 * time.Second
)

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
	since reading and iterating over index may be paralleled with consequetive maps merge
*/
type Storage struct {
	root     *os.Root
	logger   *slog.Logger
	metrics  *models.StorageMetrics
	CsOn     bool

	bufs struct {
		vol    map[uint64]bytes.Buffer  // indexed by volume Keys
		sz     uint64   // if set to 0 then buffering is off
	}

	volsMtx  *sync.RWMutex // for addition and deletion elemnts from vol and volLock maps
	                       // for modification of volumes can be read locked but volLock for each 
												 // volume should be used write locked for writes
	vol      map[uint64]*volume.Volume
	volLock  map[uint64]*sync.RWMutex // must allways be used only after volsMtx is locked for read or write
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
		stor.CsOn = true
	}
}

func WithVolumeWriteBuffering(bufSize uint64) Option {
	return func(stor *Storage) {
		stor.bufs.sz = bufSize
	}
}

func validateOpts(stor *Storage) error {
	_ = stor
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
		return nil, fmt.Errorf("failed to open storage root: %v", err)
	}

	stor := &Storage{
		root: root,
		volsMtx: &sync.RWMutex{},
	}

	for _, opt := range opts {
		opt(stor)
	}

	if err := validateOpts(stor); err != nil {
		_ = stor.root.Close()
		return nil, fmt.Errorf("params validation failed: %v", err)
	}

	enrties, err := fs.ReadDir(stor.root.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("failed to list files in storage root: %v", err)
	}

	for _, e := range enrties {
		if e.Type().IsRegular() && strings.HasPrefix(e.Name(), volPrefix) {
			// since there cannot be concurrent users of this structure we allowed to call unsafe methods 
			_, err := stor.openVolUnsafe(ctx, e.Name())
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
				return nil, fmt.Errorf("failed to open vol '%s': %v", e.Name(), err)
			}
		}
	}
	return stor, nil
}

// absolute or relative to cwd
func (stor *Storage) volPath(volKey uint64) string {
	return stor.root.Name() + "/" + fmt.Sprintf("%s%d", volPrefix, volKey)
}

func (stor *Storage) metricsAddVol(key, size uint64) {
	if stor.metrics != nil {
		if stor.metrics.Sizes != nil {
			label := fmt.Sprint(key)
			stor.metrics.Sizes.WithLabelValues(label).Set(float64(size))
		}
	}
}

func (stor *Storage) metricsRemoveVol(key uint64) {
	if stor.metrics != nil {
		if stor.metrics.Sizes != nil {
			label := fmt.Sprint(key)
			stor.metrics.Sizes.DeleteLabelValues(label)
		}
	}
}

type open_co_ctx struct{
	vol  *volume.Volume
	err  error
}

// in case of error closes volume implicitelly
func (stor *Storage) openOrCreateVolUnsafe(ctx context.Context, create bool, relpath string, id, maxSize uint64) (uint64, error) {
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
		return 0, err
	case res := <-ch:
		if res.err != nil {
			if res.vol != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), timeoutVolsClose)
				defer cancel()
				res.err = stor.closeVolUnsafe(closeCtx, res.vol)
				return 0, res.err
			}
		}
		stor.vol[res.vol.Header().Id] = res.vol
		_, sz := res.vol.Size()
		stor.metricsAddVol(res.vol.Header().Id, sz)
		return res.vol.Header().Id, nil
	}
}

func (stor *Storage) openVolUnsafe(ctx context.Context, relpath string) (uint64, error) {
	return stor.openOrCreateVolUnsafe(ctx, false, relpath, 0, 0)
}

func (stor *Storage) openVol(ctx context.Context, relpath string) (uint64, error) {
	stor.volsMtx.Lock()
	defer stor.volsMtx.Unlock()

	k, err := stor.openVolUnsafe(ctx, relpath)
	if err != nil {
		return 0, err
	}
	stor.volLock[k] = &sync.RWMutex{}
	return k, nil
}

func (stor *Storage) createVol(ctx context.Context, id, maxSize uint64) (uint64, error) {
	stor.volsMtx.Lock()
	defer stor.volsMtx.Unlock()

	if _, ok := stor.vol[id]; ok {
		return 0, models.NewErrInvalidParams(fmt.Sprintf("volume with id '%d' already exists", id))
	}

	relpath := stor.volPath(id)

	k, err := stor.openOrCreateVolUnsafe(ctx, true, relpath, id, maxSize)

	if err != nil {
		return 0, err
	}
	stor.volLock[k] = &sync.RWMutex{}
	return k, nil
}

// must be called under write locked stor.volMtx
func (stor *Storage) closeVolUnsafe(ctx context.Context, vol *volume.Volume) error {
	ch := make(chan error, 1)
	close_co := func(vol_ *volume.Volume, ch_ chan error) {
		err := vol_.Close()
		ch_ <- err
		close(ch_)
	}

	go close_co(vol, ch)
	select {
	case <-ctx.Done():
		return fmt.Errorf("vol '%d' close cancelled: %v", vol.Header().Id, ctx.Err())
	case err := <- ch:
		if err != nil {
			return fmt.Errorf("failed to close vol '%d': %v", vol.Header().Id, err)
		} else {
			return nil
		}
	}
}

func (stor *Storage) closeVol(ctx context.Context, volKey uint64) error {
	stor.volsMtx.Lock()
	defer stor.volsMtx.Unlock()
	
	// TODO add buffers flush if any after buffering implementation

	vol, ok := stor.vol[volKey] 
	if ok == false {
		return models.NewErrInvalidParams(fmt.Sprintf("no such volume '%d' for close", volKey))
	}
	err := stor.closeVolUnsafe(ctx, vol)
	delete(stor.vol, volKey)
	delete(stor.volLock, volKey)
	stor.metricsRemoveVol(volKey)
	return err
}

// perform volumes Close, may block for a while. call and wait close on all
// volumes but return the first occured error
func (stor *Storage) close(ctx context.Context) error {
	if stor.logger != nil {
		stor.logger.Info("Closing volumes")
	}

	stor.volsMtx.Lock()
	defer stor.volsMtx.Unlock()
	var err error = nil
	for k, vol := range stor.vol {
		_err := stor.closeVolUnsafe(ctx, vol)
		if err != nil {
			err = _err
		} else {
			delete(stor.vol, k)
			delete(stor.volLock, k)
		}
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
		return 0, fmt.Errorf("failed to create volume '%s': %v", name, err)
	}

	return volKey, nil
}

func (stor *Storage) RemoveVolume(ctx context.Context, volKey uint64) error {
	path := stor.volPath(volKey)
	err := stor.closeVol(ctx, volKey)
	if err != nil {
		return fmt.Errorf("failed to close volume '%d' for remove: %v", volKey, err)
	}
	if err := stor.root.Remove(path); err != nil {
		return fmt.Errorf("failed to remove volume file '%s': %v", path, err)
	}
	return nil
}

func (stor *Storage) ListVolumes(ctx context.Context) ([]models.Volume, error) {
	stor.volsMtx.RLock()
	defer stor.volsMtx.RUnlock()

	vols := make([]models.Volume, 0, len(stor.vol))
	for _, vol := range stor.vol {
		header := vol.Header()
		used, _ := vol.Size()
		vols = append(vols, models.Volume{
			Path: stor.volPath(header.Id),
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

	volLock := stor.volLock[volKey]
	volLock.RLock()
	defer volLock.RUnlock()
	vol, ok := stor.vol[volKey]
	if !ok {
		return nil, models.NewErrInvalidParams(fmt.Sprintf("no such volume '%d' for list objects", volKey))
	}

	objs := make([]models.ObjInfo, 0)
	opts := []needle.WithOption{}
	if stor.CsOn {
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
	opts = append(opts, needle.WithStartOffset(uint64(vol.HeaderEnd())))

	// actual iterator creation
	it, err := needle.NewIter(ctx, vol.Reader(), opts...)

	checkErr := func(err error) bool {
		target := &models.ErrObjValidation{}
		return err == nil || 
		       errors.Is(err, &models.ErrObjCSMismatch{}) || 
					 errors.As(err, &target)
	}

	if err != nil {
		if stor.logger != nil {
			stor.logger.Error("failed to start needle iter", "vol", stor.volPath(volKey), "err", err)
		}
		return nil, fmt.Errorf("failed to list objects in volume '%s': %v", stor.volPath(volKey), err)
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

	err = it.Close()

	if err != nil {
		return nil, fmt.Errorf("failed to close iter over objects in volume '%s': %v", stor.volPath(volKey), err)
	}

	return objs, nil
}