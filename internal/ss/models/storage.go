package models

import (
	"context"
	"io"

	prom "github.com/prometheus/client_golang/prometheus"
)

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type WriteAtCloser interface {
	io.WriterAt
	io.Closer
}

type VolumeSpaceUsage struct {
	Free             uint64  // space that can be written by new objects
	Used             uint64  // space used by all objects
}

type Volume struct {
	Path             string  // relative to SS work dir
	Space            VolumeSpaceUsage
}

type VolumeStat struct {
	Info             Volume
	ObjectsCount     uint
	PendingReads     int
	RunningReads     int
	PendingWrites    int
	PendingKiB       int // pending writes total size
}

type ObjFlags struct {
	Deleted          bool
	CsMismatched     bool
	// Others will be added as needed
}

type ObjInfo struct {
	Key              uint64
	Flags            ObjFlags
	MetaSize         uint64
	DataSize         uint64
	Offset           uint64
}

type StorageStats struct {
	PendingWrites    int
	PendingReads     int
	PendingDeletes   int
	RunningReads     int
	Volumes          []VolumeStat
	WriteBufferSzs   int
}

type StorageMetrics struct {
	TotalOps         *prom.CounterVec // labels: {<volumeKey>, [read|write|delete|datasync]} count

	TotalReadBytes   *prom.CounterVec // labels: {<volumeKey>} in bytes 
	TotalWriteBytes  *prom.CounterVec // labels: {<volumeKey>} in bytes 

	Errors           *prom.CounterVec // labels: {<volumeKey>, [read|write|delete|datasync], <error>}

	Latencies        *prom.HistogramVec // labels: {<volumeKey>, [read|write|delete|datasync]} in ms
	Sizes            *prom.GaugeVec // labels: {<volumeKey>} in bytes

	Compaction       *prom.GaugeVec // 0-1 state; labels: {<volumeKey>, [from|to]}
}
/*
Storage provides abstraction to work with storage as a set of opened volumes.
All operations are synchronous.
Storage provides its statistics and metrics for observability and logging.
Storage provides put and get descriptors for user asynchronous read and write
and implementation guarantee read-after-write consistancy and that reads and writes 
from several threads are not overlap
Methods return os errors, ErrValidation or io errors

Volume Api is also proviede for user in purpose of future offline
compaction and SS split implementation

See storage implementation for recomendation of batching io stream
*/
type Storage interface {
	// Storage must return volume key when creating it.
	// @maxSz is max allowed size for volume.
	// @name is path (relative or absolute depend on implementation)
	// of volume.
	AddVolume(ctx context.Context, name string, maxSz uint64) (uint64, error)

	// Return current Storage state, see StorageStats
	Stats(ctx context.Context) *StorageStats

	// Deletes the whole volume and releasing memory it used
	RemoveVolume(ctx context.Context, volKey uint64) error

	// return currently used volumes list, see Volume
	ListVolumes(ctx context.Context) ([]Volume, error)

	// return objects meta info for specified volume, see ObjMeta
	ListObjects(ctx context.Context, volKey uint64) ([]ObjInfo, error)

	// Moves volume needles from @fromKey volume to @toKey volume, skipping deleted needles.
	//
	// TODO returns reference on struct with progress counters (atomics or prom)
	CompactVolume(ctx context.Context, fromKey, toKey uint64) error

	// PutObjectWriter creates io.WriteCloser to which object data must be written.
	// You must write exactly dataSize bytes to it, otherwise it neither io.UnexepectedEOF or 
	// , when io closed, Close() returns ErrValidation
	//
	// Second returned value is offset of this object start in data stream. It is an ErrValidation to call
	// GetObjectReader or MarkDeleteObject on object, which io was not closed yet.
	// 
	// For PutObjectWriter Close should be offloaded 
	// (since it make block for a lot of time) to coroutine like that
	//
	//	io, off, err := PutObjectWriter(...)
	//	...
	//	ch := make(chan error, 1)
	//	go func(){ err := io.Close(); ch<- err; close(ch) }()
	//	select {
	//	case <-ctx.Done():
	//		...
	//	case err := <-ch:
	//		...
	//	}
	//
	// On errored Close() data is in undefined state and full object write should be retried.
	// Storage must provide retry mechanism for this case, but user should be ready to do it by himself. 
	// On successfull Close() data must be fully written, synced and can be read by GetObjectReader.
	PutObjectWriter(volKey, objKey, dataSize uint64) (io.WriteCloser, uint64, error)

	// GetObjectReader creates io.ReadCloser from which object data can be read
	// data can be read until io.EOF or until io.Close() called.
	// User should remember the size of requested object himself.
	GetObjectReader(volKey, objKey, off uint64) (io.ReadCloser, error)

	// Mark object as deleted. To actually remove CompactVolume or RemoveVolume must be called on this volume
	MarkDeleteObject(ctx context.Context, volKey, objKey, off uint64) (error)

	// Close all volumes, flushing all buffers and free inmem state
	// all references provided by interface must not be used after Close.
	Close(ctx context.Context) error
}