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

type VolumeStat struct {
	MaxSize          uint64 // in bytes
	Free             uint64 // in bytes
	PendingReads     int
	RunningReads     int
	PendingWrites    int
	PendingKiB       int // pending writes total size
}
type StorageStats struct {
	PendingWrites    int
	PendingReads     int
	PendingDeletes   int
	RunningReads     int
	Volumes          []VolumeStat
	WriteBufferSzs   int // volumes have own
}

type StorageMetrics struct {
	TotalWrites      *prom.Counter
	TotalReads       *prom.Counter

	ReadErrors       *prom.CounterVec // labels: {<error>}
	WriteErrors      *prom.CounterVec // labels: {<error>}

	Latencies        *prom.HistogramVec // labels: {<volumeKey>, [read|write|delete]}
	Sizes            *prom.CounterVec // labels: {<loumeKey>}
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
	Stats(ctx context.Context) StorageStats

	// Deletes the whole volume and releasing memory it used
	RemoveVolume(ctx context.Context, volKey uint64) error

	// Moves volume needles from @fromKey volume to @toKey volume, skipping deleted needles.
	//
	// TODO returns reference on struct with progress counters (atomics or prom)
	CompactVolume(ctx context.Context, fromKey, toKey uint64)

	// PutObjectWriter creates io.WriteCloser to which object data must be written.
	// You must write exactly dataSize bytes to it, otherwise it neither io.UnexepectedEOF or 
	// , when io closed, Close() returns ErrValidation
	//
	// Second returned value is offset of future needle. It is an ErrValidation to call
	// GetObjectReader or MarkDeleteObject on object, which io was not closed yet.
	// 
	// For both PutObjectWriter Close should be offloaded 
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
	PutObjectWriter(volKey, objKey, dataSize uint64) (*io.WriteCloser, uint64, error)

	// GetObjectReader creates io.ReadCloser from which object data can be read
	// data can be read until io.EOF or until io.Close() called.
	// User should remember size of the object himself.
	GetObjectReader(volKey, objKey, off uint64) (*io.ReadCloser, error)

	// Mark object as deleted. To actually remove CompactVolume or RemoveVolume must be called on this volume
	MarkDeleteObject(ctx context.Context, volKey, objKey, off uint64) (error)

	// if setted storage will report it state to them
	SetMetrics(metrics *StorageMetrics)

	// Close all volumes, flushing all buffers and free inmem state
	// all references provided by interface must not be used after Close.
	Close() error
}