// Volume package implements Volume structures (Volume, VolumeHeader) and io interface
// Volume is the IO interface that SS storage use to communicate with filesystem
package volume

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lunixbochs/struc"
)

type HeaderValidationError struct {
	msg  string
}

func (e *HeaderValidationError) Error() string {
	return e.msg
}

const (
	version1 = 1

	headerMagic uint64 = 0xE59340401F5EE90B
	currentVersion = version1

	volumeHeaderOndiskSize int = 40 // in bytes
	maxPendingReads = 128
)

// Storage service volume.$i ondisk header. Big endian. Must be @volumeHeaderOndiskSize bytes long in raw format
type volumeHeaderOndisk struct {
	Magic     uint64     `struc:"uint64,big"`
	Id        uint64     `struc:"uint64,big"`
	MaxSize   uint64     `struc:"uint64,big"`
	Version   uint32     `struc:"uint32,big"`
	Reserved  [3]uint32  `struc:"[3]uint32,big"`
}

func validateVolumeHeader(header *volumeHeaderOndisk) error {
	hdrOndiskSzActual, err := struc.Sizeof(header)
	if err != nil {
		return &HeaderValidationError{ 
			msg: fmt.Sprintf("header proto error: %s", err.Error()),
		}
	}

	if hdrOndiskSzActual != volumeHeaderOndiskSize {
		return &HeaderValidationError{ 
			msg: fmt.Sprintf("header proto size mismatched: expected '%d', got '%d'", volumeHeaderOndiskSize, hdrOndiskSzActual),
		}
	}

	if header.Magic != headerMagic {
		return &HeaderValidationError{ 
			msg: fmt.Sprintf("Magic number differ, expected '%x', got '%x'", headerMagic, header.Magic),
		}
	}

	if header.Version < version1 && header.Version > currentVersion {
		return &HeaderValidationError{ 
			msg: fmt.Sprintf("Unsupported header version '%d", header.Version),
		}
	}

	return nil
}

// Storage service volume.$i header in memory representation
type VolumeHeader struct {
	Id        uint64
	MaxSize   uint64
	Version   uint32
}

func volumeHeaderFrom(headerOndisk *volumeHeaderOndisk) (header VolumeHeader) {
	return VolumeHeader{
		Id:        headerOndisk.Id,
		MaxSize:   headerOndisk.MaxSize,
		Version:   headerOndisk.Version,
	}
}

// Volume represents ondisk volume structure. It does not know anysing about
// objects encoding, it used to share static volume info and do reads and writes.
// It can produce and share descriptors for read and single for append.
// Volume cannot be descruct if any of VolumeDescriptor is in use.
// Thread safe. 
type Volume struct {
	header       VolumeHeader  // immutable after init

	io           *os.File  // actual fd
	
	wrLock        *sync.Mutex  // used to sync writes, protect @cursor and @sync_offset
	cursor        atomic.Uint64  // file offset (since we only append it is file size + write page cache)
	sync_offset   uint64  // file offset at which os.File.Sync() was called last time
	last_sync_ms  time.Time  // last time when sync was called

	refcnt       *sync.WaitGroup // semaphore that Close() wait empty chan for termination
	close_st     bool

	logger       slog.Logger
}

// Open existing volume and returns new *Volume instance with opened underlying volume's 
// file descriptor, inited io and filled volume header. Open does not modify logger settings. 
// Return os.Error on os.file errors and HeaderValidationError on volume header validation fail 
func Open(path string, logger *slog.Logger) (*Volume, error) {
	flags := os.O_RDWR | os.O_EXCL

	io, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("Failed to open '%s': %w", path, err)
	}

	vol := &Volume{
		io: io,
		logger: *logger,
		wrLock: &sync.Mutex{},
		refcnt: &sync.WaitGroup{},
	}

	headerOndisk := &volumeHeaderOndisk{}
	if err = struc.Unpack(vol.io, headerOndisk); err != nil {
		_ = io.Close()
		return nil, fmt.Errorf("Failed to decode VolumeHeader '%s': %w", path, err)
	}

	if err = validateVolumeHeader(headerOndisk); err != nil {
		_ = io.Close()
		return nil, fmt.Errorf("Falied validation '%s': %w", path, err)
	}

	vol.header = volumeHeaderFrom(headerOndisk)
	
	stat, err := vol.io.Stat()
	if err != nil {
		_ = io.Close()
		return nil, fmt.Errorf("Failed to decode VolumeHeader '%s': %w", path, err)
	}

	vol.cursor = atomic.Uint64{}
	vol.cursor.Store(uint64(stat.Size()))
	vol.sync_offset = uint64(stat.Size())
	vol.last_sync_ms = time.Now()

	vol.logger.Info("Volume opened", "header", vol.header)

	return vol, nil
}

// Creates new volume file, filled its header and return new *Volume instance 
func CreateAndOpen(path string, id uint64, maxSize uint64, logger *slog.Logger) (*Volume, error) {
	flags := os.O_RDWR | os.O_EXCL | os.O_CREATE

	io, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("Failed to open '%s': %w", path, err)
	}

	vol := &Volume{
		io: io,
		logger: *logger,
		wrLock: &sync.Mutex{},
		refcnt: &sync.WaitGroup{},
	}

	headerOndisk := &volumeHeaderOndisk{
		Magic: headerMagic,
		Version: currentVersion,
		Id: id,
		MaxSize: maxSize,
		Reserved: [3]uint32{0, 0, 0},
	}

	if err = validateVolumeHeader(headerOndisk); err != nil {
		_ = io.Close()
		return nil, fmt.Errorf("Falied validation '%s': %w", path, err)
	}

	struc.Pack(vol.io, headerOndisk) // internally shift cursor by @volumeHeaderOndiskSize

	vol.cursor = atomic.Uint64{}
	vol.cursor.Store(uint64(volumeHeaderOndiskSize))
	vol.sync_offset = 0
	vol.last_sync_ms = time.Now()

	if err = vol.Sync(); err != nil {
		_ = io.Close()
		return nil, err
	}

	vol.header = volumeHeaderFrom(headerOndisk)

	vol.logger.Info("Create new volume", "header", vol.header)

	return vol, nil
}

func (vol *Volume) Reader() (*VolumeReader) { 
	if vol.close_st {
		return nil
	}
	
	vol.refcnt.Add(1)
	return &VolumeReader{
		vol: vol,
	}
}

func (vol *Volume) Writer() (*VolumeWriter) { 
	if vol.close_st {
		return nil
	}

	vol.refcnt.Add(1)
	return &VolumeWriter{
		vol: vol,
	}
}

func (vol *Volume) Rewriter() (*VolumeRewriter) {
	if vol.close_st {
		return nil
	}

	vol.refcnt.Add(1)
	return &VolumeRewriter{
		vol: vol,
	}
}
// Read only. do not modify values under returned pointer
func (vol *Volume) Header() (VolumeHeader) { return vol.header }
// Since does not lock on access size result maybe approximate
func (vol *Volume) Size() (unsyced, synced uint64) { return vol.cursor.Load(), vol.sync_offset }
// length of ondisk VolumeHeader
func (vol *Volume) HeaderEnd() (int) { return volumeHeaderOndiskSize }

func (vol *Volume) Sync() (error) {
	vol.wrLock.Lock()
	defer vol.wrLock.Unlock()
	
	err := vol.io.Sync()
	
	if err != nil {
		vol.logger.Error("Sync error", "desc", err.Error())
		return err
	}

	vol.sync_offset = vol.cursor.Load()
	vol.logger.Info("Sync", "offset", vol.sync_offset)

	return err
}

// Close waits for readers and writers termination than close io and destruct Volume
// You must not use @vol after Close()
func (vol *Volume) Close() (err error) {
	vol.close_st = true
	vol.refcnt.Wait()

	if vol.cursor.Load() != vol.sync_offset {
		err = vol.Sync()
	}

	if err = vol.io.Close(); err != nil {
		vol.logger.Error("Close error", "desc", err.Error())
	} else {
		vol.logger.Info("Closed noramlly")
	}

	return err
}

/*
	VolumeReader, VolumeWriter, VolumeRewriter represents logical descriptors of SS volume.
	Volume can have unlimited number of Volume descriptors. Volume descriptors and their interfaces
	are thread safe.
*/

// VolumeReader implements models.ReadAtCloser interface
type VolumeReader struct {
	vol     *Volume  // link to underlying Volume
}

func (vr *VolumeReader) ReadAt(p []byte, off int64) (n int, err error) {
	cursor := vr.vol.cursor.Load()
	if uint64(off) >= cursor {
		return 0, io.EOF
	}
	end := uint64(len(p))
	if uint64(len(p)) + uint64(off) > cursor {
		end = cursor - uint64(off)
	}
	n, err = vr.vol.io.ReadAt(p[:end], off)

	if end != uint64(len(p)) {
		err = io.EOF
	}

	if err != nil && err != io.EOF {
		vr.vol.logger.Error("Read error", "desc", err.Error())
	}

	return n, err
}

func (vr *VolumeReader) Close() (err error) {
	vr.vol.refcnt.Done()
	return nil
}

// VolumeWriter implements models.WriteAtCloser interface
type VolumeWriter struct {
	vol     *Volume       // link to underlying Volume
}

func (vw *VolumeWriter) Write(b []byte) (n int, err error) {
	vw.vol.wrLock.Lock()
	defer vw.vol.wrLock.Unlock()

	n, err = vw.vol.io.Write(b)
	
	if err != nil {
		vw.vol.logger.Error("Write error", "desc", err.Error())
		return n, err
	}

	vw.vol.cursor.Add(uint64(n))

	return n, err
}

func (vw *VolumeWriter) Close() (err error) {
	vw.vol.refcnt.Done()
	return nil
}

func (vw *VolumeWriter) Sync() (err error) {
	return vw.vol.Sync()
}

type VolumeRewriter struct {
	vol     *Volume  // link to underlying Volume
}

// VolumeReader io.WriteAt implementation
func (vrw *VolumeRewriter) WriteAt(b []byte, off int64) (n int, err error) {
	vrw.vol.wrLock.Lock()
	defer vrw.vol.wrLock.Unlock()

	cursor := vrw.vol.cursor.Load()
	if uint64(off) >= cursor {
		return 0, io.EOF
	}

	end := uint64(len(b))
	if uint64(len(b)) + uint64(off) > cursor {
		end = cursor - uint64(off)
	}

	n, err = vrw.vol.io.WriteAt(b[:end], off)

	if end != uint64(len(b)) {
		err = io.EOF
	}

	if err != nil && err != io.EOF {
		vrw.vol.logger.Error("WriteAt error", "desc", err.Error())
		return n, err
	}

	return n, err
}

func (vrw *VolumeRewriter) Sync() (err error) {
	return vrw.vol.Sync()
}

// VolumeRewriter io.Closer implementation
func (vrw *VolumeRewriter) Close() (err error) {
	vrw.vol.refcnt.Done()
	return nil
}