package needle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"log/slog"
	"testing"
	"crypto/rand"

	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"HaystackAtHome/internal/ss/models"

	"golang.org/x/sys/unix"
)

func testSimpleScanerCase(t *testing.T, ctx context.Context, logger *slog.Logger, file_buf []byte, buf_sz uint64, ra bool) {
	rd := bytes.NewReader(file_buf)
	var (
		it  *Iter
		err error
	)
	if ra {
		it, err = NewIter(ctx, rd, 
			WithBufferSize(int(buf_sz)),
			WithReadahead(),
			WithChecksumAlg(crc64.New(crc64.MakeTable(crc64.ISO))),
			WithLogger(logger),
		)
	} else {
		it, err = NewIter(ctx, rd, 
			WithBufferSize(int(buf_sz)),
			WithChecksumAlg(crc64.New(crc64.MakeTable(crc64.ISO))),
			WithLogger(logger),
		)
	}

	if err != nil || it == nil {
		t.Fatalf("failed to init iter: %e", err)
	}

	defer it.Close()

	n := 1
	nh, err := it.Next(ctx)
	for ; err == nil;  nh, err = it.Next(ctx) {
		if n > 1 {
			t.Errorf("num of needles larger than '1'")
		}
		if nh.Version != currentVersion {
			t.Errorf("version = '%d', want '%d'", nh.Version, currentVersion)
		}
		if nh.Key != 1234567 {
			t.Errorf("key = '%d', want '%d'", nh.Key, 1234567)
		}
		if nh.Flags != 0 {
			t.Errorf("flags = '%d', want '%d'", nh.Flags, 0)
		}
		n++
	}

	if !errors.Is(err, &models.ErrObjValidation{}) && !errors.Is(err, &models.ErrObjCSMismatch{}) && err != io.EOF {
		t.Errorf("last err = '%s', want '&ErrValidation{}' or 'io.EOF", err.Error())
	}
}

func TestSimpleScaner(t *testing.T) {
	t.Parallel()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	file_buf := make([]byte, 1024 * 1024)

	wrNeedle(file_buf, 1234567, []byte("zxcvbnm"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ras := []bool{false}
	// buf_szs := []uint64{1, 4, 51, 1024, 1024 * 256, 1024 * 1024 * 2}
	buf_szs := []uint64{51}
	i := 0
	for _, ra := range ras {
		for _, buf_sz := range buf_szs {
			path := fmt.Sprintf(".volume.1.%d", i)
			logger := slog.With("file", path)
			testSimpleScanerCase(t, ctx, logger, file_buf, buf_sz, ra)
			i++
		}
	}
}

func TestScanerDamagedTail(t *testing.T) {
	t.Parallel()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	file_buf := make([]byte, 1024 * 1024)

	off := wrNeedle(file_buf, 1234567, []byte("zxcvbnm"))
	wrNeedle(file_buf[off:], 12345678, []byte("asdfgzxcvbnm"))
	
	for i := range 10 {
		file_buf[off + uint64(i)] = 'z'
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ras := []bool{true, false}
	buf_szs := []uint64{51, 1024, 1024 * 256, 1024 * 1024 * 2}

	i := 0
	for _, ra := range ras {
		for _, buf_sz := range buf_szs {
			path := fmt.Sprintf(".volume.2.%d", i)
			logger := slog.With("file", path)
			testSimpleScanerCase(t, ctx, logger, file_buf, buf_sz, ra)
			i++
		}
	}
}

func testScanerOffsetsCase(
	t *testing.T, 
	ctx context.Context, 
	logger *slog.Logger, 
	file_buf []byte, 
	buf_size uint64, 
	ra bool, 
	objs []obj, 
	offs []uint64,
	stOff uint64,
) {
	rd := bytes.NewReader(file_buf)
	var (
		it  *Iter
		err error
	)
	if ra {
		it, err = NewIter(ctx, rd, 
			WithBufferSize(int(buf_size)),
			WithReadahead(),
			WithChecksumAlg(crc64.New(crc64.MakeTable(crc64.ISO))),
			WithLogger(logger),
			WithStartOffset(stOff),
		)
	} else {
		it, err = NewIter(ctx, rd, 
			WithBufferSize(int(buf_size)),
			WithChecksumAlg(crc64.New(crc64.MakeTable(crc64.ISO))),
			WithLogger(logger),
			WithStartOffset(stOff),
		)
	}

	if err != nil || it == nil {
		t.Fatalf("failed to init iter: %e", err)
	}

	defer it.Close()

	i := 0
	nh, err := it.Next(ctx)
	for ; err == nil;  {
		if nh.Key != objs[i].key {
			t.Errorf("key = '%d', want '%d'", nh.Key, objs[i].key)
		}
		if nh.Version != currentVersion {
			t.Errorf("version = '%d', want '%d'", nh.Version, currentVersion)
		}
		if nh.Flags != 0 {
			t.Errorf("flags = '%d', want '%d'", nh.Flags, 0)
		}
		dsz := uint64(len(objs[i].data))
		if nh.DataSize != dsz {
			t.Errorf("dataSize = '%d', want '%d'", nh.DataSize, dsz)
		}
		if it.Offset() != offs[i] {
			t.Errorf("header offset = '%d', want '%d'", it.Offset(), offs[i])
		}
		i++
		nh, err = it.Next(ctx)
	}

	if !errors.Is(err, &models.ErrObjValidation{}) && !errors.Is(err, &models.ErrObjCSMismatch{}) && err != io.EOF {
		t.Errorf("last err = '%s', want '&ErrValidation{}'", err.Error())
	}

	if it.Offset() != offs[i] {
		t.Errorf("last needle end = '%d', want '%d'", it.Offset(), offs[i])
	}
}

func TestScanerOffsets(t *testing.T) {
	t.Parallel()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	file_buf := make([]byte, 1024 * 1024)

	objs := []obj{
		{123, []byte("asdfghjkl")},
		{1234, []byte("asdfghjklzxcvb")},
		{1235, []byte("asdfghjklzxcvbzxcv")},
		{1236, []byte("asdfghjklzxcvbmnbvcxz")},
	}

	offs := []uint64{0}
	currOff := uint64(0)
	for _, o := range objs {
		off := wrNeedle(file_buf[currOff:], o.key, o.data)
		currOff += off
		offs = append(offs, currOff) // last offset value must be reported in last it.Offset()
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ras := []bool{true, false}
	buf_szs := []uint64{51, 1024, 1024 * 256, 1024 * 1024 * 2}

	i := 0
	for _, ra := range ras {
		for _, buf_sz := range buf_szs {
			path := fmt.Sprintf(".volume.3.%d", i)
			logger := slog.With("file", path)
			testScanerOffsetsCase(t, ctx, logger, file_buf, buf_sz, ra, objs, offs, 0)
			i++
		}
	}
}

func TestScanerStartOffsets(t *testing.T) {
	t.Parallel()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	file_buf := make([]byte, 1024 * 1024)

	offs := []uint64{0}
	currOff := uint64(0)
	for _, o := range objs {
		off := wrNeedle(file_buf[currOff:], o.key, o.data)
		currOff += off
		offs = append(offs, currOff) // last offset value must be reported in last it.Offset()
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ras := []bool{true, false}
	buf_szs := []uint64{51, 1024, 1024 * 256, 1024 * 1024 * 2}

	i := 0
	for _, ra := range ras {
		for _, buf_sz := range buf_szs {
			for k, stOff := range offs {
				path := fmt.Sprintf(".volume.3.%d", i)
				logger := slog.With("file", path)
				testScanerOffsetsCase(t, ctx, logger, file_buf, buf_sz, ra, objs[k:], offs[k:], stOff)
				i++
			}
		}
	}
}

// printFilesystemInfo prints detailed filesystem and disk information
// for the specified path using logger
func printFilesystemInfo(path string) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		fmt.Printf("Failed to get absolute path for %q: %v\n", path, err)
		absPath = path // fallback
	}

	// 1. Statfs to get filesystem stats
	var stat unix.Statfs_t
	if err := unix.Statfs(absPath, &stat); err != nil {
		fmt.Printf("Statfs failed for %q: %v\n", absPath, err)
		return
	}

	// 2. Filesystem type (magic number -> name)
	fsType := fsTypeName(stat.Type)
	fmt.Printf("Filesystem: %s\n", fsType)

	// 3. Size and usage
	blockSize := stat.Bsize
	totalBlocks := stat.Blocks
	freeBlocks := stat.Bfree
	// availBlocks := stat.Bavail // blocks available to unprivileged users

	totalSize := uint64(totalBlocks) * uint64(blockSize)
	usedSize := totalSize - uint64(freeBlocks)*uint64(blockSize)
	usedPercent := 0.0
	if totalSize > 0 {
		usedPercent = float64(usedSize) / float64(totalSize) * 100
	}

	fmt.Printf("Block size: %d bytes\n", blockSize)
	fmt.Printf("Total size: %d bytes (%.2f GiB)\n", totalSize, float64(totalSize)/(1024*1024*1024))
	fmt.Printf("Used: %d bytes (%.2f GiB)\n", usedSize, float64(usedSize)/(1024*1024*1024))
	fmt.Printf("Used: %.2f%%\n", usedPercent)

	// 4. Find the underlying device for this path
	device, err := findDeviceForPath(absPath)
	if err != nil {
		fmt.Printf("Could not determine device for %q: %v\n", absPath, err)
		return
	}
	if device == "" {
		fmt.Printf("No block device associated (maybe a virtual filesystem like tmpfs)\n")
		return
	}

	device = strings.Split(device, "p")[0]

	fmt.Printf("Underlying device: %s\n", device)

	// 5. Get disk info using lsblk (if available)
	diskInfo, err := getDiskInfo(device)
	if err != nil {
		fmt.Printf("Could not retrieve disk info via lsblk: %v\n", err)
		return
	}
	for _, line := range diskInfo {
		fmt.Printf("  %s\n", line)
	}
}

// fsTypeName returns a human-readable name for a filesystem magic number.
// Add more constants as needed (values from /usr/include/linux/magic.h).
func fsTypeName(magic int64) string {
	magicMap := map[int64]string{
		0xef53:        "ext2/ext3/ext4",
		0x58465342:    "xfs",
		0x9123683e:    "btrfs",
		0x52654973:    "reiserfs",
		0x2fc12fc1:    "zfs",
		0x6969:        "nfs",
		0x9fa0:        "proc",
		0x858458f6:    "ramfs",
		0x01021994:    "tmpfs",
		0x1cd1:        "devpts",
		0x62656572:    "sysfs",
		0x1373:        "devtmpfs",
		0x42494e4d:    "binfmt_misc",
		// Add more as needed
	}
	if name, ok := magicMap[magic]; ok {
		return name
	}
	return fmt.Sprintf("unknown (0x%x)", magic)
}

// findDeviceForPath returns the block device (e.g., /dev/sda1) that contains the given path.
// It parses /proc/mounts and finds the entry with the longest prefix match.
func findDeviceForPath(path string) (string, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return "", err
	}
	defer file.Close()

	var bestMatch string
	var bestDevice string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		device := fields[0]
		mountPoint := fields[1]

		// Check if path is under this mount point (or equal)
		if strings.HasPrefix(path, mountPoint) {
			// Prefer the longest mount point
			if len(mountPoint) > len(bestMatch) {
				bestMatch = mountPoint
				bestDevice = device
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	if bestDevice == "" {
		return "", nil
	}
	return bestDevice, nil
}

// getDiskInfo runs lsblk on the given device and returns the output lines.
// It requests NAME, ROTA (rotational), SIZE, MODEL.
func getDiskInfo(device string) ([]string, error) {
	// lsblk -o NAME,ROTA,SIZE,MODEL <device>
	cmd := exec.Command("lsblk", "-d", "-o", "NAME,ROTA,SIZE,MODEL", device)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("lsblk failed: %v\n%s", err, output)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	// The first line is the header; we can keep it or strip it.
	// For readability, we keep it.
	return lines, nil
}

func TestFSInfo(t *testing.T) {
	printFilesystemInfo(".")
}

func prepObjs(b *testing.B) {
	b.Helper()
	for i := range 40 {
		sz := i * 1024 * 4
		data := make([]byte, sz)
		n, err := rand.Read(data)
		if err != nil {
			b.Fatalf("failed to rand buf")
		}
		if n != sz {
			b.Fatalf("failed to rand buf: n '%d', want '%d", n, sz)
		}
		objs = append(objs, obj{ key: uint64(sz), data: data })
	}
}

const (
	path_bench = ".volume.bench"
)

// create in memory and ondisk file with set of needles
func prepareFile(b *testing.B, sz uint64) []byte {
	b.Helper()

	file := make([]byte, sz)
	cursor := uint64(0)

	i := 0
	for cursor < sz {
		written := wrNeedle(file[cursor:], objs[i].key, objs[i].data)
		if written == 0 {
			break
		}
		cursor += written
		i = (i + 1) % len(objs)
	}

	_ = os.Remove(path_bench)
	f, err := os.OpenFile(path_bench, os.O_CREATE | os.O_APPEND | os.O_EXCL | os.O_RDWR, 0o664)
	if err != nil {
		b.Fatalf("failed to create and open '%s': %v", path_bench, err)
	}
	defer f.Close()

	file_reader := bytes.NewReader(file)

	wr, err := io.CopyN(f, file_reader, int64(len(file)))
	if err != nil {
		b.Fatalf("failed to copy to '%s': %v", path_bench, err)
	}
	if wr != int64(len(file)) {
		b.Fatalf("failed to copy to '%s': written '%d' != '%d' size", path_bench, wr, len(file))
	}

	return file
}

type file_ struct {
	path string
	size uint64
}

func benchmarkScaner(b *testing.B, inmem []byte, ondisk *file_, bufSz uint64, ra bool) {
	var (
		it  *Iter
		err error
		buf_reader io.ReaderAt
		f   *os.File
		sz  uint64
	)
	countNext := uint64(0)

	for b.Loop() {
		b.StopTimer()
		if inmem != nil {
			buf_reader = bytes.NewReader(inmem)
			sz = uint64(len(inmem))
		} else if ondisk != nil {
			orig, err := os.Open(ondisk.path)
			if err != nil {
				b.Fatalf("failed to open '%s': %v", ondisk.path, err)
			}

			pathTmp := fmt.Sprintf("%s.%p", ondisk.path, ondisk)
			_ = os.Remove(pathTmp)
			f, err = os.OpenFile(pathTmp, os.O_CREATE | os.O_APPEND | os.O_EXCL | os.O_RDWR, 0o664)
			if err != nil {
				b.Fatalf("failed to create and open '%s': %v", ondisk.path, err)
			}

			sz = ondisk.size
			wr, err := io.CopyN(f, orig, int64(ondisk.size))
			if err != nil {
				b.Fatalf("failed to copy '%s' to '%s': %v", ondisk.path, pathTmp, err)
			}
			if wr != int64(ondisk.size) {
				b.Fatalf("failed to copy '%s' to '%s': written '%d' != '%d' size", ondisk.path, pathTmp, wr, ondisk.size)
			}

			f.Seek(0, 0)
			if err != nil {
				b.Fatalf("failed to seek '%s': %v", ondisk.path, err)
			}

			buf_reader = f

			orig.Close()
		} else {
			b.Fatalf("Neither @inmem or @ondisk must be specified")
		}
		if ra {
			it, err = NewIter(b.Context(), buf_reader,
				WithBufferSize(int(bufSz)),
				WithChecksumAlg(crc64.New(crc64.MakeTable(crc64.ISO))),
				WithReadahead(),
				// WithLogger(slog.Default()),
			)
		} else {
			it, err = NewIter(b.Context(), buf_reader,
				WithBufferSize(int(bufSz)),
				WithChecksumAlg(crc64.New(crc64.MakeTable(crc64.ISO))),
			)
		}
		if err != nil {
			b.Fatalf("Failed to init benchmark: file sz '%d', buf sz '%d', ra '%t', err '%s'", sz, bufSz, ra, err.Error())
		}
		if it == nil {
			b.Fatalf("it = 'nil'")
		}
		b.StartTimer()
		var nh *Header
		nh, err = it.Next(b.Context())
		for ; err == nil || errors.Is(err, &models.ErrObjValidation{}); nh, err = it.Next(b.Context()) {
			_ = nh
			countNext++
		}
		countNext++

		if err != io.EOF {
			b.Fatalf("err = '%s', want '%s'", err.Error(), io.EOF.Error())
		}
		b.SetBytes(int64(sz))
		it.Close()
		if f != nil {
			f.Close()
		}
	}

	b.ReportMetric(float64(b.Elapsed().Nanoseconds()) / float64(countNext) ,"ns/it.Next()")
	b.ReportMetric(float64(0), "ns/op")
}

/*
As benchmark result, ra does not give any advantage when read from slice of tiny objs.

As benchmark result, ra does not give any advantage for sequential read from file of tiny objs.

After add to objs set 40 large objects (several KiBs) the results become much better
and ra started play it role. For objects ~10 KiB size it gives 30% speed rate on btrfs 
(warmed pcache, 531G occupied) | nvme0n1 931,5G Samsung SSD 990 PRO 1TB | kernel-6.14.5-100.fc40.x86_64
so bottleneck is header\footer encoding\decoding when objects are tiny but
when objs size growed bottleneck is mostly checksumming.

Also, increasing obj size increases influence of buffer size. The bandwith is higher and alloc count 
is much smaller.
*/
func BenchmarkScanner(b *testing.B) {
	clean := func() { to_rm := path_bench + "*"; exec.Command("rm", "-rf", to_rm) }
	clean()
	b.Cleanup(clean)
	b.Helper()
	printFilesystemInfo(".")
	slog.SetLogLoggerLevel(slog.LevelDebug)
	prepObjs(b)

	// from 1 MiB to 1 GiB
	szs := []uint64{
		1024 * 1024, 
		1024 * 1024 * 2,
		1024 * 1024 * 4,
		1024 * 1024 * 8,
		1024 * 1024 * 16,
		1024 * 1024 * 32,
		1024 * 1024 * 64,
		1024 * 1024 * 128,
		1024 * 1024 * 256,
		1024 * 1024 * 512,
		1024 * 1024 * 1024,
	}

	bufSzs := []uint64{
		1024,
		1024 * 4,
		1024 * 16,
		1024 * 64,
		1024 * 256,
		1024 * 1024,
	}

	file := prepareFile(b, szs[len(szs) - 1])

	ras := []bool{true, false}

	i := 0
	for _, sz := range szs {
		for _, bufSz := range bufSzs {
			for _, ra := range ras {
				if bufSz * 4 >= sz { continue } // not interesting configuration since the inaccuracy may be higher than 25%
				b.SetParallelism(64)
				name := fmt.Sprintf("%d:memory'%d'MiB,buf'%d'KiB,ra'%t'", i, sz / (1024 * 1024), bufSz / 1024, ra)
				b.Run(name, func(b *testing.B) {
					benchmarkScaner(b, file[:sz], nil, bufSz, ra)
				})
				name = fmt.Sprintf("%d:file'%d'MiB,buf'%d'KiB,ra'%t'", i, sz / (1024 * 1024), bufSz / 1024, ra)
				b.Run(name, func(b *testing.B) {
					benchmarkScaner(b, nil, &file_{ path: path_bench, size: sz }, bufSz, ra)
				})
				i++
			}
		}
	}
}