package needle

import (
	"bytes"
	"hash/crc64"
	"io"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestSimpleReader(t *testing.T) {
	t.Parallel()
	sz := 1024 * 4
	file_buf := make([]byte, sz)
	cursor := uint64(0)

	offs := []uint64{0}
	for _, obj := range objs {
		written := wrNeedle(file_buf[cursor:], obj.key, obj.data)
		cursor += written
		offs = append(offs, cursor)
	}

	path := ".volume.test.reader"
	os.Remove(path)
	f, err := os.OpenFile(path, os.O_APPEND | os.O_CREATE | os.O_EXCL | os.O_RDWR, 0o666)
	if err != nil {
		t.Fatalf("failed to create file '%s': %v", path, err)
	}
	defer os.Remove(path)
	defer f.Close()

	f.Write(file_buf)
	f.Seek(0, 0)

	for i := range objs {
		reader, err := NewReader(f, offs[i], crc64.New(crc64.MakeTable(crc64.ISO)))

		if s := cmp.Diff(nil, err, cmpopts.EquateErrors()); s != "" {
			t.Fatalf("1 NewRead mismatch (-want +got):\n%s", s)
		}

		hGot := reader.Header()
		hWant := Header{
			currentVersion,
			objs[i].key,
			0,
			uint64(len(objs[i].data)),
		}

		if s := cmp.Diff(hWant, hGot); s != "" {
			t.Fatalf("header mismatch (-want +got):\n%s", s)
		}

		data := make([]byte, hGot.DataSize)

		to_read := hGot.DataSize
		for to_read > 0 {
			n, err := reader.Read(data)
			to_read -= uint64(n)
			if s := cmp.Diff(nil, err, cmpopts.EquateErrors()); s != "" {
				t.Fatalf("read error (-want +got):\n%s", s)
			}
		}

		if s := cmp.Diff(objs[i].data, data); s != "" {
			t.Fatalf("data mismatch (-want +got):\n%s", s)
		}

		// Do not close for reuse
		if i != len(objs) - 1 { continue }

		// Close test on last iteration
		err = reader.Close()
		if s := cmp.Diff(nil, err, cmpopts.EquateErrors()); s != "" {
			t.Fatalf("close error (-want +got):\n%s", s)
		}
	}
}

// noCloseFile wraps *os.File to prevent needle.Reader.Close from closing the
// shared file handle, allowing multiple readers to be opened and closed over
// the same file within a single test.
type noCloseFile struct{ *os.File }
func (noCloseFile) Close() error { return nil }

// TestReaderReadAll verifies that io.ReadAll returns exactly DataSize bytes and
// no more. The existing TestSimpleReader avoids this by pre-allocating a buffer
// of exactly DataSize and reading in one shot; io.ReadAll calls Read repeatedly,
// including one final call after all data is consumed, which would overread into
// the footer / next needle if Read does not return io.EOF at the data boundary.
func TestReaderReadAll(t *testing.T) {
	t.Parallel()

	const bufSz = 1024 * 64
	file_buf := make([]byte, bufSz)
	cursor := uint64(0)
	offs := make([]uint64, len(objs))
	for i, obj := range objs {
		offs[i] = cursor
		cursor += wrNeedle(file_buf[cursor:], obj.key, obj.data)
	}

	path := ".volume.test.reader.readall"
	os.Remove(path)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o666)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	defer os.Remove(path)
	defer f.Close()
	f.Write(file_buf)

	for i, obj := range objs {
		cs := crc64.New(crc64.MakeTable(crc64.ISO))
		reader, err := NewReader(noCloseFile{f}, offs[i], cs)
		if s := cmp.Diff(nil, err, cmpopts.EquateErrors()); s != "" {
			t.Fatalf("obj[%d] NewReader (-want +got):\n%s", i, s)
		}

		got, err := io.ReadAll(reader)
		if s := cmp.Diff(nil, err, cmpopts.EquateErrors()); s != "" {
			t.Errorf("obj[%d] ReadAll (-want +got):\n%s", i, s)
		}

		if !bytes.Equal(got, obj.data) {
			t.Errorf("obj[%d]: got %d bytes want %d bytes", i, len(got), len(obj.data))
		}

		if err := reader.Close(); err != nil {
			t.Errorf("obj[%d] Close: %v", i, err)
		}
	}
}