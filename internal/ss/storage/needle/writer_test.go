package needle

import (
	"bytes"
	"hash/crc64"
	"os"
	"testing"
	"io"
)

func TestSimpleWriter(t *testing.T) {
	path := ".volume.test.writer"
	f, err := os.OpenFile(path, os.O_APPEND | os.O_CREATE | os.O_EXCL | os.O_RDWR, 0o666)
	if err != nil {
		t.Fatalf("failed to create file '%s': %v", path, err)
	}
	defer os.Remove(path)
	defer f.Close()
	
	offs := []uint64{0}
	for _, obj := range objs {
		w, sz, err := NewWriter(f, obj.key, 0, uint64(len(obj.data)), crc64.New(crc64.MakeTable(crc64.ISO)))
		if err != nil {
			t.Fatalf("failed to make writer: %v", err)
		}
		offs = append(offs, sz + offs[len(offs) - 1])

		// Write returns user-data bytes only; io.EOF signals completion.
		n, err := w.Write(obj.data)
		if n != len(obj.data) {
			t.Fatalf("short write: wrote %d want %d", n, len(obj.data))
		}
		if err != nil && err != io.EOF {
			t.Fatalf("failed to write needle: %v", err)
		}
		// we will not close writer to reuse os.File
	}

	stat, err := f.Stat()
	if stat.Size() != int64(offs[len(offs) - 1]) {
		t.Fatalf("file size '%d', want '%d", stat.Size(), offs[len(offs) - 1])
	}

	for i, obj := range objs {
		h, data, err := rdNeedle(f, offs[i], crc64.New(crc64.MakeTable(crc64.ISO)))
		if err != nil {
			t.Fatalf("failed to read needle: %v", err)
		}
		if h.Key != obj.key {
			t.Fatalf("key mismatch: '%v', want '%v'", h.Key, obj.key)
		}
		if !bytes.Equal(data, obj.data) {
			t.Fatalf("data mismatch:\n  data '%v',\n  want '%v'", data, obj.data)
		}
	}
}