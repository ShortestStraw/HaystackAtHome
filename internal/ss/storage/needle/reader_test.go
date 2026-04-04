package needle

import (
	"hash/crc64"
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