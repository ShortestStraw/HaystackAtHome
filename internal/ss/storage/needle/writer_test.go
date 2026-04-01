package needle

import (
	"bytes"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"testing"

	"github.com/lunixbochs/struc"
)

func rdNeedle(rd io.ReaderAt, off uint64, cs hash.Hash64) (*Header, []byte, error) {
	h := headerOndisk{}

	_off := int64(off)
	
	reader := io.NewSectionReader(rd, _off, int64(headerOndiskSize))
	if err := struc.Unpack(reader, &h); err != nil {
		return nil, nil, fmt.Errorf("failed to Unpack header: %v", err)
	}

	if err := validateHeader(&h, off); err != nil {
		return nil, nil, fmt.Errorf("failed to validate header: %v", err)
	}

	cs.Reset()
	savedFlags := h.Flags
	h.Flags = 0
	if err := struc.Pack(cs, &h); err != nil {
		return nil, nil, fmt.Errorf("failed to Pack header to Hash: %v", err)
	}
	h.Flags = savedFlags

	header := &Header{
		Version: h.Version,
		Key: h.Key,
		DataSize: h.DataSize,
		Flags: h.Flags,
	}

	_off += int64(headerOndiskSize)
	reader = io.NewSectionReader(rd, _off, int64(h.DataSize))
	tee := io.TeeReader(reader, cs)

	data, err := io.ReadAll(tee)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to ReadAll tee: %v", err)
	}
	if len(data) != int(h.DataSize) {
		return nil, nil, fmt.Errorf("data size mismatch: len(data) '%d', want '%d'", len(data), h.DataSize)
	}

	pad := calcFooterPadding(h.DataSize)
	_off += int64(h.DataSize)
	reader = io.NewSectionReader(rd, _off, int64(pad + footerOndiskSizeMin))

	f := footerOndisk{}
	if err := footerOndiskDecoderFrom(&f, pad).Unpack(reader); err != nil {
		return nil, nil, fmt.Errorf("failed to Unpack footer: %v", err)
	}

	if err := validateFooter(&f, uint64(_off), cs.Sum64()); err != nil {
		return nil, nil, fmt.Errorf("failed to validate footer: %v", err)
	}

	return header, data, nil
}

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
		to_write := sz
		for to_write > 0 {
			written, err := w.Write(obj.data)
			to_write -= uint64(written)
			if err != nil {
				break
			}
		}
		if to_write != 0 {
			t.Fatalf("failed to write needle: to_write '%d'", to_write)
		}
		// we will not close writer to reuse os.File
		if err != nil {
			t.Fatalf("failed to write needle: %v", err)
		}
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