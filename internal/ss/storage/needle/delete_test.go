package needle

import (
	"bytes"
	"hash/crc64"
	"io"
	"testing"
)

// writableSlice wraps a []byte to implement io.WriterAt for in-memory testing.
type writableSlice []byte

func (b writableSlice) WriteAt(p []byte, off int64) (int, error) {
	if int(off)+len(p) > len(b) {
		return 0, io.ErrShortWrite
	}
	return copy(b[off:], p), nil
}

// TestMarkDeleted writes all test needles into an in-memory buffer, marks every
// other needle as deleted, then reads all needles back and verifies:
//   - the marked subset has the deleted flag set
//   - the unmarked subset does not
//   - checksum validation passes for all needles, including the modified ones,
//     confirming that Flags are excluded from checksum calculation
func TestMarkDeleted(t *testing.T) {
	const fileSz = 1024 * 1024
	file_buf := make([]byte, fileSz)

	// write all test needles sequentially, record each needle's start offset
	offsets := make([]uint64, len(objs))
	cur := uint64(0)
	for i, o := range objs {
		n := wrNeedle(file_buf[cur:], o.key, o.data)
		if n == 0 {
			t.Fatalf("wrNeedle[%d]: returned 0", i)
		}
		offsets[i] = cur
		cur += n
	}

	// mark every even-indexed needle as deleted
	ws := writableSlice(file_buf)
	for i := 0; i < len(objs); i += 2 {
		if err := MarkDeleted(ws, DefaultFlags, offsets[i]); err != nil {
			t.Fatalf("MarkDeleted[%d] off=%d: %v", i, offsets[i], err)
		}
	}

	// read all needles back and verify flags and data integrity.
	// checksum must pass even for the modified needles because Flags are zeroed
	// out before checksum calculation in both wrNeedle and rdNeedle.
	cs := crc64.New(crc64.MakeTable(crc64.ISO))
	rd := bytes.NewReader(file_buf)
	for i, o := range objs {
		h, data, err := rdNeedle(rd, offsets[i], cs)
		if err != nil {
			t.Errorf("rdNeedle[%d] off=%d: %v", i, offsets[i], err)
			continue
		}
		if h.Key != o.key {
			t.Errorf("needle[%d]: key=%d want %d", i, h.Key, o.key)
		}
		if !bytes.Equal(data, o.data) {
			t.Errorf("needle[%d]: data mismatch", i)
		}
		wantDeleted := i%2 == 0
		if FlagsDeleted(h.Flags) != wantDeleted {
			t.Errorf("needle[%d]: deleted=%v want %v", i, FlagsDeleted(h.Flags), wantDeleted)
		}
	}
}
