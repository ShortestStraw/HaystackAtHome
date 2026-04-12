package needle

import (
	"bytes"
	"fmt"
	"hash"
	"io"

	"github.com/lunixbochs/struc"
)

type Writer struct {
	fd      io.WriteCloser
	h       *headerOndisk
	hDone   bool  // set to true when header has been serialized to fd
	written uint64

	cs      hash.Hash64
}

// The caller must enshure that fd have enough space for write
// writer consume fd and will close it when close itself
// return neddle io.Writer and neddle full size or error
func NewWriter(fd io.WriteCloser, key, flags, dataSize uint64, cs hash.Hash64) (*Writer, uint64, error) {
	w := &Writer{
		fd: fd,
		cs: cs,
	}

	w.h = &headerOndisk{
		Magic: headerMagic,
		Version: currentVersion,
		Key: key,
		Flags: flags,
		DataSize: dataSize,
		Reserved: [2]uint64{0,0},
	}

	if w.cs != nil {
		w.cs.Reset()
		if err := struc.Pack(cs, w.h); err != nil {
			return nil, 0, err
		}
	}

	sz := CalcNeedleSize(dataSize)

	return w, sz, nil
}

// Write implements io.Writer. It returns exactly the number of bytes consumed
// from b (0 <= n <= len(b)) and a non-nil error when n < len(b).
//
// Internal framing (header, footer) is flushed transparently and is never
// counted in the return value. io.EOF is returned alongside the last user-data
// bytes when all DataSize bytes have been consumed and the footer has been
// flushed; the caller should treat (n, io.EOF) as a successful completion.
// Subsequent calls return (0, io.EOF).
func (w *Writer) Write(b []byte) (int, error) {
	if w.written >= w.h.DataSize {
		return 0, io.EOF
	}

	// Flush header before the first user-data byte.
	if !w.hDone {
		buf := bytes.NewBuffer(make([]byte, 0, headerOndiskSize))
		if err := struc.Pack(buf, w.h); err != nil {
			return 0, fmt.Errorf("failed to pack header: %w", err)
		}
		if _, err := io.Copy(w.fd, buf); err != nil {
			return 0, fmt.Errorf("failed to serialize header: %w", err)
		}
		w.hDone = true
	}

	// Cap to remaining user-data bytes.
	remaining := w.h.DataSize - w.written
	data := b
	if uint64(len(data)) > remaining {
		data = b[:remaining]
	}

	n, err := w.fd.Write(data)
	if n > 0 && w.cs != nil {
		// Update checksum only for bytes that reached the underlying writer.
		w.cs.Write(data[:n])
	}
	w.written += uint64(n)
	if err != nil {
		return n, err
	}

	// All user data written — flush footer.
	if w.written == w.h.DataSize {
		csum := uint64(0)
		if w.cs != nil {
			csum = w.cs.Sum64()
		}
		f := &footerOndisk{Magic: footerMagic, Checksum: csum}
		enc := footerOndiskEncoderFrom(f, calcFooterPadding(w.h.DataSize))
		if err := enc.Pack(w.fd); err != nil {
			return n, fmt.Errorf("failed to serialize footer: %w", err)
		}
		return n, io.EOF
	}

	return n, nil
}

func (w *Writer) Close() error {
	if err := w.fd.Close(); err != nil {
		return fmt.Errorf("failed to close io: %w", err)
	}
	return nil
} 