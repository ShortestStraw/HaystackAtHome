package needle

import (
	"bytes"
	"hash"
	"io"

	"github.com/lunixbochs/struc"
)

type Writer struct {
	fd     io.WriteCloser
	h      *headerOndisk
	hDone  bool  // setted to true when h was serialized to fd
	fDone  bool  // setted to true when footer was serilized to fd
	written uint64

	cs     hash.Hash64
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

	sz := calcFooterPadding(dataSize) + headerOndiskSize + footerOndiskSizeMin + w.h.DataSize

	return w, sz, nil
}

func (w *Writer) Write(b []byte) (int, error) {
	amendment := 0
	if !w.hDone {
		buf := bytes.NewBuffer(make([]byte, 0, headerOndiskSize))
		if err := struc.Pack(buf, w.h); err != nil {
			return 0, err
		}

		written, err := io.Copy(w.fd, buf)
		amendment = int(written)
		if err != nil {
			return amendment, err
		}
		
		w.hDone = true
	}

	reader := bytes.NewReader(b)
	var tee io.Reader
	if w.cs != nil {
		tee = io.TeeReader(reader, w.cs)
	} else {
		tee = reader
	}

	to_write := int64(len(b))
	_written := int(0)
	if w.written + uint64(to_write) > w.h.DataSize {
		to_write = int64(w.h.DataSize) - int64(w.written)
	}
	for to_write > 0 {
		written, err := io.CopyN(w.fd, tee, to_write)
		w.written += uint64(written)
		_written += int(written)
		to_write -= written
		if err != nil {
			return _written + amendment, err
		}
	}

	if w.written == w.h.DataSize {
		if w.fDone {
			return 0, io.EOF
		}
		defer func(){ w.fDone = true }()
		csum := uint64(0)
		if w.cs != nil {
			csum = w.cs.Sum64()
		}
		pad := calcFooterPadding(w.h.DataSize)
		f := &footerOndisk{
			Magic: footerMagic,
			Checksum: csum,
		}

		enc := footerOndiskEncoderFrom(f, pad)
		err := error(nil)
		if err = enc.Pack(w.fd); err == nil {
			amendment += int(pad) + int(footerOndiskSizeMin)
		}
		return _written + amendment, err
	}
	return _written + amendment, nil
}

func (w *Writer) Close() error {
	return w.fd.Close()
} 