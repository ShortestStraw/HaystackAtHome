package needle

import (
	"HaystackAtHome/internal/ss/models"
	"fmt"
	"hash"
	"io"

	"github.com/lunixbochs/struc"
)

// implements io.ReadCloser interface for reading needles from io.ReadCloser
// since checksum located in the end of needle
// it is calculated in Close() so it may return models.ErrObjCSMismatch
// but in this case data is fully read and can be used
type Reader struct {
	fd     models.ReadAtCloser
	off    uint64
	h      *headerOndisk
	f      *footerOndisk
	fDone  bool  // setted to true when footer was deserilized from fd
	read   uint64

	cs     hash.Hash64
}

// Creates needle reader for specified buffer
// It actually preread some data to parse and validate header
// so it can return ethier io or models.Validation errors.
//
// There is no big sense to use it if you do not need to verify checksum
// and header/footer. You can directly read from volume if you know user data
// offset and data size.
func NewReader(fd models.ReadAtCloser, off uint64, cs hash.Hash64) (*Reader, error) {
	if fd == nil {
		return nil, fmt.Errorf("fd must be nonnil")
	}
	r :=  &Reader{
		fd: fd,
		cs: cs,
		read: 0,
		off: off,
	}
	
	h := &headerOndisk{}

	if cs != nil {
		cs.Reset()	
	}

	reader := r.tee(int(headerOndiskSize))

	if err := struc.Unpack(reader, h); err != nil {
		return nil, fmt.Errorf("failed to unpack header: %v", err)
	}

	r.off += headerOndiskSize

	if err := validateHeader(h, off); err != nil {
		return nil, fmt.Errorf("header validation error: %v", err)
	}

	r.h = h

	return r, nil
}

func (r *Reader) tee(n int) (reader io.Reader) {
	reader = io.NewSectionReader(r.fd, int64(r.off), int64(n))
	if r.cs != nil {
		reader = io.TeeReader(reader, r.cs)		
	}

	return reader
}

func (r *Reader) Read(b []byte) (int, error) {
	to_read := len(b)
	reader := r.tee(to_read)
	read := 0
	for to_read > 0 {
		n, err := reader.Read(b[read:])
		to_read -= n
		read += n
		r.read += uint64(n)
		r.off += uint64(n)
		if err != nil {
			return read, err
		}
	}

	if r.read == r.h.DataSize && r.fDone == false {
		defer func(){ r.fDone = true }()
		r.f = &footerOndisk{}
		dec := footerOndiskDecoderFrom(r.f, calcFooterPadding(r.h.DataSize))
		reader := io.NewSectionReader(r.fd, int64(r.off), int64(footerOndiskSizeMax))
		if err := dec.Unpack(reader); err != nil {
			return read, fmt.Errorf("failed to decode footer: %v", err)
		}
	}

	return read, nil
}

func (r *Reader) Close() error {
	if err := r.fd.Close(); err != nil {
		return fmt.Errorf("failed to close io: %v", err)
	}
	if r.cs != nil {
		if r.f != nil {
			if err := validateFooter(r.f, r.off, r.cs.Sum64()); err != nil {
				return fmt.Errorf("footer validation error: %v", err)
			} else {
				return nil
			}
		} else {
			return models.NewErrObjValidation("footer was never read, cannot validate", r.off)
		}
	}

	return nil
}

func (r *Reader) Header() Header {
	return Header{
		r.h.Version,
		r.h.Key,
		r.h.Flags,
		r.h.DataSize,
	}
}