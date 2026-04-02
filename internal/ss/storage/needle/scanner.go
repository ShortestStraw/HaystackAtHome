package needle

import (
	"bytes"
	"context"
	"errors"
	"hash"
	"io"
	"log/slog"

	"HaystackAtHome/internal/ss/models"

	"github.com/lunixbochs/struc"
)

const (
	iterBufferSizeDefault = 128 * 1024 // 128 KiB
)

// Iterator over ondisk needles for scaning volume. All methods are thread unsafe
type Iter struct {
	volFd         io.ReaderAt
	off           uint64 // total bytes read from volume to buffers
	cursor        uint64 // total needles read in bytes from buffers (or volume in case of disabled check summing)
	currHeader    *headerOndisk
	currFooter	  *footerOndisk
	nextHeader    *headerOndisk // used only if csum is disabled
	
	nextHeaderOffset uint64
	currHeaderOffset uint64

	logger        *slog.Logger
	cs            hash.Hash64
	buf           *[]byte  // points of ra.buf
	bufReader     io.Reader
	ra struct {
		bufs        [2][]byte
		buf         *[]byte  // if nil than ra is turned off
		toggle      chan struct{} /* on toggle ra and it switch buffers:
		                             it switches it_buf and ra switches ra_buf.
		                          */
		read        uint64
		off         uint64
		err         error
	}
}

type WithOption func(*Iter)

func WithLogger(logger *slog.Logger) WithOption {
	return func(it *Iter) {
		it.logger = logger
	}
}

// algorithm must return 64 bit number
func WithChecksumAlg(cs hash.Hash64) WithOption {
	return func(it *Iter) {
		it.cs = cs
	}
}

// Must be called before WithReadahead because it uses buffer size for readahead buffer
func WithBufferSize(bufSize int) WithOption {
	return func(it *Iter) {
		it.ra.bufs[0] = make([]byte, bufSize)
		it.buf = &it.ra.bufs[0]
	}
}

func WithReadahead() WithOption {
	return func(it *Iter) {
		var sz int
		if it.buf != nil {
			sz = len(*it.buf)
		} else {
			sz = iterBufferSizeDefault
		}
		it.ra.bufs[1] = make([]byte, sz)
		it.ra.buf = &it.ra.bufs[1]
		it.ra.toggle = make(chan struct{}, 1)
		it.ra.err = nil
	}
}

func WithStartOffset(off uint64) WithOption {
	return func(it *Iter) {
		it.off = off
		it.ra.off = off
		it.cursor = off
	}
}

/*
Create new NeedleIter for scaning needles. It designed to be used by storage when it reassembles
[key -> volume, offset, size] mapping of needles.

for WithStartOffset @off is start offset of scaning. If used must be point on needle header beginning. 
NewIter will scan header magic and previous footer magic to validate but keep in mind
validation can be pass even in the middle of object data

If @off is zero than scanner will start from beginnig of volume and validate only header magic

For damaged needle io.ErrValidation is returned, but iterator still accessable.
When all needles have been read io.EOF is returned. 
For other errors io.ReadAt and context errors can be returned.

The behavior of scanner is undefined if @vol_fd is modified during scaning. 
You should not modify @vol_fd until you call NeedleIter.Close().

Iterator buffers read from disk. Default size is 128 KiB. 
You can change it with WithBufferSize option. 

If you want to use readahead you should use WithReadahead option. 
Readahead is implemented with one extra buffer and toggle channel. 
When readahead is on scanner read data into one of buffers and toggle 
channel is used to switch buffers between scanner and readahead goroutine. 
Readahead goroutine read data into buffer which is not used by scanner and when it finish 
it toggles channel to switch buffers. This way scanner can read data from one buffer while 
readahead is reading data into another buffer. 
Readahead buffer size is the same as for iterator buffer. 
*/
func NewIter(ctx context.Context, vol_fd io.ReaderAt, options... WithOption) (it *Iter, err error) {
	it = &Iter{
		volFd: vol_fd,
		currHeader: nil,
		currFooter: nil,
		nextHeader: nil,
		cs: nil,
		cursor: 0,
	}

	for _, option := range options {
		option(it)
	}

	if it.buf == nil {
		it.ra.bufs[0] = make([]byte, iterBufferSizeDefault)
		it.buf = &it.ra.bufs[0]
	}

	if it.logger != nil {
		it.logger.Info("NewIter", "start_offset", it.off, "buffer_size", len(*it.buf), "readahead", it.ra.buf != nil)
	}
	if it.ra.buf != nil && it.cs == nil {
		if it.logger != nil{
			it.logger.Warn("readahead enabled without checksum context, readahead will be turned off")
		}
		it.ra.buf = nil
		it.ra.bufs[1] = nil
		close(it.ra.toggle)
	}

	if it.cs == nil {
		// in case of disabled cs and ra we dont need to read full volume, just headers and footers
		// TODO implement
		return nil, &models.ErrUnimplemented{}
	}

	if it.ra.buf != nil {
		go it.runRa(ctx)
		if err = it.getRa(ctx); err != nil {
			return nil, err
		}
	} else if it.cs != nil {
		if err = it.fillBuf(ctx); err != nil {
			return nil, err
		}
	}

	return it, err
}

func (it *Iter) getRa(ctx context.Context) error {
	// check that current buffer is done
	if it.bufReader != nil && it.bufReader.(*bytes.Reader).Len() != 0 {
		return nil
	}

	if it.ra.toggle == nil {
		return it.ra.err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-it.ra.toggle:
		it.toggleRABufsSwitch()
	}

	err := it.ra.err
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		err = nil
	}
	if it.ra.err != nil {
		close(it.ra.toggle)
		it.ra.toggle = nil
	} else {
		go it.runRa(ctx)
	}
	
	return err
}

func (it *Iter) toggleRABufsSwitch() {
	it.buf, it.ra.buf = it.ra.buf, it.buf
	it.bufReader = bytes.NewReader((*it.buf)[:it.ra.off - it.off])
	it.off = it.ra.off
}

func (it *Iter) runRa(ctx context.Context) {
	if it.ra.buf == nil || len(*it.ra.buf) == 0 {
		return
	}
	
	total := uint64(0)
	defer func(total uint64) { it.ra.toggle <- struct{}{}; it.ra.read = total }(total)
	
	bufLen := uint64(len(*it.ra.buf))

	for total < bufLen {
		if it.ra.err != nil {
			return
		}

		n, err := it.volFd.ReadAt((*it.ra.buf)[total:], int64(it.ra.off))
		it.ra.off += uint64(n)
		total += uint64(n)

		select {
		case <-ctx.Done():
			it.ra.err = ctx.Err()
			return
		default:
		}
		
		// eliminate EOF when total is not null
		if err != nil && (total == 0 || !(errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF))) {
			if total != 0 && errors.Is(err, io.ErrUnexpectedEOF) {
				err = io.EOF
			}
			it.ra.err = err
			return
		}
		if n == 0 {
			return
		}
	}
}

func (it *Iter) fillBuf(ctx context.Context) error {
	total := uint64(0)
	defer func() {
		it.bufReader = bytes.NewReader((*it.buf)[:total])
	} ()
	bufLen := uint64(len(*it.buf))
	for total < bufLen {
		n, err := it.volFd.ReadAt((*it.buf)[total:], int64(it.off))
		it.off += uint64(n)
		total += uint64(n)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// eliminate EOF when total is not null
		if err != nil && (total == 0 || !(errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF))) {
			if total != 0 && errors.Is(err, io.ErrUnexpectedEOF) {
				err = io.EOF
			}
			return err
		}
		if n == 0 {
			if total == 0 {
				return io.EOF
			} else {
				return nil
			}
		}
	}
	return nil
}

func (it *Iter) getHeaderOndisk(ctx context.Context, h *headerOndisk) (err error) {
	// Additional buffer is needed if structure is splited between 
	// two subsequent buffers
	buf := bytes.NewBuffer(make([]byte, 0, headerOndiskSize))
	to_read := headerOndiskSize
	for to_read > 0 {
		read, err := io.CopyN(buf, it.bufReader, int64(to_read))
		to_read -= uint64(read)
		it.cursor += uint64(read)

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if it.ra.buf != nil {
				err = it.getRa(ctx)
			} else {
				err = it.fillBuf(ctx)
			}
		}
		if err != nil {
			return err
		}
	}

	if err = struc.Unpack(buf, h); err != nil {
		return err
	}

	return nil
}

func calcFooterPadding(dataSize uint64) uint64 {
	needlePureLen := headerOndiskSize + dataSize
	rem := needlePureLen % needleAlignment
	pad := needleAlignment - rem
	if pad == needleAlignment {
		pad = 0
	}
	return pad
}

func (it *Iter) getFooterOndisk(ctx context.Context,f *footerOndisk, pad uint64) (err error) {
	// As for header additional buffer is needed if structure is splited between 
	// two subsequent buffers
	buf := bytes.NewBuffer(make([]byte, 0, footerOndiskSizeMin + pad))
	to_read := footerOndiskSizeMin + pad
	for to_read > 0 {
		read, err := io.CopyN(buf, it.bufReader, int64(to_read))
		to_read -= uint64(read)
		it.cursor += uint64(read)

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if it.ra.buf != nil {
				err = it.getRa(ctx)
			} else {
				err = it.fillBuf(ctx)
			}
		}
		if err != nil {
			return err
		}
	}

	dec := footerOndiskDecoderFrom(f, pad)
	if err := dec.Unpack(buf); err != nil {
		return nil
	}
	return nil
}

// write header (without flags) and data to it.cs 
func (it *Iter) readForCheckSum(ctx context.Context, h *headerOndisk) error {
	// For calculating hash we eliminate flags from header by temporary set them to zero.
	savedFlags := h.Flags
	h.Flags = 0
	if err := struc.Pack(it.cs, h); err != nil {
		return err
	}
	h.Flags = savedFlags

	to_read := h.DataSize
	for to_read > 0 {
		read, err := io.CopyN(it.cs, it.bufReader, int64(to_read))
		to_read -= uint64(read)
		it.cursor += uint64(read)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if it.ra.buf != nil {
				err = it.getRa(ctx)
			} else {
				err = it.fillBuf(ctx)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// fill it.current_header and it.current_footer, validate them and verify checksum
func (it *Iter) getNeedle(ctx context.Context) error {
	it.cs.Reset()

	it.currHeader = &headerOndisk{}
	it.currHeaderOffset = it.cursor
	if err := it.getHeaderOndisk(ctx, it.currHeader); err != nil {
		return err
	}
	
	if err := validateHeader(it.currHeader, it.cursor); err != nil {
		return err
	}

	if err := it.readForCheckSum(ctx, it.currHeader); err != nil {
		return err
	}

	pad := calcFooterPadding(it.currHeader.DataSize)
	footer := &footerOndisk{}
	if err := it.getFooterOndisk(ctx, footer, pad); err != nil {
		return err
	}

	cs := it.cs.Sum64()
	if err := validateFooter(footer, it.cursor, cs); err != nil {
		return err
	}

	if it.logger != nil {
		it.logger.Debug("needle", "off", it.currHeaderOffset, "key", it.currHeader.Key, "flags", it.currHeader.Flags, "dataSize", it.currHeader.DataSize)
	}
	return nil
}

func (it *Iter) Close() (error) {
	if it.logger != nil {
		it.logger.Debug("Close", "off", it.off)
	}
	return nil
}

/*
Get the next needle header. Footer is not returned because user actually
dont need this ondisk for using needles.

On last call return nil, io.EOF.
Other errors are same as for NewIter.
*/
func (it *Iter) Next(ctx context.Context) (nh *Header, err error) {
	eTarget1 := &models.ErrObjValidation{}
	eTarget2 := &models.ErrObjCSMismatch{}
	if err = it.getNeedle(ctx); err != nil && !errors.As(err, &eTarget1) && !errors.As(err, &eTarget2){
		return nil, err
	} else {
		nh = &Header{
			Version: it.currHeader.Version,
			Key: it.currHeader.Key,
			Flags: it.currHeader.Flags,
			DataSize: it.currHeader.DataSize,
		}
		return nh, err
	}
}

// Return in volume offset of current needle header
func (it *Iter) Offset() uint64 { return it.currHeaderOffset }