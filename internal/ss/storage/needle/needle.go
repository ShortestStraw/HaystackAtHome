/*
needle package implements Needle structure.

Needle is ondisk structure that represent object stored in SS volume
Needle is used for two goals: scaning volume, write and delete object
and it is not used for reading since SS store [key -> volume, offset, size] mapping in memory
so it can retrive object by simple reading volume [offset, offset + size) extent.
The mapping itself defined in storage implementation.

When scaning and writing new Needle checksum is calculated as crc64 of header (without flags) and data
*/
package needle

import (
	"encoding/binary"
	"io"
	"HaystackAtHome/internal/ss/models"
)

const (
	headerOndiskSize = uint64(52)
	footerOndiskSizeMin = uint64(16)
	footerOndiskSizeMax = uint64(23)

	needleAlignment = uint64(8)

	version1 uint32 = 1
	currentVersion uint32 = version1

	headerMagic uint64 = 0xABCDFEDCBA987654
	footerMagic uint64 = 0x1234567890ABCDEF

	DataShift = headerOndiskSize // Shift of object data offset relatively to its header offset
)

// Respresent needle ondisk header. see NeedleHeader type
type headerOndisk struct {
	Magic       uint64     `struc:"uint64,big"`
	Version     uint32     `struc:"uint32,big"`
	Key         uint64     `struc:"uint64,big"`
	Flags       uint64     `struc:"uint64,big"`
	DataSize    uint64     `struc:"uint64,big"`
	Reserved    [2]uint64  `struc:"[2]uint64,big"`
}

// Represent needle ondisk trailer
// pading pads the whole Needle to 8 bytes so 
// this struct ondisk have variable size.
// For encoding decoding use user defined encoder/decoder.
// Struct field annotations are for documentation purposes only 
// and not used in encoding/decoding.
type footerOndisk struct {
	Magic       uint64  `struc:"uint64,big"`  // Neadle footer magic
	Checksum    uint64  `struc:"uint64,big"`  // Neadle checksum: header (without flags) + data
	Padding     []byte  `struc:"[]byte,big"`  // Needle padding is 8 bytes in total: header + data + footer
}

type footerOndiskEncoder struct {
	footer   *footerOndisk
	padding  uint64
}

func footerOndiskEncoderFrom(footer *footerOndisk, padding uint64) *footerOndiskEncoder {
	return &footerOndiskEncoder{
		footer: footer,
		padding: padding,
	}
}

// Pack serializes the footer into the writer
func (encoder *footerOndiskEncoder) Pack(w io.Writer) error {
	// 8 bytes (Magic) + 8 bytes (Checksum) + padding
	totalSize := 16 + int(encoder.padding)
	buf := make([]byte, totalSize)

	// Write Magic and Checksum using Big Endian
	binary.BigEndian.PutUint64(buf[0:8], encoder.footer.Magic)
	binary.BigEndian.PutUint64(buf[8:16], encoder.footer.Checksum)

	// Fill padding if the slice exists, otherwise it remains zeros
	if len(encoder.footer.Padding) > 0 {
		copy(buf[16:], encoder.footer.Padding)
	}

	_, err := w.Write(buf)
	return err
}

type footerOndiskDecoder struct {
	footer   *footerOndisk
	padding  uint64
}

func footerOndiskDecoderFrom(footer *footerOndisk, padding uint64) *footerOndiskDecoder {
	return &footerOndiskDecoder{
		footer: footer,
		padding: padding,
	}
}

// Unpack deserializes the footer from the reader
func (decoder *footerOndiskDecoder) Unpack(r io.Reader) error {
	// Calculate expected size
	totalSize := 16 + int(decoder.padding)
	buf := make([]byte, totalSize)

	// CRITICAL: Use io.ReadFull to ensure we don't get a "short read"
	// and accidentally parse partial data as valid.
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}

	decoder.footer.Magic = binary.BigEndian.Uint64(buf[0:8])
	decoder.footer.Checksum = binary.BigEndian.Uint64(buf[8:16])

	// Extract padding
	decoder.footer.Padding = make([]byte, decoder.padding)
	copy(decoder.footer.Padding, buf[16:])

	return nil
}

func validateHeader(header *headerOndisk, off uint64) error {
	if header.Magic != headerMagic {
		return models.NewErrObjValidation("Header magic mismatch", off)
	}
	return nil
}

func validateFooter(footer *footerOndisk, off uint64, csum uint64) error {
	if footer.Magic != footerMagic {
		return models.NewErrObjValidation("Footer magic mismatch", off)
	}
	if footer.Checksum != csum {
		return models.NewErrObjCSMismatch("Checksum mismatch")
	}
	return nil
}

type Header struct {
	Version     uint32   // Needle enciding version
	Key         uint64   // underlying object uid
	Flags       uint64   // see NeedleFlags
	DataSize    uint64   // actual object size
}

// @off is offset on the beggining of header
// TODO
func MarkNeedleDeleted(rd io.ReaderAt, off uint64) error {
	return nil
}

// ------ For tests -------

type obj struct {
	key  uint64
	data []byte 
}

var(
	objs = []obj{
		{123, []byte("asdfghjkl")},
		{1234, []byte("asdfghjklzxcvb")},
		{1235, []byte("asdfghjklzxcvbzxcv")},
		{1236, []byte("asdfghjklzxcvbmnbvcxz")},
		{1237, []byte("asdfghjklzxcvbmnbvcxzzxv")},
		{1238, []byte("asdfghjklzxcvbmnbvcxzzxcvb")},
		{1239, []byte("asdfghjklzxcvbmnbvcxzzxvcnbbn")},
		{12310, []byte("asdfghjklzxcvbmnbvcxzxcguioaffgd")},
		{12311, []byte("asdfghjklzxcvbmnbvcxzxcguioaffgdedrf")},
		{12312, []byte("asdfghjklzxcvbmnbvcxzxcguioaffgdwsdxcfvgbhnjmk")},
		{12313, []byte("asdfghjklzxcvbmnbvcxzxcguioaffgd1qwsedrfghnjmk,l.sdvfz`1234567890-~\\xz??|!@#$$%^&*()_+-=")},
	}
)