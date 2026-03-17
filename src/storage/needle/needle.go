/* Needle package implements Needle structure */
package needle

/** Needle is object metadata and data perceived as one entity
 *  Needles are stored on disk in volume files
 */
type Needle struct {
	Magic       uint64
	Size        uint64
	Version     uint32
	Owner       [16]byte
	KeySize     uint32
	Key         []byte
	Flags       uint64
	DataSize    uint32
	Data        []byte
	FooterMagic uint64
	Checksum    uint64
	Padding     []byte
}
