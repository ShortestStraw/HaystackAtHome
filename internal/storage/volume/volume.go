/* Volume package implements VolumeDescriptor and Volume structures */
package volume

type VolumeDescriptor struct {
	Magic   uint64
	Id      uint64
	MaxSize uint64
	Version uint32
}
