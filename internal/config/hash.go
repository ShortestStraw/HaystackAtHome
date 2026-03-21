package config

import (
	"crypto/md5"
	"encoding/binary"
)

func md5Hash(path string) int {
	tmp := md5.Sum([]byte(path))
	/* Decode using bigEndian.
	 * We don't actually care, which endian does host use,
	 * as we are using same endian for every hashable string it should be alright
	 */
	var s32 int = int(binary.BigEndian.Uint32(tmp[:4]))
	return s32
}
