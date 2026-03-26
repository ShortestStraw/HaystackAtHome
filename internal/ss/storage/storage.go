package storage

import (
	"log/slog"
)
/*
	On disk storage organized as directory on filesystem
	storage_root/
	- .index
	- .volume.0
	- .volume.1
	...

	Storage have very slow start. When it started it scanned storage_root 
	for .volume.$i and .index files, then scan index file to retrive [object -> volume] mapping
	to memory. But .index file may be delayed for volume files, so .volume.$i will be scanned from 
	last offset, indexed in .index, to the end and update [object -> volume] mapping.

	When volume effective space become lower effectiveSpaceThreshold 
*/
type Storage struct {
	logger   *slog.Logger

	effectiveSpaceThreshold   uint64
}

// func New(path string, flags int, logger *slog.Logger) *Storage {
// 	var d os.File
// 	if d, err := os.OpenFile(path, flag, 0x644); err != nil {

// 	}
// 	return &Storage{
// 		d: ,

// 	}
// }