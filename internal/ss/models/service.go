package models

import (
	"context"
	"io"
)

type ServiceFeatures struct {
	Checksum bool
}

type SpaceUsage struct {
	Total uint64
	Used  uint64
}

type ServiceInfo struct {
	UID      uint64
	Space    SpaceUsage
	Features ServiceFeatures
}

type ObjMeta struct {
	Key  uint64
	Size uint64
}

// Service abstracts storage from the caller: it manages volume selection,
// write distribution, and the object index.
type Service interface {
	GetServiceInfo(ctx context.Context) (ServiceInfo, error)

	// GetObjsMap returns metadata for all live objects across storage
	GetObjsMap(ctx context.Context) ([]ObjMeta, error)

	// PutObj returns a WriteCloser to which exactly dataSize bytes must be written.
	// On successful Close the object is durable and readable.
	PutObj(ctx context.Context, objKey, dataSize uint64) (io.WriteCloser, error)

	// GetObj returns a ReadCloser over the object data and the object size.
	GetObj(ctx context.Context, objKey uint64) (io.ReadCloser, uint64, error)

	DelObj(ctx context.Context, objKey uint64) error

	Stop(ctx context.Context) error
}
