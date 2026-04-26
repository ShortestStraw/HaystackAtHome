// Object info table: maps [objKey] -> [OInfo, volKey] across multiple volumes.
// The core of the SS service. Designed to store several GiB of object info.
package otable

import (
	"HaystackAtHome/internal/ss/models"
)

// TODO: Flags and VolKey can be compressed into a common 32-bit field.
type OInfo struct {
	Off      uint64
	DataSize uint64
	Flags    models.ObjFlags
}

// OInfoExt is OInfo extended with the object key and the volume it lives in.
type OInfoExt struct {
	OInfo
	Key    uint64
	VolKey uint64
}

// From populates oi from objInfo and returns oi for chaining.
func (oi *OInfo) From(objInfo *models.ObjInfo) *OInfo {
	oi.DataSize = objInfo.DataSize
	oi.Flags = objInfo.Flags
	oi.Off = objInfo.Offset
	return oi
}

type VolInfo struct {
	maxSize uint64
	curSize uint64
	volKey  uint64
	m       map[uint64]OInfo // persistent data (already on-disk)
}

func newVolInfo(maxSize, volKey uint64) *VolInfo {
	return &VolInfo{
		m:       make(map[uint64]OInfo),
		maxSize: maxSize,
		volKey:  volKey,
	}
}

func (vi *VolInfo) numObjects() int     { return len(vi.m) }
func (vi *VolInfo) maximumSize() uint64 { return vi.maxSize }
func (vi *VolInfo) currentSize() uint64 { return vi.curSize }

// addObj stores oi for objKey and returns the previous value (zero if new).
func (vi *VolInfo) addObj(objKey uint64, oi OInfo) OInfo {
	old, ok := vi.m[objKey]
	vi.m[objKey] = oi
	vi.curSize += oi.DataSize
	if ok {
		return old
	}
	return OInfo{}
}

type OTable struct {
	m    map[uint64]uint64    // [objKey -> volKey]
	vols map[uint64]*VolInfo  // [volKey -> *VolInfo]
}

func New() *OTable {
	return &OTable{
		m:    make(map[uint64]uint64),
		vols: make(map[uint64]*VolInfo),
	}
}

// AddVolume registers a volume. Returns ErrExists if it already exists.
func (ot *OTable) AddVolume(vol models.Volume) error {
	if _, ok := ot.vols[vol.Key]; ok {
		return models.NewErrExists("volume already exists")
	}
	ot.vols[vol.Key] = newVolInfo(vol.Space.Free+vol.Space.Used, vol.Key)
	return nil
}

// DelVolume removes a volume and all its objects from the index.
// Returns ErrNotFound if the volume does not exist.
func (ot *OTable) DelVolume(volKey uint64) error {
	vi, ok := ot.vols[volKey]
	if !ok {
		return models.NewErrNotFound("no such volume")
	}
	for objKey := range vi.m {
		delete(ot.m, objKey)
	}
	delete(ot.vols, volKey)
	return nil
}

// NumObjects returns the object count for a volume, or -1 if it does not exist.
func (ot *OTable) NumObjects(volKey uint64) int {
	vi, ok := ot.vols[volKey]
	if !ok {
		return -1
	}
	return vi.numObjects()
}

// MaxSize returns the maximum capacity of a volume (0 if not found).
func (ot *OTable) MaxSize(volKey uint64) uint64 {
	vi, ok := ot.vols[volKey]
	if !ok {
		return 0
	}
	return vi.maximumSize()
}

// CurrentSize returns the accumulated written bytes of a volume (0 if not found).
func (ot *OTable) CurrentSize(volKey uint64) uint64 {
	vi, ok := ot.vols[volKey]
	if !ok {
		return 0
	}
	return vi.currentSize()
}

// AddObj records obj into the given volume and returns the previous OInfo for
// that key (zero value if new). Returns zero silently if the volume is unknown.
// If the key previously lived in a different volume it is moved out of that volume.
func (ot *OTable) AddObj(volKey uint64, obj models.ObjInfo) OInfo {
	vi, ok := ot.vols[volKey]
	if !ok {
		return OInfo{}
	}

	var new_oi OInfo
	new_oi.From(&obj)

	// Move: evict from old volume when the object is migrating.
	if oldVolKey, exists := ot.m[obj.Key]; exists && oldVolKey != volKey {
		if oldVi, ok := ot.vols[oldVolKey]; ok {
			delete(oldVi.m, obj.Key)
		}
	}

	ot.m[obj.Key] = volKey
	return vi.addObj(obj.Key, new_oi)
}

// Lookup returns the OInfo for key. ErrNotFound if not found.
func (ot *OTable) Lookup(key uint64) (OInfo, error) {
	volKey, ok := ot.m[key]
	if !ok {
		return OInfo{}, models.NewErrNotFound("no obj with such key")
	}
	vi, ok := ot.vols[volKey]
	if !ok {
		return OInfo{}, models.NewErrNotFound("no obj with such key")
	}
	return vi.m[key], nil
}

// LookupExt returns OInfoExt (including VolKey) for key. ErrNotFound if not found.
func (ot *OTable) LookupExt(key uint64) (OInfoExt, error) {
	volKey, ok := ot.m[key]
	if !ok {
		return OInfoExt{}, models.NewErrNotFound("no obj with such key")
	}
	vi, ok := ot.vols[volKey]
	if !ok {
		return OInfoExt{}, models.NewErrNotFound("no obj with such key")
	}
	return OInfoExt{OInfo: vi.m[key], Key: key, VolKey: volKey}, nil
}

// List returns all objects across all volumes, including soft-deleted ones.
func (ot *OTable) List() []OInfoExt {
	lst := make([]OInfoExt, 0, len(ot.m))
	for _, vi := range ot.vols {
		for k, oi := range vi.m {
			lst = append(lst, OInfoExt{OInfo: oi, Key: k, VolKey: vi.volKey})
		}
	}
	return lst
}

// MarkDeleted soft-deletes key. ErrNotFound if not found.
func (ot *OTable) MarkDeleted(key uint64) error {
	volKey, ok := ot.m[key]
	if !ok {
		return models.NewErrNotFound("no obj with such key")
	}
	vi, ok := ot.vols[volKey]
	if !ok {
		return models.NewErrNotFound("no obj with such key")
	}
	oi := vi.m[key]
	oi.Flags.Deleted = true
	vi.m[key] = oi
	return nil
}

// DelObj removes key from the table. ErrNotFound if not found.
func (ot *OTable) DelObj(key uint64) error {
	volKey, ok := ot.m[key]
	if !ok {
		return models.NewErrNotFound("no obj with such key")
	}
	vi, ok := ot.vols[volKey]
	if !ok {
		return models.NewErrNotFound("no obj with such key")
	}
	delete(vi.m, key)
	delete(ot.m, key)
	return nil
}

// ObjNum returns the total number of objects across all volumes.
func (ot *OTable) ObjNum() uint32 {
	return uint32(len(ot.m))
}
