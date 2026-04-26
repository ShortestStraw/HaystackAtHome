package otable_test

import (
	"errors"
	"testing"

	"HaystackAtHome/internal/ss/models"
	"HaystackAtHome/internal/ss/service/otable"
)

// ── helpers ──────────────────────────────────────────────────────────────────

const defaultMaxSize = uint64(1) << 30 // 1 GiB

func newTable() *otable.OTable {
	return otable.New()
}

func objInfo(key, dataSize, offset uint64) models.ObjInfo {
	return models.ObjInfo{Key: key, DataSize: dataSize, Offset: offset}
}

func isNotFound(err error) bool {
	return errors.Is(err, &models.ErrNotFound{})
}

func isExists(err error) bool {
	return errors.Is(err, &models.ErrExists{})
}

// addVol registers a volume with the given key and defaultMaxSize.
func addVol(t *testing.T, ot *otable.OTable, volKey uint64) {
	t.Helper()
	err := ot.AddVolume(models.Volume{
		Key:   volKey,
		Space: models.VolumeSpaceUsage{Free: defaultMaxSize},
	})
	if err != nil {
		t.Fatalf("AddVolume(%d): %v", volKey, err)
	}
}

// ── OInfo.From ───────────────────────────────────────────────────────────────

func TestOInfoFrom(t *testing.T) {
	src := models.ObjInfo{
		Key:      42,
		DataSize: 1024,
		Offset:   512,
		Flags:    models.ObjFlags{Deleted: true, CsMismatched: false},
	}
	var oi otable.OInfo
	oi.From(&src)

	if oi.DataSize != 1024 {
		t.Errorf("DataSize: got %d want 1024", oi.DataSize)
	}
	if oi.Off != 512 {
		t.Errorf("Off: got %d want 512", oi.Off)
	}
	if oi.Flags != src.Flags {
		t.Errorf("Flags: got %+v want %+v", oi.Flags, src.Flags)
	}
}

// ── AddVolume ────────────────────────────────────────────────────────────────

func TestAddVolumeNew(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
}

func TestAddVolumeDuplicate(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	err := ot.AddVolume(models.Volume{Key: 1, Space: models.VolumeSpaceUsage{Free: defaultMaxSize}})
	if !isExists(err) {
		t.Errorf("expected ErrExists on duplicate AddVolume, got %v", err)
	}
}

func TestAddVolumeMaxSizeIsStored(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 5)
	if got := ot.MaxSize(5); got != defaultMaxSize {
		t.Errorf("MaxSize: got %d want %d", got, defaultMaxSize)
	}
}

// ── DelVolume ────────────────────────────────────────────────────────────────

func TestDelVolumeRemovesVolume(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	if err := ot.DelVolume(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := ot.NumObjects(1); got != -1 {
		t.Errorf("NumObjects after DelVolume: got %d want -1", got)
	}
}

func TestDelVolumeMissing(t *testing.T) {
	ot := newTable()
	if err := ot.DelVolume(99); !isNotFound(err) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDelVolumeRemovesItsObjects(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(1, objInfo(20, 200, 0))

	_ = ot.DelVolume(1)

	if n := ot.ObjNum(); n != 0 {
		t.Errorf("ObjNum after DelVolume: got %d want 0", n)
	}
	if _, err := ot.Lookup(10); !isNotFound(err) {
		t.Error("Lookup should fail after DelVolume")
	}
}

func TestDelVolumeDoesNotAffectOtherVolumes(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	addVol(t, ot, 2)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(2, objInfo(20, 200, 0))

	_ = ot.DelVolume(1)

	if _, err := ot.Lookup(20); err != nil {
		t.Errorf("object in surviving volume should still be reachable: %v", err)
	}
}

// ── AddObj ───────────────────────────────────────────────────────────────────

func TestAddObjNewKey(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	prev := ot.AddObj(1, objInfo(100, 512, 0))
	if prev != (otable.OInfo{}) {
		t.Errorf("new key should return zero OInfo, got %+v", prev)
	}
}

func TestAddObjUnknownVolumeReturnsZero(t *testing.T) {
	ot := newTable()
	prev := ot.AddObj(99, objInfo(100, 512, 0))
	if prev != (otable.OInfo{}) {
		t.Errorf("unknown volume should return zero OInfo, got %+v", prev)
	}
	if _, err := ot.Lookup(100); !isNotFound(err) {
		t.Error("obj should not be stored when volume is unknown")
	}
}

func TestAddObjReturnsOldOnOverwrite(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(100, 512, 0))
	prev := ot.AddObj(1, objInfo(100, 1024, 64))

	if prev.DataSize != 512 {
		t.Errorf("prev.DataSize: got %d want 512", prev.DataSize)
	}
	if prev.Off != 0 {
		t.Errorf("prev.Off: got %d want 0", prev.Off)
	}
}

func TestAddObjOverwriteStoresNew(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(100, 512, 0))
	ot.AddObj(1, objInfo(100, 1024, 64))

	got, err := ot.Lookup(100)
	if err != nil {
		t.Fatalf("Lookup after overwrite: %v", err)
	}
	if got.DataSize != 1024 || got.Off != 64 {
		t.Errorf("new value not stored: got %+v", got)
	}
}

func TestAddObjMoveBetweenVolumes(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	addVol(t, ot, 2)
	ot.AddObj(1, objInfo(42, 100, 0))
	ot.AddObj(2, objInfo(42, 200, 64)) // move obj 42 from vol1 to vol2

	// Object is in vol2 now.
	lst := ot.List()
	if len(lst) != 1 {
		t.Fatalf("expected 1 entry after move, got %d", len(lst))
	}
	if lst[0].VolKey != 2 {
		t.Errorf("after move VolKey: got %d want 2", lst[0].VolKey)
	}
	// Vol1 has no objects.
	if n := ot.NumObjects(1); n != 0 {
		t.Errorf("vol1 NumObjects after move: got %d want 0", n)
	}
}

func TestAddObjMultipleKeys(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	keys := []uint64{10, 20, 30, 100, 999}
	for _, k := range keys {
		ot.AddObj(1, objInfo(k, k*10, k*100))
	}
	for _, k := range keys {
		got, err := ot.Lookup(k)
		if err != nil {
			t.Errorf("key %d: %v", k, err)
			continue
		}
		if got.DataSize != k*10 || got.Off != k*100 {
			t.Errorf("key %d: got %+v", k, got)
		}
	}
}

// ── Lookup ───────────────────────────────────────────────────────────────────

func TestLookupMissingKey(t *testing.T) {
	ot := newTable()
	_, err := ot.Lookup(42)
	if !isNotFound(err) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestLookupReturnsCorrectData(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 5)
	ot.AddObj(5, objInfo(77, 256, 128))

	got, err := ot.Lookup(77)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.DataSize != 256 || got.Off != 128 {
		t.Errorf("unexpected OInfo: %+v", got)
	}
}

func TestLookupEmptyTable(t *testing.T) {
	ot := newTable()
	_, err := ot.Lookup(0)
	if !isNotFound(err) {
		t.Errorf("expected ErrNotFound on empty table, got %v", err)
	}
}

// ── MarkDeleted ──────────────────────────────────────────────────────────────

func TestMarkDeletedSetsFlag(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))

	if err := ot.MarkDeleted(10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := ot.Lookup(10)
	if err != nil {
		t.Fatalf("Lookup after MarkDeleted: %v", err)
	}
	if !got.Flags.Deleted {
		t.Error("Flags.Deleted should be true")
	}
}

func TestMarkDeletedPreservesOtherFields(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 3)
	ot.AddObj(3, objInfo(10, 512, 256))
	_ = ot.MarkDeleted(10)

	got, _ := ot.Lookup(10)
	if got.DataSize != 512 || got.Off != 256 {
		t.Errorf("non-flag fields changed after MarkDeleted: %+v", got)
	}
}

func TestMarkDeletedMissingKey(t *testing.T) {
	ot := newTable()
	err := ot.MarkDeleted(99)
	if !isNotFound(err) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMarkDeletedIsIdempotent(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	_ = ot.MarkDeleted(10)
	if err := ot.MarkDeleted(10); err != nil {
		t.Errorf("second MarkDeleted should succeed, got %v", err)
	}
	got, _ := ot.Lookup(10)
	if !got.Flags.Deleted {
		t.Error("still should be deleted after second call")
	}
}

// ── DelObj ───────────────────────────────────────────────────────────────────

func TestDelObjRemovesKey(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))

	if err := ot.DelObj(10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := ot.Lookup(10); !isNotFound(err) {
		t.Errorf("key should be gone after DelObj, Lookup returned %v", err)
	}
}

func TestDelObjMissingKey(t *testing.T) {
	ot := newTable()
	err := ot.DelObj(42)
	if !isNotFound(err) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDelObjAfterMarkDeleted(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	_ = ot.MarkDeleted(10)

	if err := ot.DelObj(10); err != nil {
		t.Errorf("DelObj on marked-deleted key should succeed, got %v", err)
	}
	if _, err := ot.Lookup(10); !isNotFound(err) {
		t.Error("key should not exist after DelObj")
	}
}

func TestDelObjDoesNotAffectOtherKeys(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(1, 100, 0))
	ot.AddObj(1, objInfo(2, 200, 0))

	_ = ot.DelObj(1)

	if _, err := ot.Lookup(2); err != nil {
		t.Errorf("unrelated key should survive DelObj: %v", err)
	}
}

func TestDelObjUpdatesNumObjects(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(1, objInfo(20, 200, 0))
	_ = ot.DelObj(10)

	if n := ot.NumObjects(1); n != 1 {
		t.Errorf("NumObjects after DelObj: got %d want 1", n)
	}
}

// ── List ─────────────────────────────────────────────────────────────────────

func TestListEmpty(t *testing.T) {
	if lst := newTable().List(); len(lst) != 0 {
		t.Errorf("empty table: got %d items want 0", len(lst))
	}
}

func TestListReturnsAllAdded(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	addVol(t, ot, 2)
	addVol(t, ot, 3)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(2, objInfo(20, 200, 64))
	ot.AddObj(3, objInfo(30, 300, 128))

	lst := ot.List()
	if len(lst) != 3 {
		t.Fatalf("expected 3 items, got %d", len(lst))
	}

	byKey := make(map[uint64]otable.OInfoExt, len(lst))
	for _, ext := range lst {
		byKey[ext.Key] = ext
	}
	cases := []struct{ key, volKey, dataSize, off uint64 }{
		{10, 1, 100, 0},
		{20, 2, 200, 64},
		{30, 3, 300, 128},
	}
	for _, c := range cases {
		got, ok := byKey[c.key]
		if !ok {
			t.Errorf("missing entry for Key=%d", c.key)
			continue
		}
		if got.VolKey != c.volKey {
			t.Errorf("Key=%d VolKey: got %d want %d", c.key, got.VolKey, c.volKey)
		}
		if got.DataSize != c.dataSize || got.Off != c.off {
			t.Errorf("Key=%d: DataSize=%d Off=%d want %d %d",
				c.key, got.DataSize, got.Off, c.dataSize, c.off)
		}
	}
}

func TestListIncludesMarkDeleted(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	_ = ot.MarkDeleted(10)

	lst := ot.List()
	if len(lst) != 1 {
		t.Fatalf("expected 1 item after MarkDeleted, got %d", len(lst))
	}
	if !lst[0].Flags.Deleted {
		t.Error("expected Flags.Deleted=true in list entry")
	}
}

func TestListExcludesDelObj(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(1, objInfo(20, 200, 0))
	_ = ot.DelObj(10)

	lst := ot.List()
	if len(lst) != 1 {
		t.Fatalf("expected 1 item after DelObj, got %d", len(lst))
	}
	if lst[0].Key != 20 {
		t.Errorf("expected remaining entry Key=20, got %d", lst[0].Key)
	}
}

func TestListLengthMatchesObjNum(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	for i := range uint64(7) {
		ot.AddObj(1, objInfo(i, i*10, 0))
	}
	if got, want := len(ot.List()), int(ot.ObjNum()); got != want {
		t.Errorf("List len=%d != ObjNum=%d", got, want)
	}
}

func TestListSpansMultipleVolumes(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	addVol(t, ot, 2)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(2, objInfo(20, 200, 0))

	lst := ot.List()
	vols := make(map[uint64]bool)
	for _, ext := range lst {
		vols[ext.VolKey] = true
	}
	if !vols[1] || !vols[2] {
		t.Errorf("List should include entries from both volumes, got VolKeys: %v", vols)
	}
}

// ── NumObjects ────────────────────────────────────────────────────────────────

func TestNumObjectsMissingVolume(t *testing.T) {
	ot := newTable()
	if n := ot.NumObjects(99); n != -1 {
		t.Errorf("missing volume: got %d want -1", n)
	}
}

func TestNumObjectsEmpty(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	if n := ot.NumObjects(1); n != 0 {
		t.Errorf("empty volume: got %d want 0", n)
	}
}

func TestNumObjectsAfterAdds(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(1, objInfo(20, 200, 0))
	if n := ot.NumObjects(1); n != 2 {
		t.Errorf("got %d want 2", n)
	}
}

func TestNumObjectsPerVolumeIsIndependent(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	addVol(t, ot, 2)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(1, objInfo(20, 200, 0))
	ot.AddObj(2, objInfo(30, 300, 0))

	if n := ot.NumObjects(1); n != 2 {
		t.Errorf("vol1: got %d want 2", n)
	}
	if n := ot.NumObjects(2); n != 1 {
		t.Errorf("vol2: got %d want 1", n)
	}
}

// ── ObjNum ───────────────────────────────────────────────────────────────────

func TestObjNumEmpty(t *testing.T) {
	if n := newTable().ObjNum(); n != 0 {
		t.Errorf("empty table: got %d want 0", n)
	}
}

func TestObjNumAfterAdds(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	for i := range uint64(5) {
		ot.AddObj(1, objInfo(i, 100, 0))
		if got := ot.ObjNum(); got != uint32(i+1) {
			t.Errorf("after %d adds: got %d want %d", i+1, got, i+1)
		}
	}
}

func TestObjNumOverwriteDoesNotGrow(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(42, 100, 0))
	ot.AddObj(1, objInfo(42, 200, 64)) // same obj key
	if n := ot.ObjNum(); n != 1 {
		t.Errorf("overwrite should not grow count: got %d want 1", n)
	}
}

func TestObjNumAfterMarkDeleted(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	_ = ot.MarkDeleted(10)
	if n := ot.ObjNum(); n != 1 {
		t.Errorf("MarkDeleted should not reduce count: got %d want 1", n)
	}
}

func TestObjNumAfterDelObj(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(1, objInfo(20, 100, 0))
	_ = ot.DelObj(10)
	if n := ot.ObjNum(); n != 1 {
		t.Errorf("after DelObj: got %d want 1", n)
	}
}

// ── Size ─────────────────────────────────────────────────────────────────────

func TestMaxSizeMissingVolume(t *testing.T) {
	if got := newTable().MaxSize(1); got != 0 {
		t.Errorf("missing volume MaxSize: got %d want 0", got)
	}
}

func TestCurrentSizeMissingVolume(t *testing.T) {
	if got := newTable().CurrentSize(1); got != 0 {
		t.Errorf("missing volume CurrentSize: got %d want 0", got)
	}
}

func TestCurrentSizeGrowsOnAdd(t *testing.T) {
	ot := newTable()
	addVol(t, ot, 1)
	ot.AddObj(1, objInfo(10, 100, 0))
	ot.AddObj(1, objInfo(20, 200, 0))
	if got := ot.CurrentSize(1); got != 300 {
		t.Errorf("CurrentSize: got %d want 300", got)
	}
}
