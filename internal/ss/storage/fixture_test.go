package storage

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"HaystackAtHome/internal/ss/storage/needle"
)

const (
	fixturePath     = "testcase/.root.3"
	fixtureSeed     = int64(42)
	fixtureVolMaxSz = uint64(1) << 30 // 1 GiB
)

var fixtureCounts = [3]int{5, 15, 50}

// needleFixture holds the needle header as stored on disk and the needle's
// in-volume offset (header start).
type needleFixture struct {
	needle.Header
	Off uint64
}

// storDesc[volKey][needleKey] — volKey in {1,2,3}, needleKey in {0..count-1}.
var storDesc map[uint64][]needleFixture

// generateFixture recreates testcase/.root.3 with three volumes (5, 15, 50
// needles) and populates storDesc. Needle sizes are drawn from a fixed-seed
// RNG so the fixture is identical across runs.
func generateFixture() error {
	rng := rand.New(rand.NewSource(fixtureSeed))

	os.RemoveAll(fixturePath)
	if err := os.MkdirAll(fixturePath, 0o775); err != nil {
		return fmt.Errorf("mkdir %s: %w", fixturePath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stor, err := Open(ctx, fixturePath, WithObjectChecksumming())
	if err != nil {
		return fmt.Errorf("Open: %w", err)
	}
	defer stor.Close(ctx)

	desc := make(map[uint64][]needleFixture)
	buf := make([]byte, 64*1024)

	for i, count := range fixtureCounts {
		volKey, err := stor.AddVolume(ctx, fmt.Sprintf("%d", i+1), fixtureVolMaxSz)
		if err != nil {
			return fmt.Errorf("AddVolume %d: %w", i+1, err)
		}

		entries := make([]needleFixture, count)
		for j := 0; j < count; j++ {
			const (
				minSz = 1 << 10       // 1 KiB
				maxSz = 16 << 20      // 16 MiB
			)
			dataSize := uint64(minSz + rng.Int63n(maxSz-minSz+1))
			objKey := uint64(j)

			wr, off, err := stor.PutObjectWriter(volKey, objKey, dataSize)
			if err != nil {
				return fmt.Errorf("PutObjectWriter vol=%d obj=%d: %w", volKey, objKey, err)
			}

			for remaining := dataSize; remaining > 0; {
				chunk := uint64(len(buf))
				if chunk > remaining {
					chunk = remaining
				}
				_, err := wr.Write(buf[:chunk])
				remaining -= chunk
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("Write vol=%d obj=%d: %w", volKey, objKey, err)
				}
			}

			if err := wr.Close(); err != nil {
				return fmt.Errorf("Close vol=%d obj=%d: %w", volKey, objKey, err)
			}

			entries[j] = needleFixture{
				Header: needle.Header{
					Version:  1,
					Key:      objKey,
					Flags:    needle.DefaultFlags,
					DataSize: dataSize,
				},
				Off: off,
			}
		}
		desc[volKey] = entries
	}

	storDesc = desc
	return nil
}
