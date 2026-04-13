package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── Constants ────────────────────────────────────────────────────────────────

const (
	benchPreFillDir = testcase + ".bench.prefill"
	benchMetaFile   = testcase + ".bench.meta.json"
	benchMaxLoops   = 4
	benchMaxVols    = 20
	// 1 GiB of object data pre-filled per volume (~1500 objects at mean≈700 KiB).
	benchPreFillSz = 1 << 30
	// 3 GiB max volume size: 1 GiB pre-fill + up to 2 GiB of bench writes.
	benchVolMaxSz = 3 << 30

	benchWritePerVol = 512 << 20 // 0.5 GiB writes per volume per loop
	benchMemLimit    = 8 * (1 << 30) // 8 GiB memory cap
	benchRandBufSz   = 1 << 20       // 1 MiB cycling random source buffer
	benchFillBlkSz   = 256 << 10     // 256 KiB chunks used during pre-fill I/O

	// Log-normal params for object sizes.
	// median = e^mu  ≈ 400 KiB,  mean = e^(mu+σ²/2) ≈ 700 KiB
	benchObjLNMu    = 12.923 // ln(400 * 1024)
	benchObjLNSigma = 1.058
	benchObjMinSz   = 1024 // 1 KiB hard floor
)

// ── Types ────────────────────────────────────────────────────────────────────

type benchObjMeta struct {
	Key  uint64 `json:"key"`
	Off  uint64 `json:"off"`
	Size uint64 `json:"size"`
}

type benchVolMeta struct {
	VolKey uint64         `json:"vol_key"`
	Objs   []benchObjMeta `json:"objs"`
}

type benchMeta struct {
	Vols []benchVolMeta `json:"vols"`
}

// benchVolMetrics holds per-volume atomic performance counters accumulated
// across all loops of a sub-benchmark.
type benchVolMetrics struct {
	writeBytes atomic.Uint64
	readBytes  atomic.Uint64
	writeOps   atomic.Uint64
	readOps    atomic.Uint64
	writeUsec  atomic.Uint64 // sum of individual op durations in µs
	readUsec   atomic.Uint64
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// benchLNSize returns a log-normally distributed object size ≥ benchObjMinSz.
func benchLNSize(rng *rand.Rand) uint64 {
	sz := uint64(math.Exp(benchObjLNMu + benchObjLNSigma*rng.NormFloat64()))
	if sz < benchObjMinSz {
		sz = benchObjMinSz
	}
	return sz
}

// benchRandBuf generates a 1 MiB non-zero random buffer used as cycling write
// source data throughout the benchmark.  Every byte has its LSB forced to 1 so
// the buffer is guaranteed to be non-zero.
func benchRandBuf() []byte {
	buf := make([]byte, benchRandBufSz)
	rng := rand.New(rand.NewPCG(0xdeadbeef, 0xcafebabe))
	for i := range buf {
		b := byte(rng.Uint32())
		if b == 0 {
			b = 0xFF
		}
		buf[i] = b
	}
	return buf
}

// benchWriteObj writes one object of exactly sz bytes to stor, cycling through
// randBuf starting at *bufOff.  Returns (bytesWritten, error).
func benchWriteObj(
	stor *Storage,
	volKey, key, sz uint64,
	blkSz int,
	randBuf []byte,
	bufOff *int,
) (uint64, error) {
	wr, _, err := stor.PutObjectWriter(volKey, key, sz)
	if err != nil {
		return 0, err
	}
	written := uint64(0)
	for written < sz {
		chunk := uint64(blkSz)
		if chunk > sz-written {
			chunk = sz - written
		}
		end := *bufOff + int(chunk)
		if end > len(randBuf) {
			end = len(randBuf)
			chunk = uint64(end - *bufOff)
		}
		n, werr := wr.Write(randBuf[*bufOff:end])
		written += uint64(n)
		*bufOff = (*bufOff + n) % len(randBuf)
		if werr == io.EOF {
			break
		}
		if werr != nil {
			_ = wr.Close()
			return written, fmt.Errorf("write vol=%d key=%d: %w", volKey, key, werr)
		}
	}
	if err := wr.Close(); err != nil {
		return written, fmt.Errorf("close vol=%d key=%d: %w", volKey, key, err)
	}
	return written, nil
}

// ── Pre-fill ─────────────────────────────────────────────────────────────────

// benchFillVol pre-fills a single volume with object data until totalWritten
// reaches sizTarget bytes (or the volume is full), returning metadata for each
// written object.
func benchFillVol(
	b *testing.B,
	stor *Storage,
	volKey, firstKey uint64,
	sizTarget uint64,
	rng *rand.Rand,
	randBuf []byte,
) []benchObjMeta {
	b.Helper()
	objs := make([]benchObjMeta, 0, 1600) // ~1 GiB / 700 KiB mean
	bufOff := 0
	totalWritten := uint64(0)

	for i := 0; totalWritten < sizTarget; i++ {
		if i > 0 && i%200 == 0 {
			fmt.Fprintf(os.Stderr, "    vol=%d: %d objects, %.1f/%.1f MiB\n",
				volKey, i, float64(totalWritten)/(1<<20), float64(sizTarget)/(1<<20))
		}

		sz := benchLNSize(rng)
		key := firstKey + uint64(i)

		wr, off, err := stor.PutObjectWriter(volKey, key, sz)
		if err == io.EOF {
			fmt.Fprintf(os.Stderr, "    vol=%d: volume full at object %d (wrote %.1f MiB)\n",
				volKey, i, float64(totalWritten)/(1<<20))
			break
		}
		if err != nil {
			b.Fatalf("benchFillVol PutObjectWriter vol=%d obj=%d: %v", volKey, i, err)
		}

		written := uint64(0)
		for written < sz {
			chunk := uint64(benchFillBlkSz)
			if chunk > sz-written {
				chunk = sz - written
			}
			end := bufOff + int(chunk)
			if end > len(randBuf) {
				end = len(randBuf)
				chunk = uint64(end - bufOff)
			}
			n, werr := wr.Write(randBuf[bufOff:end])
			written += uint64(n)
			bufOff = (bufOff + n) % len(randBuf)
			if werr == io.EOF {
				break
			}
			if werr != nil {
				b.Fatalf("benchFillVol Write vol=%d key=%d: %v", volKey, key, werr)
			}
		}
		if err := wr.Close(); err != nil {
			b.Fatalf("benchFillVol Close vol=%d key=%d: %v", volKey, key, err)
		}

		objs = append(objs, benchObjMeta{Key: key, Off: off, Size: sz})
		totalWritten += written
	}
	return objs
}

// benchEnsurePreFill returns pre-fill metadata, creating the pre-fill storage
// from scratch if it does not already exist on disk.
func benchEnsurePreFill(b *testing.B, randBuf []byte) *benchMeta {
	b.Helper()

	if meta, ok := benchLoadMeta(); ok {
		totalObjs := 0
		for _, v := range meta.Vols {
			totalObjs += len(v.Objs)
		}
		fmt.Fprintf(os.Stderr, "pre-fill: loaded from cache (%d volumes, %d objects total, %.1f GiB/vol target)\n",
			len(meta.Vols), totalObjs, float64(benchPreFillSz)/(1<<30))
		return meta
	}

	fmt.Fprintf(os.Stderr, "pre-fill: creating %s — %d volumes × %.1f GiB each...\n",
		benchPreFillDir, benchMaxVols, float64(benchPreFillSz)/(1<<30))
	t0 := time.Now()

	os.RemoveAll(benchPreFillDir)
	if err := os.Mkdir(benchPreFillDir, 0o777); err != nil {
		b.Fatalf("mkdir %s: %v", benchPreFillDir, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	stor, err := Open(ctx, benchPreFillDir, WithObjectChecksumming())
	if err != nil {
		b.Fatalf("open pre-fill storage: %v", err)
	}
	defer func() {
		closeCtx, cc := context.WithTimeout(context.Background(), time.Minute)
		defer cc()
		if err := stor.Close(closeCtx); err != nil {
			b.Fatalf("close pre-fill storage: %v", err)
		}
	}()

	rng := rand.New(rand.NewPCG(42, 0))
	meta := &benchMeta{Vols: make([]benchVolMeta, 0, benchMaxVols)}

	for v := range benchMaxVols {
		volKey := uint64(v + 1) // 1-based keys: 1..20
		fmt.Fprintf(os.Stderr, "pre-fill: volume %d/%d (key=%d)...\n",
			v+1, benchMaxVols, volKey)
		vt := time.Now()

		vk, err := stor.AddVolume(ctx, fmt.Sprintf("%d", volKey), benchVolMaxSz)
		if err != nil {
			b.Fatalf("AddVolume pre-fill vol=%d: %v", volKey, err)
		}

		firstKey := uint64(v*10000) + 1 // generous key space per volume (10k slots)
		objs := benchFillVol(b, stor, vk, firstKey, benchPreFillSz, rng, randBuf)

		totalWritten := uint64(0)
		for _, o := range objs {
			totalWritten += o.Size
		}
		fmt.Fprintf(os.Stderr, "pre-fill: volume %d done — %d objs, %.1f MiB in %.1fs\n",
			volKey, len(objs), float64(totalWritten)/(1<<20), time.Since(vt).Seconds())

		meta.Vols = append(meta.Vols, benchVolMeta{VolKey: vk, Objs: objs})
	}

	fmt.Fprintf(os.Stderr, "pre-fill: all volumes done in %.1fs, saving metadata...\n",
		time.Since(t0).Seconds())
	benchSaveMeta(b, meta)
	return meta
}

func benchLoadMeta() (*benchMeta, bool) {
	if _, err := os.Stat(benchPreFillDir); os.IsNotExist(err) {
		return nil, false
	}
	data, err := os.ReadFile(benchMetaFile)
	if err != nil {
		return nil, false
	}
	var meta benchMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		fmt.Fprintf(os.Stderr, "pre-fill: meta parse error (%v), re-filling\n", err)
		return nil, false
	}
	if len(meta.Vols) != benchMaxVols {
		fmt.Fprintf(os.Stderr, "pre-fill: meta has %d vols want %d, re-filling\n",
			len(meta.Vols), benchMaxVols)
		return nil, false
	}
	for _, v := range meta.Vols {
		totalSz := uint64(0)
		for _, o := range v.Objs {
			totalSz += o.Size
		}
		if totalSz < benchPreFillSz {
			fmt.Fprintf(os.Stderr, "pre-fill: vol=%d has %.1f MiB want %.1f MiB, re-filling\n",
				v.VolKey, float64(totalSz)/(1<<20), float64(benchPreFillSz)/(1<<20))
			return nil, false
		}
	}
	return &meta, true
}

func benchSaveMeta(b *testing.B, meta *benchMeta) {
	b.Helper()
	data, err := json.Marshal(meta)
	if err != nil {
		b.Fatalf("marshal bench meta: %v", err)
	}
	if err := os.WriteFile(benchMetaFile, data, 0o666); err != nil {
		b.Fatalf("write bench meta: %v", err)
	}
}

// benchCopyVolumes copies the first numVols pre-fill volume files into workDir,
// giving writers room to append data to the already-populated volume files.
func benchCopyVolumes(b *testing.B, meta *benchMeta, workDir string, numVols int) {
	b.Helper()
	for i := range numVols {
		volKey := meta.Vols[i].VolKey
		src := fmt.Sprintf("%s/.volume.%d", benchPreFillDir, volKey)
		dst := fmt.Sprintf("%s/.volume.%d", workDir, volKey)

		in, err := os.Open(src)
		if err != nil {
			b.Fatalf("open src volume %s: %v", src, err)
		}
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o666)
		if err != nil {
			in.Close()
			b.Fatalf("create dst volume %s: %v", dst, err)
		}
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			out.Close()
			b.Fatalf("copy volume key=%d: %v", volKey, err)
		}
		in.Close()
		out.Close()
	}
}

// benchCheckMem reads current process memory.  If Sys exceeds benchMemLimit it
// writes a heap profile and returns true so the caller can stop the benchmark.
func benchCheckMem(b *testing.B) bool {
	b.Helper()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if ms.Sys <= benchMemLimit {
		return false
	}
	path := fmt.Sprintf("%s.bench.heap.%d.prof", testcase, time.Now().UnixNano())
	if f, err := os.Create(path); err == nil {
		_ = pprof.WriteHeapProfile(f)
		f.Close()
	}
	b.Logf("MEMORY LIMIT: Sys=%d MiB > %d GiB — heap dump → %s; stopping loops",
		ms.Sys>>20, benchMemLimit>>30, path)
	return true
}

// ── Benchmark ────────────────────────────────────────────────────────────────

// BenchmarkStorage runs a storage benchmark sequentially across configurations.
//
// Dimensions:
//
//	numVols  : {1, 5, 10, 15, 20}
//	(W × R)  : {1×50, 10×200, 25×500, 40×2000*, 50×5000*}  (* only for ≥15 vols)
//	blkSz    : {32 KiB, 64 KiB, 128 KiB, 256 KiB, 512 KiB}
//
// Each sub-benchmark runs at most benchMaxLoops (4) loops.  At the start of
// every loop the work directory is refreshed from the pre-fill cache so volumes
// always begin at 1 GiB and writers have a full 2 GiB of headroom.
//
func BenchmarkStorage(b *testing.B) {
	if err := os.MkdirAll(testcase, 0o777); err != nil {
		b.Fatalf("mkdir testcase: %v", err)
	}

	randBuf := benchRandBuf()
	meta := benchEnsurePreFill(b, randBuf)

	type wrCfg struct{ writers, readers int }

	volCounts  := []int{1, 5, 10, 15, 20}
	wrCfgs     := []wrCfg{{1, 50}, {10, 200}, {25, 500}, {40, 2000}, {50, 5000}}
	blockSizes := []int{32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10}

	// skipWRC returns true for writer/reader configs that are under-provisioned
	// for the given volume count:
	//   {1,50}   — pointless for >1 volume (1 writer can't stress N volumes)
	//   {10,200} — pointless for >10 volumes (same reason)
	//   {40,2000},{50,5000} — too heavy for <15 volumes (runtime explodes)
	skipWRC := func(nv int, wrc wrCfg) bool {
		if wrc.writers == 1 && nv > 1 {
			return true
		}
		if wrc.writers == 10 && nv > 10 {
			return true
		}
		if wrc.writers >= 40 && nv < 15 {
			return true
		}
		return false
	}

	cfgTotal := 0
	for _, nv := range volCounts {
		for _, wrc := range wrCfgs {
			if skipWRC(nv, wrc) {
				continue
			}
			cfgTotal += len(blockSizes)
		}
	}
	cfgSeq := 0

	for _, numVols := range volCounts {
		for _, wrc := range wrCfgs {
			if skipWRC(numVols, wrc) {
				continue
			}
			for _, blkSz := range blockSizes {
				numVols := numVols
				wrc     := wrc
				blkSz   := blkSz
				thisCfg := cfgSeq

				name := fmt.Sprintf("vols%d_w%d_r%d_blk%dKiB",
					numVols, wrc.writers, wrc.readers, blkSz>>10)

				b.Run(name, func(b *testing.B) {
					b.StopTimer()

					workDir := fmt.Sprintf("%s.bench.work.%d", testcase, thisCfg)
					b.Cleanup(func() { os.RemoveAll(workDir) })

					// Build read-object pool and volKeys from pre-fill metadata.
					// These are loop-invariant: reads always target pre-fill objects.
					// Objects are interleaved round-robin by volume so that readers
					// cycling through the pool hit all volumes equally, regardless
					// of how few objects each reader processes before the shared
					// readTarget atomic cuts them off.
					type readObj struct {
						volKey uint64
						key    uint64
						off    uint64
					}
					volKeys := make([]uint64, numVols)
					perVol  := make([][]readObj, numVols)
					maxObjs := 0
					for i := range numVols {
						vm := meta.Vols[i]
						volKeys[i] = vm.VolKey
						pool := make([]readObj, len(vm.Objs))
						for j, o := range vm.Objs {
							pool[j] = readObj{vm.VolKey, o.Key, o.Off}
						}
						perVol[i] = pool
						if len(pool) > maxObjs {
							maxObjs = len(pool)
						}
					}
					// Interleave: [v0[0], v1[0], ..., vN[0], v0[1], v1[1], ...]
					readPool := make([]readObj, 0, numVols*maxObjs)
					for j := range maxObjs {
						for i := range numVols {
							if j < len(perVol[i]) {
								readPool = append(readPool, perVol[i][j])
							}
						}
					}

					// Per-volume metrics accumulated across all loops.
					volMetrics := make([]benchVolMetrics, numVols)
					volIdxOf := func(key uint64) int {
						for i, k := range volKeys {
							if k == key {
								return i
							}
						}
						return 0
					}

					// Aggregate across loops for final ReportMetric.
					var (
						aggWriteBytes atomic.Uint64
						aggReadBytes  atomic.Uint64
						aggWriteOps   atomic.Uint64
						aggReadOps    atomic.Uint64
						aggWriteUsec  atomic.Uint64
						aggReadUsec   atomic.Uint64
					)

					// Key base advances each loop so written keys never collide
					// with pre-fill keys (0..benchMaxVols*10000) or prior loops.
					var nextKeyBase atomic.Uint64
					nextKeyBase.Store(uint64(benchMaxVols*10000) + 1)

					loops := 0

					b.StartTimer()
					for b.Loop() {
						loops++
						if loops > benchMaxLoops {
							break
						}

						// ── Per-loop setup (untimed) ──────────────────────
						b.StopTimer()

						if benchCheckMem(b) {
							break
						}

						// Refresh: wipe work dir and copy fresh volumes from cache.
						os.RemoveAll(workDir)
						if err := os.Mkdir(workDir, 0o777); err != nil {
							b.Fatalf("mkdir workDir loop=%d: %v", loops, err)
						}
						fmt.Fprintf(os.Stderr,
							"\n[%d/%d] %s loop %d/%d: copying %d volumes...\n",
							thisCfg, cfgTotal, name, loops, benchMaxLoops, numVols)
						t0Copy := time.Now()
						benchCopyVolumes(b, meta, workDir, numVols)
						fmt.Fprintf(os.Stderr,
							"[%d/%d] copy done in %.1fs, opening storage...\n",
							thisCfg, cfgTotal, time.Since(t0Copy).Seconds())

						ctx, ctxCancel := context.WithTimeout(context.Background(), 3*time.Hour)
						stor, err := Open(ctx, workDir, WithObjectChecksumming())
						if err != nil {
							ctxCancel()
							b.Fatalf("Open work storage loop=%d: %v", loops, err)
						}

						// Each loop gets its own key range to avoid collisions.
						loopKeyBase := nextKeyBase.Add(uint64(wrc.writers) * 100000)
						var nextKey atomic.Uint64
						nextKey.Store(loopKeyBase)

						writeTarget := uint64(numVols) * benchWritePerVol

						var loopWriteBytes, loopReadBytes atomic.Uint64
						// Readers claim objects via this atomic index so each
						// needle is read by exactly one goroutine per loop.
						var readIdx atomic.Uint64

						// ── Timed region ──────────────────────────────────
						b.StartTimer()
						loopStart := time.Now()
						var wg sync.WaitGroup

						// Writers
						for w := range wrc.writers {
							wg.Add(1)
							go func(wID int) {
								defer wg.Done()
								rng    := rand.New(rand.NewPCG(uint64(loops), uint64(wID)))
								bufOff := wID * (benchRandBufSz / wrc.writers) % benchRandBufSz
								volRR  := wID

								for loopWriteBytes.Load() < writeTarget {
									sz   := benchLNSize(rng)
									key  := nextKey.Add(1)
									vKey := volKeys[volRR%numVols]
									volRR++

									t := time.Now()
									written, err := benchWriteObj(
										stor, vKey, key, sz, blkSz, randBuf, &bufOff)
									if err == io.EOF {
										// Volume full — no point retrying this loop.
										break
									}
									if err != nil {
										b.Errorf("writer w=%d: %v", wID, err)
										return
									}
									usec := uint64(time.Since(t).Microseconds())

									vi := volIdxOf(vKey)
									volMetrics[vi].writeBytes.Add(written)
									volMetrics[vi].writeOps.Add(1)
									volMetrics[vi].writeUsec.Add(usec)
									loopWriteBytes.Add(written)
									aggWriteBytes.Add(written)
									aggWriteOps.Add(1)
									aggWriteUsec.Add(usec)
								}
							}(w)
						}

						// Readers — each goroutine claims the next unread needle via
						// readIdx; every object in the pool is read exactly once.
						for r := range wrc.readers {
							wg.Add(1)
							go func(rID int) {
								defer wg.Done()
								buf := make([]byte, blkSz)

								for {
									idx := int(readIdx.Add(1)) - 1
									if idx >= len(readPool) {
										return
									}
									obj := readPool[idx]

									rd, err := stor.GetObjectReader(obj.volKey, obj.key, obj.off)
									if err != nil {
										b.Errorf("reader r=%d GetObjectReader vol=%d key=%d: %v",
											rID, obj.volKey, obj.key, err)
										return
									}

									t    := time.Now()
									read := uint64(0)
									for {
										n, rerr := rd.Read(buf)
										read += uint64(n)
										if rerr == io.EOF {
											break
										}
										if rerr != nil {
											b.Errorf("reader r=%d Read vol=%d key=%d: %v",
												rID, obj.volKey, obj.key, rerr)
											rd.Close()
											return
										}
									}
									if err := rd.Close(); err != nil {
										b.Errorf("reader r=%d Close vol=%d key=%d: %v",
											rID, obj.volKey, obj.key, err)
										return
									}
									usec := uint64(time.Since(t).Microseconds())

									vi := volIdxOf(obj.volKey)
									volMetrics[vi].readBytes.Add(read)
									volMetrics[vi].readOps.Add(1)
									volMetrics[vi].readUsec.Add(usec)
									loopReadBytes.Add(read)
									aggReadBytes.Add(read)
									aggReadOps.Add(1)
									aggReadUsec.Add(usec)
								}
							}(r)
						}

						wg.Wait()

						loopElapsed := time.Since(loopStart)
						wb := loopWriteBytes.Load()
						rb := loopReadBytes.Load()
						b.Logf("loop %d/%d: write=%.1f MiB (%.1f MiB/s)  read=%.1f MiB (%.1f MiB/s)  elapsed=%s",
							loops, benchMaxLoops,
							float64(wb)/(1<<20), float64(wb)/(1<<20)/loopElapsed.Seconds(),
							float64(rb)/(1<<20), float64(rb)/(1<<20)/loopElapsed.Seconds(),
							loopElapsed.Round(time.Millisecond),
						)

						// ── Per-loop teardown (untimed) ───────────────────
						b.StopTimer()
						cc, ccCancel := context.WithTimeout(context.Background(), time.Minute)
						if err := stor.Close(cc); err != nil {
							b.Logf("Close work storage loop=%d: %v", loops, err)
						}
						ccCancel()
						ctxCancel()
						b.StartTimer() // b.Loop() requires timer running on next call
					}

					b.StopTimer()

					// ── Report metrics ────────────────────────────────────
					elapsed := b.Elapsed()
					wOps    := aggWriteOps.Load()
					rOps    := aggReadOps.Load()
					wBytes  := aggWriteBytes.Load()
					rBytes  := aggReadBytes.Load()
					wUsec   := aggWriteUsec.Load()
					rUsec   := aggReadUsec.Load()

					if wOps > 0 {
						b.ReportMetric(float64(wBytes)/(1<<20)/elapsed.Seconds(), "writeMiB/s")
						b.ReportMetric(float64(wUsec)/float64(wOps)/1000, "writeMs/op")
					}
					if rOps > 0 {
						b.ReportMetric(float64(rBytes)/(1<<20)/elapsed.Seconds(), "readMiB/s")
						b.ReportMetric(float64(rUsec)/float64(rOps)/1000, "readMs/op")
					}

					var ms runtime.MemStats
					runtime.ReadMemStats(&ms)
					b.ReportMetric(float64(ms.Sys>>20), "peakMemMiB")

					// Per-volume summary to stderr for offline analysis.
					fmt.Fprintf(os.Stderr,
						"[%d/%d] %s — %d loops | write=%.1f MiB | read=%.1f MiB | mem=%d MiB\n",
						thisCfg, cfgTotal, name, loops,
						float64(wBytes)/(1<<20), float64(rBytes)/(1<<20), ms.Sys>>20)
					for i := range numVols {
						vm    := &volMetrics[i]
						vwOps := vm.writeOps.Load()
						vrOps := vm.readOps.Load()
						vwMs, vrMs := 0.0, 0.0
						if vwOps > 0 {
							vwMs = float64(vm.writeUsec.Load()) / float64(vwOps) / 1000
						}
						if vrOps > 0 {
							vrMs = float64(vm.readUsec.Load()) / float64(vrOps) / 1000
						}
						fmt.Fprintf(os.Stderr,
							"  vol=%d: write=%.1f MiB (%d ops, %.2f ms/op)  read=%.1f MiB (%d ops, %.2f ms/op)\n",
							volKeys[i],
							float64(vm.writeBytes.Load())/(1<<20), vwOps, vwMs,
							float64(vm.readBytes.Load())/(1<<20), vrOps, vrMs,
						)
					}
				})
				cfgSeq++
			}
		}
	}
}
