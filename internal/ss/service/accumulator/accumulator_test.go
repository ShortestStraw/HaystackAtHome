package accumulator

import (
	"sync"
	"testing"
	"time"
)

// fakeClock provides a controllable time source for tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

func newTestAccumulator(windowDur time.Duration, clk *fakeClock) *Accumulator {
	a := New(windowDur)
	a.now = clk.now
	return a
}

// ── Account / Collect ─────────────────────────────────────────────────────────

func TestCollectUnknownKeyReturnsZero(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)
	if got := a.Collect(99, time.Minute); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

func TestAccountCollectBasic(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 100)
	a.Account(1, 200)

	got := a.Collect(1, time.Minute)
	if got != 300 {
		t.Errorf("got %d, want 300", got)
	}
}

func TestAccountSameBucketAccumulates(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	// Three accounts within the same 1s bucket.
	a.Account(1, 10)
	a.Account(1, 20)
	a.Account(1, 30)

	c := a.getOrCreate(1)
	c.mu.Lock()
	winCount := len(c.windows)
	c.mu.Unlock()

	if winCount != 1 {
		t.Errorf("same-bucket accounts should produce 1 window, got %d", winCount)
	}
	if got := a.Collect(1, time.Minute); got != 60 {
		t.Errorf("got %d, want 60", got)
	}
}

func TestCollectExcludesEventsBeyondDur(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 500) // t=0
	clk.advance(10 * time.Second)
	a.Account(1, 100) // t=10s

	// Query only the last 5s — should see only the second account.
	got := a.Collect(1, 5*time.Second)
	if got != 100 {
		t.Errorf("got %d, want 100", got)
	}
}

func TestCollectIsolatedByKey(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 111)
	a.Account(2, 222)

	if got := a.Collect(1, time.Minute); got != 111 {
		t.Errorf("key 1: got %d, want 111", got)
	}
	if got := a.Collect(2, time.Minute); got != 222 {
		t.Errorf("key 2: got %d, want 222", got)
	}
}

// ── CollectAll ────────────────────────────────────────────────────────────────

func TestCollectAllEmpty(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)
	if got := a.CollectAll(time.Minute); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

func TestCollectAllSumsAllKeys(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 100)
	a.Account(2, 200)
	a.Account(3, 300)

	got := a.CollectAll(time.Minute)
	if got != 600 {
		t.Errorf("got %d, want 600", got)
	}
}

func TestCollectAllExcludesOldEvents(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 500) // t=0
	a.Account(2, 500) // t=0
	clk.advance(10 * time.Second)
	a.Account(1, 50) // t=10s

	got := a.CollectAll(5 * time.Second)
	if got != 50 {
		t.Errorf("got %d, want 50", got)
	}
}

// ── Prune ─────────────────────────────────────────────────────────────────────

func TestPruneRemovesOldWindows(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 100) // t=0
	clk.advance(10 * time.Second)
	a.Account(1, 200) // t=10s

	a.Prune(5 * time.Second) // drop everything older than 5s

	got := a.Collect(1, time.Minute)
	if got != 200 {
		t.Errorf("after Prune: got %d, want 200", got)
	}
}

func TestPruneKeepsRecentWindows(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 100)
	a.Prune(time.Minute) // maxAge >> event age — nothing should be dropped

	got := a.Collect(1, time.Minute)
	if got != 100 {
		t.Errorf("after Prune with large maxAge: got %d, want 100", got)
	}
}

func TestPruneAllWindowsResultsInZero(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	a.Account(1, 100) // t=0
	clk.advance(10 * time.Second)
	a.Prune(5 * time.Second) // window at t=0 is now 10s old → dropped

	got := a.Collect(1, time.Minute)
	if got != 0 {
		t.Errorf("all windows pruned: got %d, want 0", got)
	}
}

// ── compact ───────────────────────────────────────────────────────────────────

func TestCompactWindowCountIsLogarithmic(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	// Add 64 events, each in a distinct 1s bucket.
	const n = 64
	for range n {
		a.Account(1, 1)
		clk.advance(time.Second)
	}

	c := a.getOrCreate(1)
	c.mu.Lock()
	winCount := len(c.windows)
	c.mu.Unlock()

	// With logarithmic compaction the window count must be << n.
	// A loose upper bound: log2(64) * 2 = 12.
	if winCount > 12 {
		t.Errorf("expected O(log n) windows after %d accounts, got %d", n, winCount)
	}
}

func TestCompactRecentWindowsNotMerged(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	// Two accounts in consecutive 1s buckets — should stay separate.
	a.Account(1, 10)
	clk.advance(time.Second)
	a.Account(1, 20)

	c := a.getOrCreate(1)
	c.mu.Lock()
	winCount := len(c.windows)
	c.mu.Unlock()

	if winCount != 2 {
		t.Errorf("two recent consecutive windows should not merge, got %d windows", winCount)
	}
}

func TestCompactPreservesTotal(t *testing.T) {
	clk := newFakeClock()
	a := newTestAccumulator(time.Second, clk)

	const n = 32
	var want uint64
	for i := range n {
		v := uint64(i + 1)
		a.Account(1, v)
		want += v
		clk.advance(time.Second)
	}

	got := a.Collect(1, time.Duration(n+1)*time.Second)
	if got != want {
		t.Errorf("compaction lost data: got %d, want %d", got, want)
	}
}

// ── concurrency ───────────────────────────────────────────────────────────────

func TestConcurrentAccountCollect(t *testing.T) {
	a := New(time.Millisecond)
	var wg sync.WaitGroup

	for i := range 8 {
		wg.Add(2)
		go func(key uint64) {
			defer wg.Done()
			for i := range 1000 {
				a.Account(key, uint64(i))
			}
		}(uint64(i))
		go func(key uint64) {
			defer wg.Done()
			for range 1000 {
				_ = a.Collect(key, time.Hour)
			}
		}(uint64(i))
	}
	wg.Wait()
}
