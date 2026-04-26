// Package accumulator implements an exponential-histogram accumulator.
//
// Events (key, value) are recorded via Account and queried via Collect.
// Internally each key maintains O(log N) time windows: recent data stays
// fine-grained, old data is merged into coarser buckets (logarithmic rule).
//
// Boundary precision: a Collect query may include the full sum of the oldest
// overlapping window even when that window only partially falls inside the
// requested range. For merged windows this error can exceed windowDur.
package accumulator

import (
	"sync"
	"time"
)

// window holds the pre-aggregated sum for a time span.
type window struct {
	start time.Time
	end   time.Time
	sum   uint64
}

type container struct {
	mu      sync.Mutex
	windows []window // sorted by start, oldest first
}

type Accumulator struct {
	windowDur  time.Duration
	now        func() time.Time
	containers sync.Map // uint64 -> *container
}

// New creates an Accumulator with the given base window granularity.
// windowDur is the minimum time resolution for new buckets (e.g. 1s).
func New(windowDur time.Duration) *Accumulator {
	return &Accumulator{
		windowDur: windowDur,
		now:       time.Now,
	}
}

// Account records val for key at the current time.
func (a *Accumulator) Account(key uint64, val uint64) {
	c := a.getOrCreate(key)
	now := a.now()
	bucket := now.Truncate(a.windowDur)

	c.mu.Lock()
	n := len(c.windows)
	if n > 0 && c.windows[n-1].start.Equal(bucket) {
		c.windows[n-1].sum += val
	} else {
		c.windows = append(c.windows, window{
			start: bucket,
			end:   bucket.Add(a.windowDur),
			sum:   val,
		})
		c.compact(now)
	}
	c.mu.Unlock()
}

// Collect returns the sum for key over the last dur.
func (a *Accumulator) Collect(key uint64, dur time.Duration) uint64 {
	raw, ok := a.containers.Load(key)
	if !ok {
		return 0
	}
	c := raw.(*container)
	cutoff := a.now().Add(-dur)

	c.mu.Lock()
	total := c.sumSince(cutoff)
	c.mu.Unlock()
	return total
}

// CollectAll returns the sum across all keys over the last dur.
// The result is not a consistent snapshot: in-flight Account calls may be
// partially visible.
func (a *Accumulator) CollectAll(dur time.Duration) uint64 {
	cutoff := a.now().Add(-dur)
	var total uint64

	a.containers.Range(func(_, raw any) bool {
		c := raw.(*container)
		c.mu.Lock()
		total += c.sumSince(cutoff)
		c.mu.Unlock()
		return true
	})
	return total
}

// Prune drops windows older than maxAge from all containers.
// Call periodically to bound memory usage.
func (a *Accumulator) Prune(maxAge time.Duration) {
	cutoff := a.now().Add(-maxAge)
	a.containers.Range(func(_, raw any) bool {
		c := raw.(*container)
		c.mu.Lock()
		c.dropBefore(cutoff)
		c.mu.Unlock()
		return true
	})
}

// --- container internals (caller holds c.mu) ---

// compact merges adjacent windows using the logarithmic rule:
// two windows merge when their combined span is less than the age of the newer one.
//
// This keeps O(log(totalAccounts)) windows per container: recent windows stay
// fine-grained; old windows are coalesced into progressively larger buckets.
// The in-place reuse of the backing array is safe because windows[i] is read
// into a local copy before the slot at position len(merged) is overwritten.
func (c *container) compact(now time.Time) {
	if len(c.windows) < 2 {
		return
	}

	merged := c.windows[:1] // reuse backing array; safe — see above
	for i := 1; i < len(c.windows); i++ {
		prev := &merged[len(merged)-1]
		w := c.windows[i]

		combinedSpan := w.end.Sub(prev.start)
		age := now.Sub(w.start)

		if combinedSpan < age {
			prev.end = w.end
			prev.sum += w.sum
			// Cascading merges happen naturally: the next iteration
			// checks the enlarged prev against the following window.
		} else {
			merged = append(merged, w)
		}
	}
	c.windows = merged
}

// sumSince returns the sum of all windows whose end is after cutoff.
func (c *container) sumSince(cutoff time.Time) uint64 {
	var total uint64
	for i := range c.windows {
		if c.windows[i].end.After(cutoff) {
			total += c.windows[i].sum
		}
	}
	return total
}

func (c *container) dropBefore(cutoff time.Time) {
	i := 0
	for i < len(c.windows) && !c.windows[i].end.After(cutoff) {
		i++
	}
	if i > 0 {
		remaining := make([]window, len(c.windows)-i)
		copy(remaining, c.windows[i:])
		c.windows = remaining
	}
}

func (a *Accumulator) getOrCreate(key uint64) *container {
	if raw, ok := a.containers.Load(key); ok {
		return raw.(*container)
	}
	c := &container{}
	actual, _ := a.containers.LoadOrStore(key, c)
	return actual.(*container)
}
