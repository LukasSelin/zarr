package zarr

import (
	"context"
	"sync"
	"sync/atomic"
)

// defaultConcurrency is how many stored objects - chunks, or shards - Read,
// Write and Resize work on at once when an array says nothing. It hides the
// round trip of an object store, tens of milliseconds against the
// microseconds a chunk takes to decode, without holding many of them in
// memory at once. It is a variable so that the tests can turn it down.
var defaultConcurrency = 16

// limit is how many goroutines a loop over n stored objects may use.
func (a *Array) limit(n int) int {
	c := a.Concurrency
	if c <= 0 {
		c = defaultConcurrency
	}
	return min(max(c, 1), n)
}

// byRange says whether the chunks of a stored object are read a range at a
// time rather than the object whole: a shard, from a store that reads ranges,
// with nothing but the sharding codec between it and its bytes.
func (a *Array) byRange() bool {
	if a.shard == nil {
		return false
	}
	_, ranged := a.store.(RangeGetter)
	return ranged && len(a.codecs.bytes) == 0
}

// spanLen is how many indices there are from lo to hi inclusive: one, of no
// dimensions, for a scalar array.
func spanLen(lo, hi []int) int {
	n := 1
	for k := range lo {
		n *= max(hi[k]-lo[k]+1, 0)
	}
	return n
}

// spanInto writes the nth index from lo to hi inclusive into idx, counting in
// eachIndex's order: the last dimension fastest.
func spanInto(idx, lo, hi []int, n int) {
	for k := len(lo) - 1; k >= 0; k-- {
		w := hi[k] - lo[k] + 1
		idx[k], n = lo[k]+n%w, n/w
	}
}

// spanOf is where idx falls from lo to hi inclusive: spanInto turned round.
func spanOf(lo, hi, idx []int) int {
	n := 0
	for k := range lo {
		n = n*(hi[k]-lo[k]+1) + idx[k] - lo[k]
	}
	return n
}

// eachSpan calls f with every index from lo to hi inclusive, in up to limit
// goroutines at once, and returns the error of the first call that failed. n
// is where the index falls in eachIndex's order, so that f may keep what it
// makes in a slice of its own without a lock. f must not keep idx.
//
// A limit of one starts no goroutine and goes in eachIndex's order. Above
// one, the order indices are taken in is not defined, and the ctx f is given
// is cancelled as soon as any f fails, so that a read whose chunk is broken
// does not wait for the chunks beside it; the error returned is the one that
// failed, never that cancellation. A worker takes another index only while
// the ctx is good, so cancelling the ctx eachSpan was given stops it within
// one stored object of each goroutine, and it returns that cancellation
// rather than the nothing of a loop that never reached the end.
func eachSpan(ctx context.Context, limit int, lo, hi []int, f func(ctx context.Context, n int, idx []int) error) error {
	total := spanLen(lo, hi)
	if limit <= 1 || total <= 1 {
		idx := make([]int, len(lo))
		for n := range total {
			spanInto(idx, lo, hi, n)
			if err := f(ctx, n, idx); err != nil {
				return err
			}
		}
		return nil
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu    sync.Mutex
		first error
		next  atomic.Int64
		wg    sync.WaitGroup
	)
	for range min(limit, total) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := make([]int, len(lo))
			for {
				n := int(next.Add(1)) - 1
				if n >= total || ctx.Err() != nil {
					return
				}
				spanInto(idx, lo, hi, n)
				if err := f(ctx, n, idx); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if first == nil {
		// The only other reason a worker stopped before the end is the ctx
		// it was given being cancelled, which is an error of its own: a
		// cancelled read must not look like one that read everything.
		return parent.Err()
	}
	return first
}
