package geozarr

import (
	"container/list"
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/LukasSelin/zarr"
)

// Sampler reads the values of a raster at points, keeping the chunks it has
// decoded so that points near one another cost one fetch. It is safe for use
// from many goroutines, and two that want the same chunk at once fetch it
// once.
type Sampler[T zarr.Element] struct {
	r     *Raster
	chunk []int
	fill  T
	max   int

	mu      sync.Mutex
	lru     *list.List // of *cached[T], most recently used first
	entries map[string]*list.Element
}

type cached[T zarr.Element] struct {
	key  string
	done chan struct{}
	data []T
	err  error
}

// NewSampler is a sampler of r keeping up to chunks decoded chunks, 256 if
// zero. The memory it holds is that many chunks of the raster.
func NewSampler[T zarr.Element](r *Raster, chunks int) (*Sampler[T], error) {
	if r.Array.DataType() != zarr.DataTypeOf[T]() {
		return nil, fmt.Errorf("geozarr: raster %q holds %s, not %s", r.Array.Path(), r.Array.DataType(), zarr.DataTypeOf[T]())
	}
	if chunks <= 0 {
		chunks = 256
	}
	s := &Sampler[T]{r: r, chunk: r.Array.ChunkShape(), max: chunks, lru: list.New(), entries: map[string]*list.Element{}}
	if f, ok := r.Array.FillValue().(T); ok {
		s.fill = f
	}
	return s, nil
}

// At is the value at (x, y) in the plane at lead, and false, with the fill
// value, if (x, y) is off the raster.
func (s *Sampler[T]) At(ctx context.Context, lead []int, x, y float64) (T, bool, error) {
	if err := s.r.checkLead(lead); err != nil {
		return s.fill, false, err
	}
	row, col, ok := s.r.Pixel(x, y)
	if !ok {
		return s.fill, false, nil
	}
	idx, off := s.locate(lead, row, col)
	data, err := s.get(ctx, chunkKey(idx), idx)
	if err != nil {
		return s.fill, false, err
	}
	return data[off], true, nil
}

// locate is the chunk the element at lead, row, col is in, and where in it.
func (s *Sampler[T]) locate(lead []int, row, col int) (idx []int, off int) {
	nd := len(s.chunk)
	idx = make([]int, nd)
	for d := range nd {
		var at int
		switch {
		case d < len(lead):
			at = lead[d]
		case d == nd-2:
			at = row
		default:
			at = col
		}
		idx[d] = at / s.chunk[d]
		off = off*s.chunk[d] + at%s.chunk[d]
	}
	return idx, off
}

func chunkKey(idx []int) string {
	b := make([]byte, 0, 8*len(idx))
	for _, i := range idx {
		b = strconv.AppendInt(b, int64(i), 10)
		b = append(b, '/')
	}
	return string(b)
}

// Points is the value at each (xs[i], ys[i]) in the plane at lead, and
// whether it is on the raster; one off it has the fill value. The chunks the
// points are in are fetched at once, up to the array's Concurrency.
func (s *Sampler[T]) Points(ctx context.Context, lead []int, xs, ys []float64) ([]T, []bool, error) {
	if len(xs) != len(ys) {
		return nil, nil, fmt.Errorf("geozarr: %d xs and %d ys", len(xs), len(ys))
	}
	if err := s.r.checkLead(lead); err != nil {
		return nil, nil, err
	}
	type spot struct {
		point, offset int
	}
	byChunk := map[string][]spot{}
	var order [][]int // chunk indices, in the order first seen
	keys := []string{}
	in := make([]bool, len(xs))
	for p := range xs {
		row, col, ok := s.r.Pixel(xs[p], ys[p])
		if !ok {
			continue
		}
		in[p] = true
		idx, off := s.locate(lead, row, col)
		k := chunkKey(idx)
		if _, seen := byChunk[k]; !seen {
			order, keys = append(order, idx), append(keys, k)
		}
		byChunk[k] = append(byChunk[k], spot{p, off})
	}

	out := make([]T, len(xs))
	for p := range out {
		out[p] = s.fill
	}
	workers := s.r.Array.Concurrency
	if workers <= 0 {
		workers = 16
	}
	var mu sync.Mutex
	err := parallel(ctx, len(order), workers, nil, func(ctx context.Context, i int, _ any) error {
		data, err := s.get(ctx, keys[i], order[i])
		if err != nil {
			return err
		}
		mu.Lock()
		for _, sp := range byChunk[keys[i]] {
			out[sp.point] = data[sp.offset]
		}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return out, in, nil
}

// get is the chunk at idx, from the cache or fetched.
func (s *Sampler[T]) get(ctx context.Context, key string, idx []int) ([]T, error) {
	s.mu.Lock()
	if e, ok := s.entries[key]; ok {
		s.lru.MoveToFront(e)
		c := e.Value.(*cached[T])
		s.mu.Unlock()
		select {
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if c.err != nil {
			return nil, c.err
		}
		return c.data, nil
	}
	c := &cached[T]{key: key, done: make(chan struct{})}
	s.entries[key] = s.lru.PushFront(c)
	for s.lru.Len() > s.max {
		old := s.lru.Back()
		s.lru.Remove(old)
		delete(s.entries, old.Value.(*cached[T]).key)
	}
	s.mu.Unlock()

	// The fetch is not cancelled with ctx: another caller may be waiting on
	// it with a context of its own. A failed one leaves the cache.
	c.data, c.err = zarr.ReadChunk[T](context.WithoutCancel(ctx), s.r.Array, idx)
	if c.err != nil {
		s.mu.Lock()
		if e, ok := s.entries[key]; ok && e.Value == c {
			s.lru.Remove(e)
			delete(s.entries, key)
		}
		s.mu.Unlock()
	}
	close(c.done)
	return c.data, c.err
}

// Forget empties the cache, as after the raster is written.
func (s *Sampler[T]) Forget() {
	s.mu.Lock()
	s.lru.Init()
	clear(s.entries)
	s.mu.Unlock()
}
