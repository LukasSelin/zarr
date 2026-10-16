package geodb

import (
	"context"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/LukasSelin/zarr"
)

// UpdateComposites makes the composites of every period from the one from
// is in to the one to is in, from the scenes in them. Doing it again for a
// period gives what it gave before unless scenes were added to the period,
// so run it after each ingest for the periods the new scenes fall in.
//
// It works a block of MapChunk square at a time, as many at once as there
// are processors: a block reads its chunk of each scene in the periods, and
// writes its own shards of the composites, which no other block touches.
func (t *Tile) UpdateComposites(ctx context.Context, from, to time.Time) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	st := t.state.Load()
	m := st.meta
	n := len(st.times)
	if n == 0 {
		return nil
	}
	// Periods past the scenes there are would be fill, so they are not made.
	p0 := max(0, m.period(from.Unix()), m.period(st.times[0]))
	p1 := min(m.period(to.Unix()), m.period(st.times[n-1])) + 1
	if p1 <= p0 {
		return nil
	}
	// The scenes of each period, by index.
	byPeriod := make([][]int, p1-p0)
	for i, ts := range st.times {
		if p := m.period(ts); p >= p0 && p < p1 {
			byPeriod[p-p0] = append(byPeriod[p-p0], i)
		}
	}
	g := m.Grid
	for _, v := range m.Variables {
		a, err := zarr.OpenArray(ctx, t.db.store, st.comps[v.Name].Path())
		if err != nil {
			return err
		}
		if k := a.Shape()[0]; int64(k) < p1 {
			if err := a.Resize(ctx, []int{int(p1), g.Height, g.Width}); err != nil {
				return err
			}
		}
		var blocks []Window
		for r := 0; r < g.Height; r += m.MapChunk {
			for c := 0; c < g.Width; c += m.MapChunk {
				blocks = append(blocks, Window{c, r, min(m.MapChunk, g.Width-c), min(m.MapChunk, g.Height-r)})
			}
		}
		err = parallel(ctx, len(blocks), runtime.GOMAXPROCS(0), func(ctx context.Context, b int) error {
			return compositeBlock(ctx, st.scenes[v.Name], a, v, int(p0), byPeriod, blocks[b])
		})
		if err != nil {
			return err
		}
	}
	return t.reload(ctx)
}

func compositeBlock(ctx context.Context, scenes, comp *zarr.Array, v Variable, p0 int, byPeriod [][]int, w Window) error {
	px := w.Width * w.Height
	out := make([]int16, len(byPeriod)*px)
	for k := range out {
		out[k] = fill16
	}
	vals := make([]int16, 0, 16)
	for pi, idx := range byPeriod {
		if len(idx) == 0 {
			continue
		}
		reads := make([][]int16, len(idx))
		for k, i := range idx {
			r, err := zarr.Read[int16](ctx, scenes, []int{i, w.Row, w.Col}, []int{1, w.Height, w.Width})
			if err != nil {
				return err
			}
			reads[k] = r
		}
		o := out[pi*px : (pi+1)*px]
		for p := range px {
			vals = vals[:0]
			for _, r := range reads {
				if r[p] != fill16 {
					vals = append(vals, r[p])
				}
			}
			o[p] = reduce(v.Composite, vals)
		}
	}
	return zarr.Write(ctx, comp, []int{p0, w.Row, w.Col}, []int{len(byPeriod), w.Height, w.Width}, out)
}

// reduce is the composite of a pixel's valid values in a period, as stored.
func reduce(how string, vals []int16) int16 {
	switch len(vals) {
	case 0:
		return fill16
	case 1:
		return vals[0]
	}
	switch how {
	case "max":
		return slices.Max(vals)
	case "mean":
		var sum int64
		for _, x := range vals {
			sum += int64(x)
		}
		n := int64(len(vals))
		if sum >= 0 {
			return int16((sum + n/2) / n)
		}
		return int16((sum - n/2) / n)
	default: // median, the mean of the middle two of an even count
		slices.Sort(vals)
		mid := len(vals) / 2
		if len(vals)%2 == 1 {
			return vals[mid]
		}
		a, b := int32(vals[mid-1]), int32(vals[mid])
		return int16((a + b) / 2)
	}
}

// parallel runs fn for 0 to n-1, at most workers at once, and returns the
// first error, stopping those not yet begun once there is one.
func parallel(ctx context.Context, n, workers int, fn func(ctx context.Context, i int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
		next     = make(chan int)
	)
	for range min(n, max(1, workers)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				if err := fn(ctx, i); err != nil {
					once.Do(func() { firstErr = err; cancel() })
				}
			}
		}()
	}
feed:
	for i := range n {
		select {
		case next <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(next)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
