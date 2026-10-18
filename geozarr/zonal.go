package geozarr

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/LukasSelin/zarr"
)

// Stats are what Zonal gathers of the pixels of one zone.
type Stats struct {
	Count         int64
	Sum, Min, Max float64
}

// Mean is the mean of the zone's pixels, NaN if it has none.
func (s Stats) Mean() float64 {
	if s.Count == 0 {
		return math.NaN()
	}
	return s.Sum / float64(s.Count)
}

func (s *Stats) add(v float64) {
	if s.Count == 0 {
		s.Min, s.Max = v, v
	} else {
		s.Min, s.Max = math.Min(s.Min, v), math.Max(s.Max, v)
	}
	s.Count++
	s.Sum += v
}

func (s *Stats) merge(o Stats) {
	if o.Count == 0 {
		return
	}
	if s.Count == 0 {
		*s = o
		return
	}
	s.Count += o.Count
	s.Sum += o.Sum
	s.Min, s.Max = math.Min(s.Min, o.Min), math.Max(s.Max, o.Max)
}

// Label is the integer types a zone raster can hold: the id of the unit each
// pixel is in.
type Label interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64
}

// Zonal gathers the statistics of the pixels of values in the plane at lead
// for each unit, the unit of a pixel being the label zones has for it.
// zones is one plane on the same grid as values: the same height, width and
// transform. Pixels that are NaN or values' fill value are left out, and so
// are pixels whose label is zones' fill value. The planes are read a chunk
// of values at a time, as many at once as values' Concurrency.
func Zonal[T zarr.Element, Z Label](ctx context.Context, values *Raster, lead []int, zones *Raster) (map[int64]Stats, error) {
	if err := values.checkLead(lead); err != nil {
		return nil, err
	}
	if zones.lead != 0 {
		return nil, fmt.Errorf("geozarr: zones %q has leading dimensions", zones.Array.Path())
	}
	if values.Height() != zones.Height() || values.Width() != zones.Width() || values.Transform != zones.Transform {
		return nil, fmt.Errorf("geozarr: zones %q is not on the grid of %q", zones.Array.Path(), values.Array.Path())
	}
	if _, ok := any(*new(T)).(bool); ok {
		return nil, fmt.Errorf("geozarr: no statistics of booleans")
	}
	vfill, hasVFill := values.Array.FillValue().(T)
	zfill, hasZFill := zones.Array.FillValue().(Z)

	cs := values.Array.ChunkShape()
	ch, cw := cs[values.lead], cs[values.lead+1]
	nr, nc := (values.Height()+ch-1)/ch, (values.Width()+cw-1)/cw
	workers := values.Array.Concurrency
	if workers <= 0 {
		workers = 16
	}
	var mu sync.Mutex
	total := map[int64]Stats{}
	err := parallel(ctx, nr*nc, workers, nil, func(ctx context.Context, i int, _ any) error {
		w := Window{Row: i / nc * ch, Col: i % nc * cw}
		w.Height, w.Width = min(ch, values.Height()-w.Row), min(cw, values.Width()-w.Col)
		vs, err := ReadWindow[T](ctx, values, lead, w)
		if err != nil {
			return err
		}
		zs, err := ReadWindow[Z](ctx, zones, nil, w)
		if err != nil {
			return err
		}
		local := map[int64]*Stats{}
		var last int64
		var lastStats *Stats
		for j, z := range zs {
			v := vs[j]
			if (hasZFill && z == zfill) || (hasVFill && v == vfill) {
				continue
			}
			f := toFloat(v)
			if math.IsNaN(f) {
				continue
			}
			id := int64(z)
			// Neighbouring pixels are mostly in one zone.
			if lastStats == nil || id != last {
				s, ok := local[id]
				if !ok {
					s = &Stats{}
					local[id] = s
				}
				last, lastStats = id, s
			}
			lastStats.add(f)
		}
		mu.Lock()
		for id, s := range local {
			t := total[id]
			t.merge(*s)
			total[id] = t
		}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return total, nil
}

func toFloat[T zarr.Element](v T) float64 {
	switch v := any(v).(type) {
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	}
	return math.NaN()
}
