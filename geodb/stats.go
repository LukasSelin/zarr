package geodb

import (
	"context"
	"math"
	"slices"
	"sort"
)

// The statistics kept of a parcel for each scene, in this order along the
// last axis of a tile's stats arrays.
const (
	StatValidFraction = iota // of the parcel's pixels, those with a value
	StatMean
	StatStd
	StatP10
	StatP50
	StatP90
	numStats
)

// StatNames are the statistics by name, in the order of the Stat constants.
var StatNames = [numStats]string{"valid_fraction", "mean", "std", "p10", "p50", "p90"}

// tileParcels is which parcels a tile covers and the pixels of each.
type tileParcels struct {
	generation int64
	ordinal    []int64 // the parcel each local index is, in the DB's order
	npix       []int32
	spanOff    []int64
	spans      []span
	local      map[int64]int // ordinal -> local index
	minRow     []int32
	maxRow     []int32
	byMinRow   []int32 // local indices, by minRow
}

func (tp *tileParcels) len() int {
	if tp == nil {
		return 0
	}
	return len(tp.ordinal)
}

// finish works out what is derived from the spans.
func (tp *tileParcels) finish() {
	n := len(tp.ordinal)
	tp.local = make(map[int64]int, n)
	tp.minRow = make([]int32, n)
	tp.maxRow = make([]int32, n)
	tp.byMinRow = make([]int32, n)
	for i := range n {
		tp.local[tp.ordinal[i]] = i
		ss := tp.spans[tp.spanOff[i]:tp.spanOff[i+1]]
		tp.minRow[i], tp.maxRow[i] = math.MaxInt32, -1
		for _, s := range ss {
			tp.minRow[i] = min(tp.minRow[i], s.row)
			tp.maxRow[i] = max(tp.maxRow[i], s.row)
		}
		tp.byMinRow[i] = int32(i)
	}
	sort.SliceStable(tp.byMinRow, func(a, b int) bool {
		return tp.minRow[tp.byMinRow[a]] < tp.minRow[tp.byMinRow[b]]
	})
}

// bandSource gives rows r0 to r1 of a scene of a variable as stored, every
// column of them, in C order.
type bandSource func(ctx context.Context, r0, r1 int) ([]int16, error)

// compute is the statistics of every parcel of the tile in one scene: Pt
// rows of numStats, NaN where a parcel has no valid pixel. The scene is
// read a band of rows at a time, and only the bands some parcel is in;
// the values of a parcel are held from its first band to its last.
func (tp *tileParcels) compute(ctx context.Context, width, height, bandRows int, v Variable, src bandSource) ([]float32, error) {
	n := tp.len()
	out := make([]float32, n*numStats)
	for k := range out {
		out[k] = float32(math.NaN()) // what a parcel with no pixels keeps
	}
	if n == 0 {
		return out, nil
	}
	values := make(map[int][]int16) // of parcels begun and not finished
	next := 0                       // into byMinRow
	for r0 := 0; r0 < height; r0 += bandRows {
		r1 := min(height, r0+bandRows)
		if len(values) == 0 && (next == n || int(tp.minRow[tp.byMinRow[next]]) >= r1) {
			continue
		}
		band, err := src(ctx, r0, r1)
		if err != nil {
			return nil, err
		}
		for next < n && int(tp.minRow[tp.byMinRow[next]]) < r1 {
			values[int(tp.byMinRow[next])] = nil
			next++
		}
		for p, vals := range values {
			for _, s := range tp.spans[tp.spanOff[p]:tp.spanOff[p+1]] {
				if int(s.row) < r0 || int(s.row) >= r1 {
					continue
				}
				row := band[(int(s.row)-r0)*width:]
				for _, x := range row[s.c0:s.c1] {
					if x != fill16 {
						vals = append(vals, x)
					}
				}
			}
			if int(tp.maxRow[p]) < r1 {
				finishParcel(out, p, vals, tp.npix[p], v)
				delete(values, p)
			} else {
				values[p] = vals
			}
		}
	}
	return out, ctx.Err()
}

func finishParcel(out []float32, p int, vals []int16, npix int32, v Variable) {
	o := out[p*numStats : (p+1)*numStats]
	nan := float32(math.NaN())
	for k := range o {
		o[k] = nan
	}
	if npix <= 0 {
		return
	}
	o[StatValidFraction] = float32(len(vals)) / float32(npix)
	if len(vals) == 0 {
		return
	}
	slices.Sort(vals)
	// Two passes, so that the deviation does not cancel away when the
	// values are large next to their spread.
	var sum float64
	for _, x := range vals {
		sum += float64(x)*v.Scale + v.Offset
	}
	n := float64(len(vals))
	mean := sum / n
	var ss float64
	for _, x := range vals {
		d := float64(x)*v.Scale + v.Offset - mean
		ss += d * d
	}
	o[StatMean] = float32(mean)
	o[StatStd] = float32(math.Sqrt(ss / n))
	o[StatP10] = float32(percentile(vals, 0.10)*v.Scale + v.Offset)
	o[StatP50] = float32(percentile(vals, 0.50)*v.Scale + v.Offset)
	o[StatP90] = float32(percentile(vals, 0.90)*v.Scale + v.Offset)
}

// percentile of sorted values, interpolated between the two either side of
// it, as numpy's default is.
func percentile(sorted []int16, q float64) float64 {
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := min(lo+1, len(sorted)-1)
	f := pos - float64(lo)
	return float64(sorted[lo])*(1-f) + float64(sorted[hi])*f
}
