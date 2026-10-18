package geodb

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/LukasSelin/zarr"
)

// ParcelObs is the statistics of a parcel in one scene. Values are NaN
// where the parcel had no valid pixel.
type ParcelObs struct {
	Time          time.Time `json:"time"`
	Tile          string    `json:"tile"`
	Pixels        int       `json:"pixels"` // of the parcel in the tile
	ValidFraction float32   `json:"valid_fraction"`
	Mean          float32   `json:"mean"`
	Std           float32   `json:"std"`
	P10           float32   `json:"p10"`
	P50           float32   `json:"p50"`
	P90           float32   `json:"p90"`
}

// valid is how many pixels of the parcel had a value.
func (o ParcelObs) valid() float64 {
	if o.ValidFraction != o.ValidFraction {
		return 0
	}
	return float64(o.ValidFraction) * float64(o.Pixels)
}

// ParcelSeries is the statistics of a parcel in every scene of every tile
// it is in, in order of time. Tiles overlap, so where two tiles saw the
// parcel within a minute of each other - the same pass - it is the one that
// saw more of its pixels with a value. It reads, in each tile, a chunk for
// every StatsScenes scenes.
func (db *DB) ParcelSeries(ctx context.Context, id int64, variable string) ([]ParcelObs, error) {
	set := db.parcels.Load()
	ord, ok := set.byIDOK(id)
	if !ok {
		return nil, fmt.Errorf("%w: parcel %d", ErrNotFound, id)
	}
	var all []ParcelObs
	known := false
	for _, t := range db.Tiles() {
		st := t.state.Load()
		if _, ok := st.meta.variable(variable); !ok {
			continue
		}
		known = true
		tp := st.parcels
		if tp == nil || tp.generation != set.generation {
			continue
		}
		local, ok := tp.local[int64(ord)]
		if !ok {
			continue
		}
		n := st.statsN[variable]
		if n == 0 {
			continue
		}
		data, err := zarr.Read[float32](ctx, st.stats[variable], []int{local, 0, 0}, []int{1, n, numStats})
		if err != nil {
			return nil, err
		}
		for i := range n {
			s := data[i*numStats : (i+1)*numStats]
			all = append(all, ParcelObs{
				Time:          time.Unix(st.times[i], 0).UTC(),
				Tile:          t.name,
				Pixels:        int(tp.npix[local]),
				ValidFraction: s[StatValidFraction],
				Mean:          s[StatMean],
				Std:           s[StatStd],
				P10:           s[StatP10],
				P50:           s[StatP50],
				P90:           s[StatP90],
			})
		}
	}
	if !known {
		return nil, fmt.Errorf("%w: variable %s in any tile", ErrNotFound, variable)
	}
	sort.SliceStable(all, func(a, b int) bool { return all[a].Time.Before(all[b].Time) })
	out := all[:0]
	for _, o := range all {
		if k := len(out) - 1; k >= 0 && o.Tile != out[k].Tile && o.Time.Sub(out[k].Time) < time.Minute {
			if o.valid() > out[k].valid() {
				out[k] = o
			}
			continue
		}
		out = append(out, o)
	}
	return out, nil
}
