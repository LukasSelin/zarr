package geodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/LukasSelin/zarr"
)

// Parcel is a field or any other area statistics are kept for: rings in
// longitude and latitude, filled by the even-odd rule, so that a hole is a
// ring inside another and a parcel in two parts is two rings apart. A ring
// is closed whether or not its last point repeats its first.
type Parcel struct {
	ID    int64
	Rings [][]Point
}

// parcelSet is the parcels as read from the store, in their stored order.
type parcelSet struct {
	generation int64
	ids        []int64
	boxes      []BBox
	ringOff    []int64 // rings of parcel i are ringOff[i] to ringOff[i+1]
	coordOff   []int64 // points of ring j are coordOff[j] to coordOff[j+1]
	coords     []float64
	byID       map[int64]int
	tree       *rtree
}

func (s *parcelSet) len() int {
	if s == nil {
		return 0
	}
	return len(s.ids)
}

// rings are the rings of the parcel at ordinal i, as views of the store's
// coordinates.
func (s *parcelSet) rings(i int) [][]Point {
	out := make([][]Point, 0, s.ringOff[i+1]-s.ringOff[i])
	for j := s.ringOff[i]; j < s.ringOff[i+1]; j++ {
		c := s.coords[2*s.coordOff[j] : 2*s.coordOff[j+1]]
		ring := make([]Point, len(c)/2)
		for k := range ring {
			ring[k] = Point{c[2*k], c[2*k+1]}
		}
		out = append(out, ring)
	}
	return out
}

func validateParcels(ps []Parcel) error {
	seen := make(map[int64]bool, len(ps))
	for _, p := range ps {
		if seen[p.ID] {
			return fmt.Errorf("geodb: parcel %d is given twice", p.ID)
		}
		seen[p.ID] = true
		if len(p.Rings) == 0 {
			return fmt.Errorf("geodb: parcel %d has no rings", p.ID)
		}
		for _, r := range p.Rings {
			if len(r) < 3 {
				return fmt.Errorf("geodb: parcel %d has a ring of %d points", p.ID, len(r))
			}
			for _, pt := range r {
				if !(pt.X >= -180 && pt.X <= 180 && pt.Y >= -90 && pt.Y <= 90) {
					return fmt.Errorf("geodb: parcel %d has the point (%g, %g), which is no longitude and latitude", p.ID, pt.X, pt.Y)
				}
			}
		}
	}
	return nil
}

func ringsBBox(rings [][]Point) BBox {
	b := emptyBBox()
	for _, r := range rings {
		for _, p := range r {
			b = b.extend(p)
		}
	}
	return b
}

const parcelChunk = 1 << 16

// PutParcels replaces the parcels with ps. It is the one write of parcels
// there is: the statistics of every tile and the samples of every weather
// source are of the parcels they were made for, so it drops them, indexes
// each tile anew and works out its statistics again from the scenes it
// holds, which reads every scene. Do it before the scenes come, or when the
// parcels change, not often.
//
// The parcels are kept in the order of a Hilbert curve through the centres
// of their boxes, so that parcels near each other are near each other in
// the store and in the chunks of statistics.
func (db *DB) PutParcels(ctx context.Context, ps []Parcel) error {
	if err := validateParcels(ps); err != nil {
		return err
	}
	db.adminMu.Lock()
	defer db.adminMu.Unlock()

	boxes := make([]BBox, len(ps))
	for i, p := range ps {
		boxes[i] = ringsBBox(p.Rings)
	}
	keys := hilbertKeys(boxes)
	order := make([]int, len(ps))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return keys[order[a]] < keys[order[b]] })

	n := len(ps)
	ids := make([]int64, n)
	bbox := make([]float64, 4*n)
	ringOff := append(make([]int64, 0, n+1), 0)
	coordOff := []int64{0}
	var coords []float64
	for k, i := range order {
		p := ps[i]
		ids[k] = p.ID
		b := boxes[i]
		copy(bbox[4*k:], []float64{b.MinX, b.MinY, b.MaxX, b.MaxY})
		for _, r := range p.Rings {
			for _, pt := range r {
				coords = append(coords, pt.X, pt.Y)
			}
			coordOff = append(coordOff, int64(len(coords)/2))
		}
		ringOff = append(ringOff, int64(len(coordOff)-1))
	}

	// What is made of the old parcels goes first, then the parcels, whose
	// count is written last: a PutParcels that fails part way leaves no
	// parcels rather than half of them.
	for _, t := range db.Tiles() {
		if err := zarr.Delete(ctx, db.store, joinPath(t.path, "stats")); err != nil {
			return err
		}
	}
	if err := zarr.Delete(ctx, db.store, "parcel_weather"); err != nil {
		return err
	}
	if _, err := zarr.CreateGroup(ctx, db.store, "parcel_weather", nil); err != nil {
		return err
	}
	if err := zarr.Delete(ctx, db.store, "parcels"); err != nil {
		return err
	}
	g, err := zarr.CreateGroup(ctx, db.store, "parcels", nil)
	if err != nil {
		return err
	}
	if err := writeVector(ctx, g, "id", []int{n}, []int{parcelChunk}, []string{"parcel"}, ids); err != nil {
		return err
	}
	if err := writeVector(ctx, g, "bbox", []int{n, 4}, []int{parcelChunk, 4}, []string{"parcel", "bound"}, bbox); err != nil {
		return err
	}
	if err := writeVector(ctx, g, "ring_offset", []int{n + 1}, []int{parcelChunk}, []string{"parcel_edge"}, ringOff); err != nil {
		return err
	}
	if err := writeVector(ctx, g, "coord_offset", []int{len(coordOff)}, []int{parcelChunk}, []string{"ring_edge"}, coordOff); err != nil {
		return err
	}
	if err := writeVector(ctx, g, "coords", []int{len(coords) / 2, 2}, []int{4 * parcelChunk, 2}, []string{"point", "axis"}, coords); err != nil {
		return err
	}
	gen := time.Now().UnixNano()
	if err := g.SetAttributes(ctx, map[string]any{"count": n, "generation": gen, "crs": "EPSG:4326"}); err != nil {
		return err
	}
	set, err := loadParcels(ctx, db.store)
	if err != nil {
		return err
	}
	db.parcels.Store(set)
	for _, w := range db.WeatherSources() {
		if err := w.reload(ctx); err != nil {
			return err
		}
	}
	for _, t := range db.Tiles() {
		if err := t.indexParcels(ctx); err != nil {
			return fmt.Errorf("geodb: indexing parcels of tile %s: %w", t.name, err)
		}
	}
	for _, w := range db.WeatherSources() {
		if err := w.SampleParcels(ctx); err != nil {
			return fmt.Errorf("geodb: sampling weather source %s at the parcels: %w", w.name, err)
		}
	}
	return nil
}

func writeVector[T zarr.Element](ctx context.Context, g *zarr.Group, name string, shape, chunks []int, dims []string, data []T) error {
	for k := range chunks {
		chunks[k] = max(1, min(chunks[k], shape[k]))
	}
	a, err := g.CreateArray(ctx, name, zarr.ArrayOptions{
		Shape:          shape,
		ChunkShape:     chunks,
		DataType:       zarr.DataTypeOf[T](),
		Codecs:         codecs(zarr.DataTypeOf[T]()),
		DimensionNames: dims,
	})
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return zarr.Write(ctx, a, nil, nil, data)
}

func readVector[T zarr.Element](ctx context.Context, s zarr.Store, path string) ([]T, error) {
	a, err := zarr.OpenArray(ctx, s, path)
	if err != nil {
		return nil, err
	}
	for _, d := range a.Shape() {
		if d == 0 {
			return nil, nil
		}
	}
	return zarr.Read[T](ctx, a, nil, nil)
}

// loadParcels reads the parcels, or nil if there are none.
func loadParcels(ctx context.Context, s zarr.Store) (*parcelSet, error) {
	g, err := zarr.OpenGroup(ctx, s, "parcels")
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var count int
	ok, err := g.Attribute("count", &count)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // a PutParcels that did not finish
	}
	set := &parcelSet{}
	if _, err := g.Attribute("generation", &set.generation); err != nil {
		return nil, err
	}
	if set.ids, err = readVector[int64](ctx, s, "parcels/id"); err != nil {
		return nil, err
	}
	bbox, err := readVector[float64](ctx, s, "parcels/bbox")
	if err != nil {
		return nil, err
	}
	if set.ringOff, err = readVector[int64](ctx, s, "parcels/ring_offset"); err != nil {
		return nil, err
	}
	if set.coordOff, err = readVector[int64](ctx, s, "parcels/coord_offset"); err != nil {
		return nil, err
	}
	if set.coords, err = readVector[float64](ctx, s, "parcels/coords"); err != nil {
		return nil, err
	}
	if len(set.ids) != count || len(bbox) != 4*count || len(set.ringOff) != count+1 {
		return nil, errors.New("geodb: the parcels in the store do not agree with their count")
	}
	nRings, nPoints := int64(len(set.coordOff)-1), int64(len(set.coords)/2)
	if !offsetsOK(set.ringOff, nRings) || !offsetsOK(set.coordOff, nPoints) {
		return nil, errors.New("geodb: the offsets of the parcels in the store are broken")
	}
	set.boxes = make([]BBox, count)
	set.byID = make(map[int64]int, count)
	for i := range set.boxes {
		set.boxes[i] = BBox{bbox[4*i], bbox[4*i+1], bbox[4*i+2], bbox[4*i+3]}
		set.byID[set.ids[i]] = i
	}
	set.tree = newRTree(set.boxes)
	return set, nil
}

// offsetsOK is whether off runs from 0 to end and never falls.
func offsetsOK(off []int64, end int64) bool {
	if len(off) == 0 || off[0] != 0 || off[len(off)-1] != end {
		return false
	}
	return slices.IsSorted(off)
}

// ParcelsIn is the IDs of the parcels whose boxes meet b, in longitude and
// latitude.
func (db *DB) ParcelsIn(b BBox) []int64 {
	set := db.parcels.Load()
	if set.len() == 0 {
		return nil
	}
	var out []int64
	set.tree.search(b, func(i int) { out = append(out, set.ids[i]) })
	return out
}

// ParcelAt is the IDs of the parcels p is inside, in longitude and latitude.
func (db *DB) ParcelAt(p Point) []int64 {
	set := db.parcels.Load()
	if set.len() == 0 {
		return nil
	}
	var out []int64
	set.tree.search(BBox{p.X, p.Y, p.X, p.Y}, func(i int) {
		if inside(set.rings(i), p) {
			out = append(out, set.ids[i])
		}
	})
	return out
}

// GetParcel is the parcel with the given ID.
func (db *DB) GetParcel(id int64) (Parcel, bool) {
	set := db.parcels.Load()
	if set.len() == 0 {
		return Parcel{}, false
	}
	i, ok := set.byID[id]
	if !ok {
		return Parcel{}, false
	}
	return Parcel{ID: id, Rings: set.rings(i)}, true
}

// NumParcels is how many parcels there are.
func (db *DB) NumParcels() int { return db.parcels.Load().len() }

// inside is whether p is inside rings by the even-odd rule.
func inside(rings [][]Point, p Point) bool {
	in := false
	for _, r := range rings {
		for i, j := 0, len(r)-1; i < len(r); j, i = i, i+1 {
			a, b := r[i], r[j]
			if (a.Y > p.Y) != (b.Y > p.Y) && p.X < a.X+(p.Y-a.Y)*(b.X-a.X)/(b.Y-a.Y) {
				in = !in
			}
		}
	}
	return in
}

// span is a run of pixels in one row: columns c0 to c1, c1 not included.
type span struct{ row, c0, c1 int32 }

// rasterize is the pixels of g whose centres are inside rings, which are in
// g's coordinates, as runs along rows. A parcel too small to hold the centre
// of any pixel is the pixel its own centre is in.
func rasterize(g Grid, rings [][]Point) []span {
	b := ringsBBox(rings)
	w, ok := g.Window(b)
	if !ok {
		return smallParcel(g, b)
	}
	var out []span
	var xs []float64
	for row := w.Row; row < w.Row+w.Height; row++ {
		y := g.Y0 - (float64(row)+0.5)*g.Dy
		xs = xs[:0]
		for _, r := range rings {
			for i, j := 0, len(r)-1; i < len(r); j, i = i, i+1 {
				a, c := r[i], r[j]
				if (a.Y > y) != (c.Y > y) {
					xs = append(xs, a.X+(y-a.Y)*(c.X-a.X)/(c.Y-a.Y))
				}
			}
		}
		sort.Float64s(xs)
		for k := 0; k+1 < len(xs); k += 2 {
			c0 := math.Max(0, math.Ceil((xs[k]-g.X0)/g.Dx-0.5))
			c1 := math.Min(float64(g.Width), math.Ceil((xs[k+1]-g.X0)/g.Dx-0.5))
			if c1 > c0 {
				out = append(out, span{int32(row), int32(c0), int32(c1)})
			}
		}
	}
	if len(out) == 0 {
		return smallParcel(g, b)
	}
	return out
}

func smallParcel(g Grid, b BBox) []span {
	if col, row, ok := g.Pixel(b.center()); ok {
		return []span{{int32(row), int32(col), int32(col + 1)}}
	}
	return nil
}
