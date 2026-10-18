package geozarr

import (
	"context"
	"fmt"
	"slices"

	"github.com/LukasSelin/zarr"
)

// Units are the places data is kept for when it is not on a grid: regions,
// postcodes, catchments, the cells of a discrete global grid. Each has an
// int64 id and may have a bounding box. They are a group holding a "unit"
// array of the ids, a "unit_bbox" array of their boxes, n by 4, and the
// variables kept for them, each an array whose last dimension is "unit".
type Units struct {
	Group *zarr.Group
	CRS   CRS
	// IDs are the units' ids in the order the variables keep them.
	IDs []int64
	// Boxes are the units' bounding boxes, or nil if they have none.
	Boxes []BBox
	index map[int64]int
}

// UnitsOptions is what units are created with.
type UnitsOptions struct {
	IDs []int64
	// Boxes, if set, is the bounding box of each unit, in CRS.
	Boxes      []BBox
	CRS        CRS
	Attributes map[string]any
}

// CreateUnits creates units in a group name of g.
func CreateUnits(ctx context.Context, g *zarr.Group, name string, o UnitsOptions) (*Units, error) {
	n := len(o.IDs)
	if n == 0 {
		return nil, fmt.Errorf("geozarr: units %q has no ids", name)
	}
	if o.Boxes != nil && len(o.Boxes) != n {
		return nil, fmt.Errorf("geozarr: %d boxes for %d units", len(o.Boxes), n)
	}
	u := &Units{CRS: o.CRS, IDs: slices.Clone(o.IDs), Boxes: slices.Clone(o.Boxes)}
	if err := u.buildIndex(); err != nil {
		return nil, err
	}
	for _, b := range o.Boxes {
		if err := b.valid(); err != nil {
			return nil, err
		}
	}
	attrs := map[string]any{}
	for k, v := range o.Attributes {
		attrs[k] = v
	}
	o.CRS.attributes(attrs)
	ug, err := g.CreateGroup(ctx, name, attrs)
	if err != nil {
		return nil, err
	}
	u.Group = ug
	const chunk = 1 << 16
	ids, err := ug.CreateArray(ctx, "unit", zarr.ArrayOptions{
		Shape: []int{n}, ChunkShape: []int{min(n, chunk)}, DataType: zarr.Int64,
		DimensionNames: []string{"unit"},
	})
	if err != nil {
		return nil, err
	}
	if err := zarr.Write(ctx, ids, nil, nil, u.IDs); err != nil {
		return nil, err
	}
	if o.Boxes == nil {
		return u, nil
	}
	boxes, err := ug.CreateArray(ctx, "unit_bbox", zarr.ArrayOptions{
		Shape: []int{n, 4}, ChunkShape: []int{min(n, chunk), 4}, DataType: zarr.Float64,
		DimensionNames: []string{"unit", "bbox"},
	})
	if err != nil {
		return nil, err
	}
	flat := make([]float64, 0, 4*n)
	for _, b := range o.Boxes {
		flat = append(flat, b.MinX, b.MinY, b.MaxX, b.MaxY)
	}
	return u, zarr.Write(ctx, boxes, nil, nil, flat)
}

// OpenUnits opens the units name in g, reading every id, and every box if
// they have them, into memory: eight bytes a unit, and thirty-two more.
func OpenUnits(ctx context.Context, g *zarr.Group, name string) (*Units, error) {
	ug, err := g.OpenGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	u := &Units{Group: ug}
	if u.CRS, err = crsFrom(ug.Attribute); err != nil {
		return nil, err
	}
	ids, err := ug.OpenArray(ctx, "unit")
	if err != nil {
		return nil, err
	}
	if len(ids.Shape()) != 1 {
		return nil, fmt.Errorf("geozarr: units %q: unit has %d dimensions, not one", name, len(ids.Shape()))
	}
	if u.IDs, err = zarr.Read[int64](ctx, ids, nil, nil); err != nil {
		return nil, err
	}
	if err := u.buildIndex(); err != nil {
		return nil, err
	}
	children, err := ug.Children(ctx)
	if err != nil {
		return nil, err
	}
	if !slices.ContainsFunc(children, func(c zarr.Child) bool { return c.Name == "unit_bbox" }) {
		return u, nil
	}
	boxes, err := ug.OpenArray(ctx, "unit_bbox")
	if err != nil {
		return nil, err
	}
	if s := boxes.Shape(); len(s) != 2 || s[0] != len(u.IDs) || s[1] != 4 {
		return nil, fmt.Errorf("geozarr: units %q: unit_bbox is %v, not %d by 4", name, s, len(u.IDs))
	}
	flat, err := zarr.Read[float64](ctx, boxes, nil, nil)
	if err != nil {
		return nil, err
	}
	u.Boxes = make([]BBox, len(u.IDs))
	for i := range u.Boxes {
		u.Boxes[i] = BBox{flat[4*i], flat[4*i+1], flat[4*i+2], flat[4*i+3]}
	}
	return u, nil
}

func (u *Units) buildIndex() error {
	u.index = make(map[int64]int, len(u.IDs))
	for i, id := range u.IDs {
		if _, dup := u.index[id]; dup {
			return fmt.Errorf("geozarr: unit %d is there twice", id)
		}
		u.index[id] = i
	}
	return nil
}

// Index is where the unit id is along the unit dimension, and false if
// there is no such unit.
func (u *Units) Index(id int64) (int, bool) {
	i, ok := u.index[id]
	return i, ok
}

// Within is the ids of the units whose boxes meet b, in the order they are
// kept: none if the units have no boxes.
func (u *Units) Within(b BBox) []int64 {
	var ids []int64
	for i, ub := range u.Boxes {
		if ub.Intersects(b) {
			ids = append(ids, u.IDs[i])
		}
	}
	return ids
}

// Column is f of each unit's stats, in the units' order, and missing for a
// unit with none: a column of zonal statistics ready to Write to a variable.
func (u *Units) Column(stats map[int64]Stats, f func(Stats) float64, missing float64) []float64 {
	out := make([]float64, len(u.IDs))
	for i, id := range u.IDs {
		if s, ok := stats[id]; ok && s.Count > 0 {
			out[i] = f(s)
		} else {
			out[i] = missing
		}
	}
	return out
}

// VariableOptions is what a variable of units is created with.
type VariableOptions struct {
	// Leading are the dimensions before the unit: a time, a band. A Chunk
	// of zero is all of Size, so that one unit's series is one chunk.
	Leading   []Dim
	DataType  zarr.DataType
	FillValue any
	Codecs    []zarr.Codec
	// UnitChunk is how many units a chunk holds, 64 if zero. Reading one
	// unit fetches its whole chunk.
	UnitChunk int
	// UnitShard, if set, keeps the chunks in shards of this many units, a
	// multiple of UnitChunk. Many units want it: a million in chunks of 64
	// are sixteen thousand objects, and in shards of 8192 a hundred and
	// twenty-three, while one unit still costs one ranged read.
	UnitShard  int
	Attributes map[string]any
}

// Variable is an array of values kept for units.
type Variable struct {
	Array *zarr.Array
	Units *Units
}

// CreateVariable creates a variable of the units.
func (u *Units) CreateVariable(ctx context.Context, name string, o VariableOptions) (*Variable, error) {
	uc := o.UnitChunk
	if uc == 0 {
		uc = 64
	}
	if uc < 0 || o.UnitShard < 0 || (o.UnitShard != 0 && o.UnitShard%uc != 0) {
		return nil, fmt.Errorf("geozarr: unit shard %d is not a multiple of unit chunk %d", o.UnitShard, uc)
	}
	var shape, chunks []int
	var dims []string
	for _, d := range o.Leading {
		c := d.Chunk
		if c == 0 {
			c = max(d.Size, 1)
		}
		shape, chunks, dims = append(shape, d.Size), append(chunks, c), append(dims, d.Name)
	}
	shape = append(shape, len(u.IDs))
	chunks = append(chunks, uc)
	dims = append(dims, "unit")
	var shards []int
	if o.UnitShard != 0 {
		shards = append(slices.Clone(chunks[:len(chunks)-1]), o.UnitShard)
	}
	a, err := u.Group.CreateArray(ctx, name, zarr.ArrayOptions{
		Shape: shape, ChunkShape: chunks, ShardShape: shards,
		DataType: o.DataType, FillValue: o.FillValue, Codecs: o.Codecs,
		DimensionNames: dims, Attributes: o.Attributes,
	})
	if err != nil {
		return nil, err
	}
	return &Variable{Array: a, Units: u}, nil
}

// OpenVariable opens a variable of the units.
func (u *Units) OpenVariable(ctx context.Context, name string) (*Variable, error) {
	a, err := u.Group.OpenArray(ctx, name)
	if err != nil {
		return nil, err
	}
	s := a.Shape()
	if len(s) == 0 || s[len(s)-1] != len(u.IDs) {
		return nil, fmt.Errorf("geozarr: %q is %v, not along %d units", a.Path(), s, len(u.IDs))
	}
	return &Variable{Array: a, Units: u}, nil
}

// ReadUnit reads every value of the unit id: its whole series, row-major
// over the leading dimensions.
func ReadUnit[T zarr.Element](ctx context.Context, v *Variable, id int64) ([]T, error) {
	vals, err := ReadUnits[T](ctx, v, []int64{id})
	if err != nil {
		return nil, err
	}
	return vals[0], nil
}

// ReadUnits reads every value of each of ids, as ReadUnit does. The units are
// read a chunk of units at a time, each chunk once however many of ids are
// in it, and as many chunks at once as the array's Concurrency.
func ReadUnits[T zarr.Element](ctx context.Context, v *Variable, ids []int64) ([][]T, error) {
	a := v.Array
	shape := a.Shape()
	lead := shape[:len(shape)-1]
	n := product(lead)
	uc := a.ChunkShape()[len(shape)-1]
	byChunk := map[int][]int{} // chunk of units to the places in ids
	var order []int
	for p, id := range ids {
		i, ok := v.Units.Index(id)
		if !ok {
			return nil, fmt.Errorf("geozarr: no unit %d", id)
		}
		c := i / uc
		if _, seen := byChunk[c]; !seen {
			order = append(order, c)
		}
		byChunk[c] = append(byChunk[c], p)
	}
	out := make([][]T, len(ids))
	workers := a.Concurrency
	if workers <= 0 {
		workers = 16
	}
	err := parallel(ctx, len(order), workers, nil, func(ctx context.Context, k int, _ any) error {
		c := order[k]
		first := c * uc
		width := min(uc, shape[len(shape)-1]-first)
		start := make([]int, len(shape))
		start[len(shape)-1] = first
		block, err := zarr.Read[T](ctx, a, start, append(slices.Clone(lead), width))
		if err != nil {
			return err
		}
		for _, p := range byChunk[c] {
			i, _ := v.Units.Index(ids[p])
			col := make([]T, n)
			for j := range col {
				col[j] = block[j*width+i-first]
			}
			out[p] = col
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
