package geozarr

import (
	"context"
	"fmt"
	"slices"

	"github.com/LukasSelin/zarr"
)

// Dim is a dimension of a raster before its rows and columns - a band, a
// time - or of a unit variable before its units.
type Dim struct {
	Name string
	Size int
	// Chunk is how many of the dimension a chunk holds; zero is one for a
	// raster and the whole of Size for a unit variable.
	Chunk int
}

// RasterOptions is what a raster is created with.
type RasterOptions struct {
	Width, Height int
	// Leading are the dimensions before y and x, outermost first.
	Leading   []Dim
	DataType  zarr.DataType
	FillValue any
	CRS       CRS
	Transform Transform
	// ChunkSize is the side of a chunk in pixels, 512 if zero. A point read
	// fetches one chunk, so smaller chunks make points cheaper and whole
	// reads dearer.
	ChunkSize int
	// ShardSize, if set, keeps the chunks in shards this many pixels on a
	// side, a multiple of ChunkSize: fewer, larger objects to write, while a
	// store that reads ranges still fetches one chunk for a point.
	ShardSize int
	// Codecs is the pipeline of each chunk; nil is little-endian bytes,
	// uncompressed.
	Codecs []zarr.Codec
	// Coordinates also writes, next to the raster, the x and y coordinates
	// of pixel centres and a spatial_ref array holding the CRS and the
	// GeoTransform, as xarray, rioxarray and GDAL look for them. An array
	// of that name already in the group is taken to be the same grid's and
	// left alone. A rotated transform has no x and y.
	Coordinates bool
	Attributes  map[string]any
}

// Raster is an array whose last two dimensions are rows and columns of
// pixels at known places on the earth.
type Raster struct {
	Array     *zarr.Array
	CRS       CRS
	Transform Transform
	lead      int
}

const defaultChunk = 512

// CreateRaster creates a raster in g. Its attributes say where it is under
// the proj: and spatial: conventions of GeoZarr, and in GDAL's _CRS.
func CreateRaster(ctx context.Context, g *zarr.Group, name string, o RasterOptions) (*Raster, error) {
	if o.Width <= 0 || o.Height <= 0 {
		return nil, fmt.Errorf("geozarr: raster of %d by %d", o.Height, o.Width)
	}
	if err := o.Transform.valid(); err != nil {
		return nil, err
	}
	cs := o.ChunkSize
	if cs == 0 {
		cs = defaultChunk
	}
	if cs < 0 || o.ShardSize < 0 || (o.ShardSize != 0 && o.ShardSize%cs != 0) {
		return nil, fmt.Errorf("geozarr: shard size %d is not a multiple of chunk size %d", o.ShardSize, cs)
	}
	var shape, chunks, shards []int
	var names []string
	for _, d := range o.Leading {
		c := d.Chunk
		if c == 0 {
			c = 1
		}
		shape, chunks, names = append(shape, d.Size), append(chunks, c), append(names, d.Name)
	}
	shape = append(shape, o.Height, o.Width)
	chunks = append(chunks, cs, cs)
	names = append(names, "y", "x")
	if o.ShardSize != 0 {
		shards = append(slices.Clone(chunks[:len(o.Leading)]), o.ShardSize, o.ShardSize)
	}

	r := &Raster{CRS: o.CRS, Transform: o.Transform, lead: len(o.Leading)}
	attrs := map[string]any{}
	for k, v := range o.Attributes {
		attrs[k] = v
	}
	o.CRS.attributes(attrs)
	t := o.Transform
	b := r.boundsOf(o.Height, o.Width)
	attrs["spatial:dimensions"] = []string{"y", "x"}
	attrs["spatial:transform"] = []float64{t.A, t.B, t.C, t.D, t.E, t.F}
	attrs["spatial:shape"] = []int{o.Height, o.Width}
	attrs["spatial:bbox"] = []float64{b.MinX, b.MinY, b.MaxX, b.MaxY}
	attrs["spatial:registration"] = "pixel"
	if o.Coordinates {
		attrs["grid_mapping"] = "spatial_ref"
	}

	a, err := g.CreateArray(ctx, name, zarr.ArrayOptions{
		Shape:          shape,
		ChunkShape:     chunks,
		ShardShape:     shards,
		DataType:       o.DataType,
		FillValue:      o.FillValue,
		Codecs:         o.Codecs,
		DimensionNames: names,
		Attributes:     attrs,
	})
	if err != nil {
		return nil, err
	}
	r.Array = a
	if o.Coordinates {
		if err := r.writeCoordinates(ctx, g); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// writeCoordinates writes x, y and spatial_ref, each unless g has it.
func (r *Raster) writeCoordinates(ctx context.Context, g *zarr.Group) error {
	have := map[string]bool{}
	children, err := g.Children(ctx)
	if err != nil {
		return err
	}
	for _, c := range children {
		have[c.Name] = true
	}
	if !have["spatial_ref"] {
		gt := r.Transform.GDAL()
		attrs := map[string]any{
			"GeoTransform": fmt.Sprintf("%.17g %.17g %.17g %.17g %.17g %.17g", gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]),
		}
		if r.CRS.WKT2 != "" {
			attrs["crs_wkt"] = r.CRS.WKT2
			attrs["spatial_ref"] = r.CRS.WKT2
		}
		if r.CRS.EPSG != 0 {
			attrs["epsg"] = r.CRS.EPSG
		}
		if _, err := g.CreateArray(ctx, "spatial_ref", zarr.ArrayOptions{
			Shape: []int{}, ChunkShape: []int{}, DataType: zarr.Int64, Attributes: attrs,
		}); err != nil {
			return err
		}
	}
	if r.Transform.rotated() {
		return nil
	}
	h, w := r.Height(), r.Width()
	for _, c := range []struct {
		name string
		n    int
		at   func(i int) float64
	}{
		{"x", w, func(i int) float64 { x, _ := r.Transform.Apply(float64(i)+0.5, 0.5); return x }},
		{"y", h, func(i int) float64 { _, y := r.Transform.Apply(0.5, float64(i)+0.5); return y }},
	} {
		if have[c.name] {
			continue
		}
		v := make([]float64, c.n)
		for i := range v {
			v[i] = c.at(i)
		}
		a, err := g.CreateArray(ctx, c.name, zarr.ArrayOptions{
			Shape: []int{c.n}, ChunkShape: []int{c.n}, DataType: zarr.Float64,
			DimensionNames: []string{c.name},
		})
		if err != nil {
			return err
		}
		if err := zarr.Write(ctx, a, nil, nil, v); err != nil {
			return err
		}
	}
	return nil
}

// OpenRaster opens the raster name in g: an array with a spatial:transform,
// or, failing that, a GeoTransform on the array grid_mapping names.
func OpenRaster(ctx context.Context, g *zarr.Group, name string) (*Raster, error) {
	a, err := g.OpenArray(ctx, name)
	if err != nil {
		return nil, err
	}
	return rasterOf(ctx, g, a)
}

func rasterOf(ctx context.Context, g *zarr.Group, a *zarr.Array) (*Raster, error) {
	if len(a.Shape()) < 2 {
		return nil, fmt.Errorf("geozarr: %q has %d dimensions, not rows and columns", a.Path(), len(a.Shape()))
	}
	r := &Raster{Array: a, lead: len(a.Shape()) - 2}
	var err error
	if r.CRS, err = crsFrom(a.Attribute); err != nil {
		return nil, err
	}
	var t []float64
	ok, err := a.Attribute("spatial:transform", &t)
	if err != nil {
		return nil, err
	}
	switch {
	case ok && len(t) == 6:
		r.Transform = Transform{t[0], t[1], t[2], t[3], t[4], t[5]}
	case ok:
		return nil, fmt.Errorf("geozarr: %q has a spatial:transform of %d numbers", a.Path(), len(t))
	default:
		if r.Transform, err = gridMapping(ctx, g, a, &r.CRS); err != nil {
			return nil, err
		}
	}
	if err := r.Transform.valid(); err != nil {
		return nil, fmt.Errorf("%w: %q", err, a.Path())
	}
	return r, nil
}

// gridMapping is the transform, and the CRS if crs has none, of the CF grid
// mapping variable a names, as rioxarray writes it.
func gridMapping(ctx context.Context, g *zarr.Group, a *zarr.Array, crs *CRS) (Transform, error) {
	var name string
	if ok, err := a.Attribute("grid_mapping", &name); err != nil {
		return Transform{}, err
	} else if !ok {
		return Transform{}, fmt.Errorf("geozarr: %q has neither spatial:transform nor grid_mapping", a.Path())
	}
	gm, err := g.OpenArray(ctx, name)
	if err != nil {
		return Transform{}, fmt.Errorf("geozarr: grid mapping of %q: %w", a.Path(), err)
	}
	var s string
	if ok, err := gm.Attribute("GeoTransform", &s); err != nil {
		return Transform{}, err
	} else if !ok {
		return Transform{}, fmt.Errorf("geozarr: grid mapping %q has no GeoTransform", name)
	}
	var gt [6]float64
	if n, err := fmt.Sscan(s, &gt[0], &gt[1], &gt[2], &gt[3], &gt[4], &gt[5]); err != nil || n != 6 {
		return Transform{}, fmt.Errorf("geozarr: GeoTransform %q is not six numbers", s)
	}
	if crs.IsZero() {
		if _, err := gm.Attribute("crs_wkt", &crs.WKT2); err != nil {
			return Transform{}, err
		}
	}
	return FromGDAL(gt), nil
}

// Height is the raster's number of rows.
func (r *Raster) Height() int { return r.Array.Shape()[r.lead] }

// Width is the raster's number of columns.
func (r *Raster) Width() int { return r.Array.Shape()[r.lead+1] }

// Leading is the size of each dimension before the rows and columns.
func (r *Raster) Leading() []int { return r.Array.Shape()[:r.lead] }

// Bounds is the box the raster covers in map coordinates.
func (r *Raster) Bounds() BBox { return r.boundsOf(r.Height(), r.Width()) }

func (r *Raster) boundsOf(h, w int) BBox {
	return r.Transform.bounds(0, 0, float64(w), float64(h))
}

// Window is the pixels the box b touches, clipped to the raster; empty if b
// misses it.
func (r *Raster) Window(b BBox) Window { return r.Transform.window(b, r.Height(), r.Width()) }

// WindowTransform is the transform of a window read out of the raster.
func (r *Raster) WindowTransform(w Window) Transform { return r.Transform.shift(w.Col, w.Row) }

// Pixel is the row and column of the pixel (x, y) is in, and false if
// (x, y) is off the raster.
func (r *Raster) Pixel(x, y float64) (row, col int, ok bool) {
	c, rw := r.Transform.Invert(x, y)
	if c < 0 || rw < 0 || c >= float64(r.Width()) || rw >= float64(r.Height()) {
		return 0, 0, false
	}
	return int(rw), int(c), true
}

func (r *Raster) checkLead(lead []int) error {
	if len(lead) != r.lead {
		return fmt.Errorf("geozarr: %d leading indices for a raster with %d leading dimensions", len(lead), r.lead)
	}
	for i, n := range r.Leading() {
		if lead[i] < 0 || lead[i] >= n {
			return fmt.Errorf("geozarr: leading index %v out of %v", lead, r.Leading())
		}
	}
	return nil
}

// ReadWindow reads the pixels of w in the plane at lead, one index for each
// leading dimension, row by row.
func ReadWindow[T zarr.Element](ctx context.Context, r *Raster, lead []int, w Window) ([]T, error) {
	if err := r.checkLead(lead); err != nil {
		return nil, err
	}
	if w.Empty() {
		return nil, nil
	}
	start := append(slices.Clone(lead), w.Row, w.Col)
	shape := make([]int, r.lead, r.lead+2)
	for i := range shape {
		shape[i] = 1
	}
	return zarr.Read[T](ctx, r.Array, start, append(shape, w.Height, w.Width))
}

// ReadBBox reads the pixels the box b touches in the plane at lead, and the
// window they are. Only the chunks the window is in are fetched. A box that
// misses the raster reads nothing.
func ReadBBox[T zarr.Element](ctx context.Context, r *Raster, lead []int, b BBox) ([]T, Window, error) {
	if err := b.valid(); err != nil {
		return nil, Window{}, err
	}
	w := r.Window(b)
	v, err := ReadWindow[T](ctx, r, lead, w)
	return v, w, err
}

// Block is a part of a raster that Ingest asks for: Count[i] of leading
// dimension i from Start[i], and a window of each plane.
type Block struct {
	Start, Count []int
	Window
}

// Ingest writes the whole raster, block by block, as fast as fill can make
// them. Each block is exactly one stored object - a shard, or a chunk if the
// raster is not sharded - clipped to the raster, so no write reads anything
// back, and as many are made and written at once as the array's
// Concurrency, sixteen if zero. fill is called from as many goroutines and
// writes the block into dst row-major: leading dimensions, then rows, then
// columns. It must not keep dst.
func Ingest[T zarr.Element](ctx context.Context, r *Raster, fill func(b Block, dst []T) error) error {
	a := r.Array
	unit := a.ShardShape()
	if unit == nil {
		unit = a.ChunkShape()
	}
	shape := a.Shape()
	n := make([]int, len(shape))
	for i := range n {
		n[i] = (shape[i] + unit[i] - 1) / unit[i]
	}
	full := 1
	for _, u := range unit {
		full *= u
	}
	workers := a.Concurrency
	if workers <= 0 {
		workers = 16
	}
	return parallel(ctx, product(n), workers, func() any { return make([]T, full) }, func(ctx context.Context, i int, scratch any) error {
		idx := unravel(i, n)
		start, count := make([]int, len(n)), make([]int, len(n))
		for d := range n {
			start[d] = idx[d] * unit[d]
			count[d] = min(unit[d], shape[d]-start[d])
		}
		dst := scratch.([]T)[:product(count)]
		b := Block{
			Start: start[:r.lead], Count: count[:r.lead],
			Window: Window{Row: start[r.lead], Col: start[r.lead+1], Height: count[r.lead], Width: count[r.lead+1]},
		}
		if err := fill(b, dst); err != nil {
			return err
		}
		return zarr.Write(ctx, a, start, count, dst)
	})
}

// parallel calls do for each of 0 to n-1 from up to workers goroutines, each
// with a scratch value of its own from mk, and stops at the first error.
func parallel(ctx context.Context, n, workers int, mk func() any, do func(ctx context.Context, i int, scratch any) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	next := make(chan int)
	done := make(chan struct{})
	workers = max(1, min(workers, n))
	for range workers {
		go func() {
			defer func() { done <- struct{}{} }()
			var scratch any
			if mk != nil {
				scratch = mk()
			}
			for i := range next {
				if err := do(ctx, i, scratch); err != nil {
					cancel(err)
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
	for range workers {
		<-done
	}
	return context.Cause(ctx)
}

func product(s []int) int {
	p := 1
	for _, v := range s {
		p *= v
	}
	return p
}

// unravel is the index of the i-th element of an array of shape n, row-major.
func unravel(i int, n []int) []int {
	idx := make([]int, len(n))
	for d := len(n) - 1; d >= 0; d-- {
		idx[d] = i % n[d]
		i /= n[d]
	}
	return idx
}
