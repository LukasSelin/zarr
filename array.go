package zarr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

// Array is an array in a store.
type Array struct {
	store Store
	path  string
	meta  ArrayMetadata
	keys  keyEncoding
	fill  any
	// codecs is the whole pipeline, which a stored object - a chunk, or a
	// shard - is encoded through.
	codecs pipeline
	// grid is the shape of a stored object: the chunk_grid of the metadata.
	// chunks is the shape of a chunk read or written, which is grid unless
	// the array is sharded; perShard is how many chunks one stored object
	// holds along each dimension.
	grid, chunks, perShard []int
	// shard is the sharding codec of a sharded array, and indexSize the
	// length of a shard's index; nil and 0 otherwise.
	shard     *ShardingCodec
	indexSize int
	// WriteEmptyChunks keeps a chunk that holds nothing but the fill value.
	// By default such a chunk is not stored; it reads as the fill value
	// either way.
	WriteEmptyChunks bool
}

// ArrayOptions is what an array is created with.
type ArrayOptions struct {
	Shape      []int
	ChunkShape []int
	// ShardShape, if set, keeps the chunks in shards of this shape, each a
	// whole number of chunks along every dimension; Codecs are then the
	// codecs of each chunk within a shard. See ShardingCodec.
	ShardShape []int
	DataType   DataType
	// FillValue is what an element nobody wrote reads as. Nil is zero; a Go
	// number of another type is taken if the data type holds it exactly.
	FillValue any
	// Codecs is the pipeline chunks are encoded through. Nil is little-endian
	// bytes, uncompressed.
	Codecs []Codec
	// Separator is between the indices of a chunk's key: "/" (the default)
	// or ".".
	Separator      string
	DimensionNames []string
	Attributes     map[string]any
}

// CreateArray creates an array at path, which must not hold a node already.
// It does not create the groups above it.
func CreateArray(ctx context.Context, s Store, path string, o ArrayOptions) (*Array, error) {
	if err := mustBeNew(ctx, s, path); err != nil {
		return nil, err
	}
	if o.DataType.Size() == 0 {
		return nil, fmt.Errorf("%w: data type %q", ErrUnsupported, o.DataType)
	}
	fill, err := fillFrom(o.DataType, o.FillValue)
	if err != nil {
		return nil, err
	}
	if o.Codecs == nil {
		o.Codecs = []Codec{BytesCodec{Endian: Little}}
	}
	if o.Separator == "" {
		o.Separator = "/"
	}
	grid := o.ChunkShape
	if o.ShardShape != nil {
		o.Codecs = []Codec{&ShardingCodec{ChunkShape: o.ChunkShape, Codecs: o.Codecs}}
		grid = o.ShardShape
	}
	gridJSON, _ := json.Marshal(map[string][]int{"chunk_shape": grid})
	m := ArrayMetadata{
		ZarrFormat:       3,
		NodeType:         "array",
		Shape:            append([]int{}, o.Shape...),
		DataType:         o.DataType,
		ChunkGrid:        Named{Name: "regular", Configuration: gridJSON},
		ChunkKeyEncoding: keyEncoding{sep: o.Separator}.named(),
		FillValue:        formatFill(fill),
	}
	if m.Codecs, err = namedCodecs(o.Codecs); err != nil {
		return nil, err
	}
	if o.DimensionNames != nil {
		for _, name := range o.DimensionNames {
			if name == "" {
				m.DimensionNames = append(m.DimensionNames, nil)
			} else {
				m.DimensionNames = append(m.DimensionNames, &name)
			}
		}
	}
	if m.Attributes, err = withAttributes(nil, o.Attributes); err != nil {
		return nil, err
	}
	a, err := newArray(s, path, m)
	if err != nil {
		return nil, err
	}
	if err := writeMetadata(ctx, s, path, a.meta); err != nil {
		return nil, err
	}
	return a, nil
}

// OpenArray opens the array at path.
func OpenArray(ctx context.Context, s Store, path string) (*Array, error) {
	var m ArrayMetadata
	if err := readMetadata(ctx, s, path, "array", arrayKeys, &m); err != nil {
		return nil, err
	}
	return newArray(s, path, m)
}

func newArray(s Store, path string, m ArrayMetadata) (*Array, error) {
	a := &Array{store: s, path: path, meta: m}
	bad := func(format string, args ...any) error {
		return fmt.Errorf("zarr: array %q: %s", path, fmt.Sprintf(format, args...))
	}
	if m.DataType.Size() == 0 {
		return nil, fmt.Errorf("%w: array %q: data type %q", ErrUnsupported, path, m.DataType)
	}
	if len(m.StorageTransformers) > 0 {
		return nil, fmt.Errorf("%w: array %q: storage transformers", ErrUnsupported, path)
	}
	if m.ChunkGrid.Name != "regular" {
		return nil, fmt.Errorf("%w: array %q: chunk grid %q", ErrUnsupported, path, m.ChunkGrid.Name)
	}
	var grid struct {
		ChunkShape []int `json:"chunk_shape"`
	}
	if err := json.Unmarshal(m.ChunkGrid.Configuration, &grid); err != nil {
		return nil, bad("chunk grid: %v", err)
	}
	a.grid = grid.ChunkShape
	if len(a.grid) != len(m.Shape) {
		return nil, bad("shape %v and chunk shape %v differ in dimensions", m.Shape, a.grid)
	}
	for k := range m.Shape {
		if m.Shape[k] < 0 || a.grid[k] <= 0 {
			return nil, bad("shape %v, chunk shape %v", m.Shape, a.grid)
		}
	}
	if _, ok := elements(m.Shape); !ok {
		return nil, bad("shape %v has more elements than an int can count", m.Shape)
	}
	if m.DimensionNames != nil && len(m.DimensionNames) != len(m.Shape) {
		return nil, bad("%d dimension names for %d dimensions", len(m.DimensionNames), len(m.Shape))
	}
	if _, err := storedBytes(a.grid, m.DataType); err != nil {
		return nil, bad("%v", err)
	}
	var err error
	if a.keys, err = parseKeyEncoding(m.ChunkKeyEncoding); err != nil {
		return nil, err
	}
	if a.fill, err = parseFill(m.DataType, m.FillValue); err != nil {
		return nil, err
	}
	if a.codecs, err = newPipeline(m.Codecs, m.DataType); err != nil {
		return nil, err
	}
	a.chunks, a.perShard = a.grid, slices.Repeat([]int{1}, len(a.grid))
	if sc, ok := a.codecs.array.(*ShardingCodec); ok {
		a.shard, a.chunks = sc, sc.ChunkShape
		if a.perShard, err = sc.perShard(a.grid); err != nil {
			return nil, bad("%v", err)
		}
		if a.indexSize, err = sc.indexSize(a.perShard); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Array) Path() string            { return a.path }
func (a *Array) DataType() DataType      { return a.meta.DataType }
func (a *Array) Shape() []int            { return slices.Clone(a.meta.Shape) }
func (a *Array) FillValue() any          { return a.fill }
func (a *Array) Metadata() ArrayMetadata { return a.meta }
func (a *Array) Attribute(name string, v any) (bool, error) {
	return attribute(a.meta.Attributes, name, v)
}

// ChunkShape is the shape of a chunk: of the chunks within a shard, if the
// array is sharded.
func (a *Array) ChunkShape() []int { return slices.Clone(a.chunks) }

// ShardShape is the shape of a shard, or nil if the array is not sharded.
func (a *Array) ShardShape() []int {
	if a.shard == nil {
		return nil
	}
	return slices.Clone(a.grid)
}

// DimensionNames is the name of each dimension, "" where it has none, or nil
// if the array names none.
func (a *Array) DimensionNames() []string {
	if a.meta.DimensionNames == nil {
		return nil
	}
	names := make([]string, len(a.meta.DimensionNames))
	for i, n := range a.meta.DimensionNames {
		if n != nil {
			names[i] = *n
		}
	}
	return names
}

// NumChunks is how many chunks the array is cut into along each dimension.
func (a *Array) NumChunks() []int {
	n := make([]int, len(a.chunks))
	for k := range n {
		n[k] = a.meta.Shape[k] / a.chunks[k]
		if a.meta.Shape[k]%a.chunks[k] != 0 {
			n[k]++
		}
	}
	return n
}

// SetAttributes lays attrs over the array's attributes and writes its
// metadata again.
func (a *Array) SetAttributes(ctx context.Context, attrs map[string]any) error {
	merged, err := withAttributes(a.meta.Attributes, attrs)
	if err != nil {
		return err
	}
	m := a.meta
	m.Attributes = merged
	if err := writeMetadata(ctx, a.store, a.path, m); err != nil {
		return err
	}
	a.meta = m
	return nil
}

// ChunkKey is the store key the chunk at idx is kept under: its own, or its
// shard's.
func (a *Array) ChunkKey(idx []int) string { return a.storedKey(a.shardOf(idx)) }

func (a *Array) storedKey(sidx []int) string { return join(a.path, a.keys.key(sidx)) }

// shardOf is the index of the stored object the chunk at idx is in.
func (a *Array) shardOf(idx []int) []int {
	s := make([]int, len(idx))
	for k := range idx {
		s[k] = idx[k] / a.perShard[k]
	}
	return s
}

func product(s []int) int {
	n := 1
	for _, v := range s {
		n *= v
	}
	return n
}

func checkType[T Element](a *Array) error {
	if d := dataTypeOf[T](); d != a.meta.DataType {
		return fmt.Errorf("zarr: array %q holds %s, not %s", a.path, a.meta.DataType, d)
	}
	return nil
}

func (a *Array) checkChunk(idx []int) error {
	n := a.NumChunks()
	if len(idx) != len(n) {
		return fmt.Errorf("zarr: chunk %v of an array of %d dimensions", idx, len(n))
	}
	for k := range idx {
		if idx[k] < 0 || idx[k] >= n[k] {
			return fmt.Errorf("zarr: chunk %v is outside %v chunks", idx, n)
		}
	}
	return nil
}

// ReadChunk reads the whole chunk at idx, chunk shape and all, the part past
// the end of the array included. A chunk never written reads as the fill
// value.
func ReadChunk[T Element](ctx context.Context, a *Array, idx []int) ([]T, error) {
	if err := checkType[T](a); err != nil {
		return nil, err
	}
	if err := a.checkChunk(idx); err != nil {
		return nil, err
	}
	sidx := a.shardOf(idx)
	read, err := openStored[T](ctx, a, sidx)
	if err != nil {
		return nil, err
	}
	return read(minus(idx, times(sidx, a.perShard)))
}

// WriteChunk writes the whole chunk at idx: data is chunk shape long. The
// part of it past the end of the array is kept as the fill value, whatever
// data holds there, so that growing the array later reads fill. In a sharded
// array the rest of the chunk's shard is read and written back.
func WriteChunk[T Element](ctx context.Context, a *Array, idx []int, data []T) error {
	if err := checkType[T](a); err != nil {
		return err
	}
	if err := a.checkChunk(idx); err != nil {
		return err
	}
	if len(data) != product(a.chunks) {
		return fmt.Errorf("zarr: chunk of %d elements, not %d", len(data), product(a.chunks))
	}
	if a.shard == nil {
		return writeStored(ctx, a, idx, data, false)
	}
	sidx := a.shardOf(idx)
	buf, err := readStored[T](ctx, a, sidx)
	if err != nil {
		return err
	}
	at := times(minus(idx, times(sidx, a.perShard)), a.chunks)
	copyBlock(buf, a.grid, at, data, a.chunks, make([]int, len(idx)), a.chunks)
	return writeStored(ctx, a, sidx, buf, true)
}

func (a *Array) storedSpec() ChunkSpec {
	return ChunkSpec{Shape: a.grid, DataType: a.meta.DataType, Fill: a.fill, WriteEmptyChunks: a.WriteEmptyChunks}
}

// readStored reads and decodes the whole of the stored object at sidx.
func readStored[T Element](ctx context.Context, a *Array, sidx []int) ([]T, error) {
	key := a.storedKey(sidx)
	b, err := a.store.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return filled(product(a.grid), a.fill.(T)), nil
	}
	if err != nil {
		return nil, err
	}
	v, err := a.codecs.decode(b, a.storedSpec())
	if err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	return asChunk[T](v, product(a.grid), key)
}

func asChunk[T Element](v any, n int, key string) ([]T, error) {
	s, ok := v.([]T)
	if !ok || len(s) != n {
		return nil, fmt.Errorf("zarr: %s decoded to %T of %d, not %d elements", key, v, len(s), n)
	}
	return s, nil
}

// writeStored encodes and writes the whole of the stored object at sidx, or
// deletes it if it is nothing but fill. What of it is past the end of the
// array is written as fill, in data itself if owned and in a copy otherwise:
// no stored object holds anything past the end, which is what lets an array
// grow without reading one.
func writeStored[T Element](ctx context.Context, a *Array, sidx []int, data []T, owned bool) error {
	data = fillPastEnd(a, sidx, data, owned)
	key := a.storedKey(sidx)
	if !a.WriteEmptyChunks && allFill(data, a.fill.(T)) {
		return a.store.Delete(ctx, key)
	}
	b, err := a.codecs.encode(data, a.storedSpec())
	if err != nil {
		return fmt.Errorf("zarr: %s: %w", key, err)
	}
	return a.store.Set(ctx, key, b)
}

// openStored is a reader of the chunks of the stored object at sidx, each by
// its index within that object. An unsharded chunk is read whole when it is
// asked for. A shard is read whole at once, unless the store can read ranges
// and nothing but the sharding codec is between the shard and its bytes:
// then its index is read now and each chunk when it is asked for.
func openStored[T Element](ctx context.Context, a *Array, sidx []int) (func(local []int) ([]T, error), error) {
	sidx = slices.Clone(sidx)
	if a.shard == nil {
		return func([]int) ([]T, error) { return readStored[T](ctx, a, sidx) }, nil
	}
	key, n := a.storedKey(sidx), product(a.chunks)
	spec := a.shard.innerSpec(a.storedSpec())
	nothing := func([]int) ([]T, error) { return filled(n, a.fill.(T)), nil }
	rg, ranged := a.store.(RangeGetter)
	if !ranged || len(a.codecs.bytes) > 0 {
		all, err := readStored[T](ctx, a, sidx)
		if err != nil {
			return nil, err
		}
		return func(local []int) ([]T, error) {
			out := make([]T, n)
			copyBlock(out, a.chunks, make([]int, len(local)), all, a.grid, times(local, a.chunks), a.chunks)
			return out, nil
		}, nil
	}
	off := int64(0)
	if a.shard.location() == IndexEnd {
		off = -int64(a.indexSize)
	}
	ib, err := rg.GetRange(ctx, key, off, int64(a.indexSize))
	if errors.Is(err, ErrNotFound) {
		return nothing, nil
	}
	if err != nil {
		return nil, err
	}
	index, err := a.shard.decodeIndex(a.perShard, ib)
	if err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	lo := uint64(0)
	if a.shard.location() == IndexStart {
		lo = uint64(a.indexSize)
	}
	if err := checkIndex(index, lo, math.MaxInt64); err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	return func(local []int) ([]T, error) {
		k := 0
		for d := range local {
			k = k*a.perShard[d] + local[d]
		}
		o, l, ok, err := entrySpan(index, k, math.MaxInt64)
		if err != nil {
			return nil, fmt.Errorf("zarr: %s: %w", key, err)
		}
		if !ok {
			return nothing(local)
		}
		b, err := rg.GetRange(ctx, key, int64(o), int64(l))
		if err != nil {
			return nil, err
		}
		v, err := a.shard.inner.decode(b, spec)
		if err != nil {
			return nil, fmt.Errorf("zarr: %s chunk %v: %w", key, local, err)
		}
		return asChunk[T](v, n, key)
	}, nil
}

func filled[T Element](n int, fill T) []T {
	s := make([]T, n)
	var zero T
	if fill != zero || fill != fill {
		for i := range s {
			s[i] = fill
		}
	}
	return s
}

// allFill says whether every element is the fill value, NaN matching NaN.
func allFill[T Element](s []T, fill T) bool {
	nan := fill != fill
	for _, v := range s {
		if v != fill && !(nan && v != v) {
			return false
		}
	}
	return true
}

// region checks a region of the array and says where on a grid of cells of
// the shape cell it begins and ends, inclusive; empty is true if it has no
// elements.
func (a *Array) region(start, shape, cell []int) (start2, shape2, lo, hi []int, empty bool, err error) {
	d := len(a.meta.Shape)
	if start == nil {
		start = make([]int, d)
	}
	if shape == nil && len(start) == d {
		shape = make([]int, d)
		for k := range shape {
			shape[k] = a.meta.Shape[k] - min(start[k], a.meta.Shape[k])
		}
	}
	if len(start) != d || len(shape) != d {
		return nil, nil, nil, nil, false, fmt.Errorf("zarr: region at %v of %v in an array of %d dimensions", start, shape, d)
	}
	lo, hi = make([]int, d), make([]int, d)
	for k := range start {
		if start[k] < 0 || shape[k] < 0 || start[k] > a.meta.Shape[k] || shape[k] > a.meta.Shape[k]-start[k] {
			return nil, nil, nil, nil, false, fmt.Errorf("zarr: region at %v of %v is outside %v", start, shape, a.meta.Shape)
		}
		if shape[k] == 0 {
			empty = true
			continue
		}
		lo[k] = start[k] / cell[k]
		hi[k] = (start[k] + shape[k] - 1) / cell[k]
	}
	return start, shape, lo, hi, empty, nil
}

// overlap is where the region at start of shape meets the cell at idx on a
// grid of cells of shape cell: its first element and extent, in the array's
// coordinates, and whether it covers all of the cell that is in the array.
func (a *Array) overlap(idx, start, shape, cell []int) (at, n []int, whole bool) {
	at, n = make([]int, len(idx)), make([]int, len(idx))
	whole = true
	for k := range idx {
		c0 := idx[k] * cell[k]
		c1 := min(c0+cell[k], a.meta.Shape[k])
		at[k] = max(start[k], c0)
		n[k] = min(start[k]+shape[k], c1) - at[k]
		whole = whole && at[k] == c0 && n[k] == c1-c0
	}
	return at, n, whole
}

func minus(a []int, b []int) []int {
	out := make([]int, len(a))
	for k := range a {
		out[k] = a[k] - b[k]
	}
	return out
}

// Read reads the region of the array beginning at start and of shape, in C
// order. A nil start is the origin, and a nil shape the rest of the array.
// The region is made whole in memory: the shape of an array from a store
// that is not trusted is worth a look before all of it is read.
func Read[T Element](ctx context.Context, a *Array, start, shape []int) ([]T, error) {
	if err := checkType[T](a); err != nil {
		return nil, err
	}
	start, shape, lo, hi, empty, err := a.region(start, shape, a.chunks)
	if err != nil {
		return nil, err
	}
	if n, _ := elements(shape); n > math.MaxInt/a.meta.DataType.Size() {
		return nil, fmt.Errorf("zarr: a region of %v is more than can be held in memory", shape)
	}
	out := make([]T, product(shape))
	if empty {
		return out, nil
	}
	err = eachIndex(a.shardOf(lo), a.shardOf(hi), func(sidx []int) error {
		read, err := openStored[T](ctx, a, sidx)
		if err != nil {
			return err
		}
		first := times(sidx, a.perShard)
		ilo, ihi := make([]int, len(lo)), make([]int, len(lo))
		for k := range lo {
			ilo[k] = max(lo[k], first[k])
			ihi[k] = min(hi[k], first[k]+a.perShard[k]-1)
		}
		return eachIndex(ilo, ihi, func(idx []int) error {
			buf, err := read(minus(idx, first))
			if err != nil {
				return err
			}
			at, n, _ := a.overlap(idx, start, shape, a.chunks)
			copyBlock(out, shape, minus(at, start), buf, a.chunks, minus(at, times(idx, a.chunks)), n)
			return nil
		})
	})
	return out, err
}

// Write writes data over the region of the array beginning at start and of
// shape, in C order. A nil start is the origin, and a nil shape the rest of
// the array. A chunk - or a shard, in a sharded array - the region covers
// only part of is read and written back.
func Write[T Element](ctx context.Context, a *Array, start, shape []int, data []T) error {
	if err := checkType[T](a); err != nil {
		return err
	}
	start, shape, lo, hi, empty, err := a.region(start, shape, a.grid)
	if err != nil {
		return err
	}
	if len(data) != product(shape) {
		return fmt.Errorf("zarr: %d elements for a region of %v", len(data), shape)
	}
	if empty {
		return nil
	}
	return eachIndex(lo, hi, func(sidx []int) error {
		at, n, whole := a.overlap(sidx, start, shape, a.grid)
		var buf []T
		if whole {
			buf = filled(product(a.grid), a.fill.(T))
		} else if buf, err = readStored[T](ctx, a, sidx); err != nil {
			return err
		}
		copyBlock(buf, a.grid, minus(at, times(sidx, a.grid)), data, shape, minus(at, start), n)
		return writeStored(ctx, a, sidx, buf, true)
	})
}

// eachIndex calls f with every index from lo to hi inclusive, the last
// dimension fastest. f must not keep idx.
func eachIndex(lo, hi []int, f func(idx []int) error) error {
	idx := append([]int{}, lo...)
	for {
		if err := f(idx); err != nil {
			return err
		}
		k := len(idx) - 1
		for ; k >= 0; k-- {
			if idx[k]++; idx[k] <= hi[k] {
				break
			}
			idx[k] = lo[k]
		}
		if k < 0 {
			return nil
		}
	}
}

// copyBlock copies the block of shape n at srcAt in src, an array of
// srcShape, to dstAt in dst, an array of dstShape.
func copyBlock[T any](dst []T, dstShape, dstAt []int, src []T, srcShape, srcAt, n []int) {
	d := len(n)
	if d == 0 {
		dst[0] = src[0]
		return
	}
	if product(n) == 0 {
		return
	}
	last := d - 1
	pos := make([]int, last)
	for {
		di, si := 0, 0
		dstStride, srcStride := 1, 1
		for k := last; k >= 0; k-- {
			p := 0
			if k < last {
				p = pos[k]
			}
			di += (dstAt[k] + p) * dstStride
			si += (srcAt[k] + p) * srcStride
			dstStride *= dstShape[k]
			srcStride *= srcShape[k]
		}
		copy(dst[di:di+n[last]], src[si:si+n[last]])
		k := last - 1
		for ; k >= 0; k-- {
			if pos[k]++; pos[k] < n[k] {
				break
			}
			pos[k] = 0
		}
		if k < 0 {
			return
		}
	}
}

// keyEncoding is how a chunk's indices become its key.
type keyEncoding struct {
	v2  bool
	sep string
}

func parseKeyEncoding(n Named) (keyEncoding, error) {
	var k keyEncoding
	switch n.Name {
	case "default":
		k.sep = "/"
	case "v2":
		k.v2, k.sep = true, "."
	default:
		return k, fmt.Errorf("%w: chunk key encoding %q", ErrUnsupported, n.Name)
	}
	if len(n.Configuration) > 0 {
		var c struct {
			Separator *string `json:"separator"`
		}
		if err := json.Unmarshal(n.Configuration, &c); err != nil {
			return k, fmt.Errorf("zarr: chunk key encoding: %w", err)
		}
		if c.Separator != nil {
			k.sep = *c.Separator
		}
	}
	if k.sep != "/" && k.sep != "." {
		return k, fmt.Errorf("zarr: chunk key separator %q", k.sep)
	}
	return k, nil
}

func (k keyEncoding) named() Named {
	name := "default"
	if k.v2 {
		name = "v2"
	}
	cfg, _ := json.Marshal(map[string]string{"separator": k.sep})
	return Named{Name: name, Configuration: cfg}
}

func (k keyEncoding) key(idx []int) string {
	parts := make([]string, len(idx))
	for i, v := range idx {
		parts[i] = strconv.Itoa(v)
	}
	if k.v2 {
		if len(parts) == 0 {
			return "0"
		}
		return strings.Join(parts, k.sep)
	}
	return strings.Join(append([]string{"c"}, parts...), k.sep)
}
