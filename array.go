package zarr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Array is an array in a store.
type Array struct {
	store  Store
	path   string
	meta   ArrayMetadata
	chunks []int
	keys   keyEncoding
	codecs pipeline
	fill   any
	// WriteEmptyChunks keeps a chunk that holds nothing but the fill value.
	// By default such a chunk is deleted instead; it reads as the fill value
	// either way.
	WriteEmptyChunks bool
}

// ArrayOptions is what an array is created with.
type ArrayOptions struct {
	Shape      []int
	ChunkShape []int
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
	grid, _ := json.Marshal(map[string][]int{"chunk_shape": o.ChunkShape})
	m := ArrayMetadata{
		ZarrFormat:       3,
		NodeType:         "array",
		Shape:            append([]int{}, o.Shape...),
		DataType:         o.DataType,
		ChunkGrid:        Named{Name: "regular", Configuration: grid},
		ChunkKeyEncoding: keyEncoding{sep: o.Separator}.named(),
		FillValue:        formatFill(fill),
	}
	for _, c := range o.Codecs {
		n, err := namedCodec(c)
		if err != nil {
			return nil, err
		}
		m.Codecs = append(m.Codecs, n)
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
	a.chunks = grid.ChunkShape
	if len(a.chunks) != len(m.Shape) {
		return nil, bad("shape %v and chunk shape %v differ in dimensions", m.Shape, a.chunks)
	}
	for k := range m.Shape {
		if m.Shape[k] < 0 || a.chunks[k] <= 0 {
			return nil, bad("shape %v, chunk shape %v", m.Shape, a.chunks)
		}
	}
	if m.DimensionNames != nil && len(m.DimensionNames) != len(m.Shape) {
		return nil, bad("%d dimension names for %d dimensions", len(m.DimensionNames), len(m.Shape))
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
	return a, nil
}

func (a *Array) Path() string            { return a.path }
func (a *Array) DataType() DataType      { return a.meta.DataType }
func (a *Array) Shape() []int            { return append([]int{}, a.meta.Shape...) }
func (a *Array) ChunkShape() []int       { return append([]int{}, a.chunks...) }
func (a *Array) FillValue() any          { return a.fill }
func (a *Array) Metadata() ArrayMetadata { return a.meta }
func (a *Array) Attribute(name string, v any) (bool, error) {
	return attribute(a.meta.Attributes, name, v)
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
		n[k] = (a.meta.Shape[k] + a.chunks[k] - 1) / a.chunks[k]
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

// ChunkKey is the store key of the chunk at idx.
func (a *Array) ChunkKey(idx []int) string { return join(a.path, a.keys.key(idx)) }

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
	return readChunk[T](ctx, a, idx)
}

func readChunk[T Element](ctx context.Context, a *Array, idx []int) ([]T, error) {
	n := product(a.chunks)
	b, err := a.store.Get(ctx, a.ChunkKey(idx))
	if errors.Is(err, ErrNotFound) {
		return filled(n, a.fill.(T)), nil
	}
	if err != nil {
		return nil, err
	}
	v, err := a.codecs.decode(b, a.meta.DataType, n)
	if err != nil {
		return nil, fmt.Errorf("zarr: chunk %s: %w", a.ChunkKey(idx), err)
	}
	s, ok := v.([]T)
	if !ok || len(s) != n {
		return nil, fmt.Errorf("zarr: chunk %s decoded to %T of %d", a.ChunkKey(idx), v, len(s))
	}
	return s, nil
}

// WriteChunk writes the whole chunk at idx: data is chunk shape long.
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
	return writeChunk(ctx, a, idx, data)
}

func writeChunk[T Element](ctx context.Context, a *Array, idx []int, data []T) error {
	key := a.ChunkKey(idx)
	if !a.WriteEmptyChunks && allFill(data, a.fill.(T)) {
		return a.store.Delete(ctx, key)
	}
	b, err := a.codecs.encode(data, a.meta.DataType)
	if err != nil {
		return fmt.Errorf("zarr: chunk %s: %w", key, err)
	}
	return a.store.Set(ctx, key, b)
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

// region checks a region of the array and says where its chunks begin and
// end, inclusive; empty is true if it has no elements.
func (a *Array) region(start, shape []int) (start2, shape2, lo, hi []int, empty bool, err error) {
	d := len(a.meta.Shape)
	if start == nil {
		start = make([]int, d)
	}
	if shape == nil {
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
		if start[k] < 0 || shape[k] < 0 || start[k]+shape[k] > a.meta.Shape[k] {
			return nil, nil, nil, nil, false, fmt.Errorf("zarr: region at %v of %v is outside %v", start, shape, a.meta.Shape)
		}
		if shape[k] == 0 {
			empty = true
			continue
		}
		lo[k] = start[k] / a.chunks[k]
		hi[k] = (start[k] + shape[k] - 1) / a.chunks[k]
	}
	return start, shape, lo, hi, empty, nil
}

// overlap is where the region at start of shape meets the chunk at idx: its
// first element and extent, in the array's coordinates.
func (a *Array) overlap(idx, start, shape []int) (at, n []int, whole bool) {
	at, n = make([]int, len(idx)), make([]int, len(idx))
	whole = true
	for k := range idx {
		c0 := idx[k] * a.chunks[k]
		c1 := min(c0+a.chunks[k], a.meta.Shape[k])
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

func (a *Array) chunkOrigin(idx []int) []int {
	o := make([]int, len(idx))
	for k := range idx {
		o[k] = idx[k] * a.chunks[k]
	}
	return o
}

// Read reads the region of the array beginning at start and of shape, in C
// order. A nil start is the origin, and a nil shape the rest of the array.
func Read[T Element](ctx context.Context, a *Array, start, shape []int) ([]T, error) {
	if err := checkType[T](a); err != nil {
		return nil, err
	}
	start, shape, lo, hi, empty, err := a.region(start, shape)
	if err != nil {
		return nil, err
	}
	out := make([]T, product(shape))
	if empty {
		return out, nil
	}
	err = eachIndex(lo, hi, func(idx []int) error {
		buf, err := readChunk[T](ctx, a, idx)
		if err != nil {
			return err
		}
		at, n, _ := a.overlap(idx, start, shape)
		copyBlock(out, shape, minus(at, start), buf, a.chunks, minus(at, a.chunkOrigin(idx)), n)
		return nil
	})
	return out, err
}

// Write writes data over the region of the array beginning at start and of
// shape, in C order. A nil start is the origin, and a nil shape the rest of
// the array. A chunk the region covers only part of is read and written
// back.
func Write[T Element](ctx context.Context, a *Array, start, shape []int, data []T) error {
	if err := checkType[T](a); err != nil {
		return err
	}
	start, shape, lo, hi, empty, err := a.region(start, shape)
	if err != nil {
		return err
	}
	if len(data) != product(shape) {
		return fmt.Errorf("zarr: %d elements for a region of %v", len(data), shape)
	}
	if empty {
		return nil
	}
	return eachIndex(lo, hi, func(idx []int) error {
		at, n, whole := a.overlap(idx, start, shape)
		var buf []T
		if whole {
			buf = filled(product(a.chunks), a.fill.(T))
		} else if buf, err = readChunk[T](ctx, a, idx); err != nil {
			return err
		}
		copyBlock(buf, a.chunks, minus(at, a.chunkOrigin(idx)), data, shape, minus(at, start), n)
		return writeChunk(ctx, a, idx, buf)
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
