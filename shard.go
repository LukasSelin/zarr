package zarr

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
)

// ShardingCodec keeps many chunks in one stored object, a shard: the chunks
// that are not all fill, each encoded through Codecs and laid end to end,
// and an index of where each one is. The index is an array of an offset and
// a length in bytes for every chunk, in C order, encoded through
// IndexCodecs; a chunk that is not there has both as 2^64-1.
//
// An array's chunk grid is then the grid of shards, and ChunkShape, which
// must divide a shard exactly, is the grid of chunks within one. Reading
// part of a shard from a store that is a RangeGetter reads the index and
// the chunks wanted; writing any of a shard writes all of it.
//
// Use it as a pointer. ArrayOptions.ShardShape makes one for an array.
type ShardingCodec struct {
	ChunkShape []int
	// Codecs is the pipeline each chunk is encoded through. Nil is
	// little-endian bytes, uncompressed.
	Codecs []Codec
	// IndexCodecs is the pipeline the index is encoded through, which must
	// encode every index of a shape to the same length. Nil is little-endian
	// bytes and a crc32c checksum.
	IndexCodecs []Codec
	// IndexLocation is where in a shard the index is: IndexEnd, which ""
	// is, or IndexStart.
	IndexLocation IndexLocation

	inner, index pipeline
	ready        bool
}

// IndexLocation is where a shard keeps its index.
type IndexLocation string

const (
	IndexEnd   IndexLocation = "end"
	IndexStart IndexLocation = "start"
)

// noChunk is a shard index's offset and length for a chunk it does not hold.
const noChunk = math.MaxUint64

func parseSharding(cfg json.RawMessage, d DataType) (Codec, error) {
	var j struct {
		ChunkShape    []int         `json:"chunk_shape"`
		Codecs        []Named       `json:"codecs"`
		IndexCodecs   []Named       `json:"index_codecs"`
		IndexLocation IndexLocation `json:"index_location"`
	}
	if err := json.Unmarshal(cfg, &j); err != nil {
		return nil, fmt.Errorf("zarr: sharding_indexed codec: %w", err)
	}
	if j.ChunkShape == nil || j.Codecs == nil || j.IndexCodecs == nil {
		return nil, fmt.Errorf("zarr: sharding_indexed codec needs chunk_shape, codecs and index_codecs: %s", cfg)
	}
	c := &ShardingCodec{ChunkShape: j.ChunkShape, IndexLocation: j.IndexLocation}
	var err error
	if c.inner, err = newPipeline(j.Codecs, d); err != nil {
		return nil, err
	}
	if c.index, err = newPipeline(j.IndexCodecs, Uint64); err != nil {
		return nil, err
	}
	c.Codecs, c.IndexCodecs = c.inner.codecs(), c.index.codecs()
	if err := c.prepare(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *ShardingCodec) codecLists() (inner, index []Codec) {
	inner, index = c.Codecs, c.IndexCodecs
	if inner == nil {
		inner = []Codec{BytesCodec{Endian: Little}}
	}
	if index == nil {
		index = []Codec{BytesCodec{Endian: Little}, CRC32CCodec{}}
	}
	return inner, index
}

func (c *ShardingCodec) location() IndexLocation {
	if c.IndexLocation == "" {
		return IndexEnd
	}
	return c.IndexLocation
}

// prepare makes the pipelines of a codec built rather than parsed.
func (c *ShardingCodec) prepare() error {
	if c.ready {
		return nil
	}
	if l := c.location(); l != IndexEnd && l != IndexStart {
		return fmt.Errorf("zarr: sharding_indexed codec: no such index location %q", l)
	}
	for _, v := range c.ChunkShape {
		if v <= 0 {
			return fmt.Errorf("zarr: sharding_indexed codec: chunk shape %v", c.ChunkShape)
		}
	}
	inner, index := c.codecLists()
	var err error
	if c.inner, err = pipelineOf(inner); err != nil {
		return err
	}
	if c.index, err = pipelineOf(index); err != nil {
		return err
	}
	// The index is found by its length, so it must not depend on what is in
	// it: two indices as unlike as can be must encode to the same length.
	empty, err := c.encodeIndex([]int{1, 1, 1}, slices.Repeat([]uint64{noChunk}, 6))
	if err != nil {
		return err
	}
	full, err := c.encodeIndex([]int{1, 1, 1}, []uint64{0, 1, 2, 3, 4, 5})
	if err != nil {
		return err
	}
	if len(empty) != len(full) {
		return fmt.Errorf("zarr: sharding_indexed codec: index codecs must encode to a fixed length")
	}
	c.ready = true
	return nil
}

func (*ShardingCodec) Name() string { return "sharding_indexed" }

func (c *ShardingCodec) Configuration() any {
	inner, index := c.codecLists()
	in, err := namedCodecs(inner)
	if err != nil {
		return failing{err}
	}
	ix, err := namedCodecs(index)
	if err != nil {
		return failing{err}
	}
	return map[string]any{
		"chunk_shape":    c.ChunkShape,
		"codecs":         in,
		"index_codecs":   ix,
		"index_location": c.location(),
	}
}

// failing is a configuration that fails to marshal with its error.
type failing struct{ err error }

func (f failing) MarshalJSON() ([]byte, error) { return nil, f.err }

// perShard is how many chunks a shard of shape holds along each dimension.
func (c *ShardingCodec) perShard(shape []int) ([]int, error) {
	if len(shape) != len(c.ChunkShape) {
		return nil, fmt.Errorf("zarr: shards of %v cannot hold chunks of %v", shape, c.ChunkShape)
	}
	n := make([]int, len(shape))
	for k := range shape {
		if c.ChunkShape[k] <= 0 || shape[k]%c.ChunkShape[k] != 0 {
			return nil, fmt.Errorf("zarr: shards of %v are not a whole number of chunks of %v", shape, c.ChunkShape)
		}
		n[k] = shape[k] / c.ChunkShape[k]
	}
	return n, nil
}

func (c *ShardingCodec) indexSpec(perShard []int) ChunkSpec {
	return ChunkSpec{Shape: append(slices.Clone(perShard), 2), DataType: Uint64, Fill: uint64(noChunk)}
}

func (c *ShardingCodec) encodeIndex(perShard []int, index []uint64) ([]byte, error) {
	return c.index.encode(index, c.indexSpec(perShard))
}

func (c *ShardingCodec) decodeIndex(perShard []int, b []byte) ([]uint64, error) {
	v, err := c.index.decode(b, c.indexSpec(perShard))
	if err != nil {
		return nil, fmt.Errorf("zarr: shard index: %w", err)
	}
	return v.([]uint64), nil
}

// indexSize is how many bytes the index of a shard holding perShard chunks is.
func (c *ShardingCodec) indexSize(perShard []int) (int, error) {
	b, err := c.encodeIndex(perShard, slices.Repeat([]uint64{noChunk}, 2*product(perShard)))
	return len(b), err
}

func (c *ShardingCodec) innerSpec(spec ChunkSpec) ChunkSpec {
	spec.Shape = c.ChunkShape
	return spec
}

func (c *ShardingCodec) EncodeArray(chunk any, spec ChunkSpec) ([]byte, error) {
	if err := c.prepare(); err != nil {
		return nil, err
	}
	perShard, err := c.perShard(spec.Shape)
	if err != nil {
		return nil, err
	}
	shard, err := chunkOpsOf(chunk)
	if err != nil {
		return nil, err
	}
	index := slices.Repeat([]uint64{noChunk}, 2*product(perShard))
	var body []byte
	k := 0
	err = eachIndex(make([]int, len(perShard)), minusOne(perShard), func(local []int) error {
		defer func() { k++ }()
		sub := shard.extract(spec.Shape, times(local, c.ChunkShape), c.ChunkShape)
		if !spec.WriteEmptyChunks && sub.allFill(spec.Fill) {
			return nil
		}
		b, err := c.inner.encode(sub.slice(), c.innerSpec(spec))
		if err != nil {
			return err
		}
		index[2*k], index[2*k+1] = uint64(len(body)), uint64(len(b))
		body = append(body, b...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if c.location() == IndexEnd {
		ix, err := c.encodeIndex(perShard, index)
		return append(body, ix...), err
	}
	size, err := c.indexSize(perShard)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(index); i += 2 {
		if index[i] != noChunk {
			index[i] += uint64(size)
		}
	}
	ix, err := c.encodeIndex(perShard, index)
	return append(ix, body...), err
}

// indexOf is the bytes of a whole shard that are its index.
func (c *ShardingCodec) indexOf(data []byte, size int) ([]byte, error) {
	if len(data) < size {
		return nil, fmt.Errorf("zarr: shard of %d bytes has no index of %d", len(data), size)
	}
	if c.location() == IndexStart {
		return data[:size], nil
	}
	return data[len(data)-size:], nil
}

func (c *ShardingCodec) DecodeArray(data []byte, spec ChunkSpec) (any, error) {
	if err := c.prepare(); err != nil {
		return nil, err
	}
	perShard, err := c.perShard(spec.Shape)
	if err != nil {
		return nil, err
	}
	size, err := c.indexSize(perShard)
	if err != nil {
		return nil, err
	}
	ib, err := c.indexOf(data, size)
	if err != nil {
		return nil, err
	}
	index, err := c.decodeIndex(perShard, ib)
	if err != nil {
		return nil, err
	}
	out := filledAny(spec.DataType, product(spec.Shape), spec.Fill)
	shard, err := chunkOpsOf(out)
	if err != nil {
		return nil, err
	}
	k := 0
	err = eachIndex(make([]int, len(perShard)), minusOne(perShard), func(local []int) error {
		defer func() { k++ }()
		b, ok, err := entry(index, k, data)
		if err != nil || !ok {
			return err
		}
		sub, err := c.inner.decode(b, c.innerSpec(spec))
		if err != nil {
			return err
		}
		return shard.insert(spec.Shape, times(local, c.ChunkShape), sub, c.ChunkShape)
	})
	return out, err
}

// entry is the bytes of the k-th chunk of a shard, from the shard's index and
// the shard itself, and whether the shard holds that chunk at all.
func entry(index []uint64, k int, shard []byte) ([]byte, bool, error) {
	off, n, ok, err := entrySpan(index, k, uint64(len(shard)))
	if err != nil || !ok {
		return nil, ok, err
	}
	return shard[off : off+n], true, nil
}

// entrySpan is where the k-th chunk is in a shard of size bytes.
func entrySpan(index []uint64, k int, size uint64) (off, n uint64, ok bool, err error) {
	off, n = index[2*k], index[2*k+1]
	if off == noChunk && n == noChunk {
		return 0, 0, false, nil
	}
	if off > size || n > size-off {
		return 0, 0, false, fmt.Errorf("zarr: shard index puts chunk %d at %d+%d, past %d bytes", k, off, n, size)
	}
	return off, n, true, nil
}

func minusOne(s []int) []int {
	out := make([]int, len(s))
	for k := range s {
		out[k] = s[k] - 1
	}
	return out
}

func times(a, b []int) []int {
	out := make([]int, len(a))
	for k := range a {
		out[k] = a[k] * b[k]
	}
	return out
}

// chunkOps is what a codec that holds several chunks does to a chunk it
// knows only as an any.
type chunkOps interface {
	slice() any
	// extract is a new chunk of the block of shape n at at in this one, an
	// array of shape.
	extract(shape, at, n []int) chunkOps
	// insert copies sub, of shape n, into this chunk, of shape, at at.
	insert(shape, at []int, sub any, n []int) error
	allFill(fill any) bool
}

type typedChunk[T Element] []T

func (s typedChunk[T]) slice() any { return []T(s) }

func (s typedChunk[T]) extract(shape, at, n []int) chunkOps {
	out := make([]T, product(n))
	copyBlock(out, n, make([]int, len(n)), []T(s), shape, at, n)
	return typedChunk[T](out)
}

func (s typedChunk[T]) insert(shape, at []int, sub any, n []int) error {
	t, ok := sub.([]T)
	if !ok || len(t) != product(n) {
		return fmt.Errorf("zarr: chunk decoded to %T of %d, not %d elements", sub, len(t), product(n))
	}
	copyBlock([]T(s), shape, at, t, n, make([]int, len(n)), n)
	return nil
}

func (s typedChunk[T]) allFill(fill any) bool { return allFill([]T(s), fill.(T)) }

func chunkOpsOf(chunk any) (chunkOps, error) {
	switch s := chunk.(type) {
	case []bool:
		return typedChunk[bool](s), nil
	case []int8:
		return typedChunk[int8](s), nil
	case []int16:
		return typedChunk[int16](s), nil
	case []int32:
		return typedChunk[int32](s), nil
	case []int64:
		return typedChunk[int64](s), nil
	case []uint8:
		return typedChunk[uint8](s), nil
	case []uint16:
		return typedChunk[uint16](s), nil
	case []uint32:
		return typedChunk[uint32](s), nil
	case []uint64:
		return typedChunk[uint64](s), nil
	case []float32:
		return typedChunk[float32](s), nil
	case []float64:
		return typedChunk[float64](s), nil
	}
	return nil, fmt.Errorf("zarr: a chunk cannot be a %T", chunk)
}

// filledAny is n elements of the fill value, as a []T in an any.
func filledAny(d DataType, n int, fill any) any {
	switch f := fill.(type) {
	case bool:
		return filled(n, f)
	case int8:
		return filled(n, f)
	case int16:
		return filled(n, f)
	case int32:
		return filled(n, f)
	case int64:
		return filled(n, f)
	case uint8:
		return filled(n, f)
	case uint16:
		return filled(n, f)
	case uint32:
		return filled(n, f)
	case uint64:
		return filled(n, f)
	case float32:
		return filled(n, f)
	case float64:
		return filled(n, f)
	}
	return makeSlice(d, n)
}
