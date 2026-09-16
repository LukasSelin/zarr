package zarr

import (
	"fmt"
	"math"
)

// maxStoredBytes is the most bytes the elements of one stored object - a
// chunk, or a whole shard - may take, and the most a shard's index may. It
// is what stops metadata from a store making the package allocate without
// bound: a chunk is made whole in memory when it is read, and a chunk that
// was never written is made of the fill value. Everything else a store can
// make the package allocate is held to what the metadata implies: a
// compressed chunk may not inflate past the bytes its elements take.
//
// It is a variable so that the fuzz tests can lower it.
var maxStoredBytes int64 = 1 << 31

// elements is the product of shape, and whether it is a count of elements at
// all: no dimension negative, and the product within an int.
func elements(shape []int) (int, bool) {
	n := 1
	for _, v := range shape {
		if v < 0 || (v > 0 && n > math.MaxInt/v) {
			return 0, false
		}
		n *= v
	}
	return n, true
}

// storedBytes is how many bytes the elements of a stored object of shape
// take, or an error if that is more than a stored object may.
func storedBytes(shape []int, d DataType) (int64, error) {
	n, ok := elements(shape)
	if !ok || d.Size() == 0 || int64(n) > maxStoredBytes/int64(d.Size()) {
		return 0, fmt.Errorf("zarr: a chunk of %v %s is more than the %d bytes a chunk may be", shape, d, maxStoredBytes)
	}
	return int64(n) * int64(d.Size()), nil
}

// unbounded is a bound on encoded bytes that cannot be said.
const unbounded = -1

// encodedBound is the most bytes the array-to-bytes codec can encode a chunk
// of spec to, or unbounded for a codec this package does not know.
func encodedBound(c ArrayBytesCodec, spec ChunkSpec) int64 {
	switch c := c.(type) {
	case BytesCodec:
		n, err := storedBytes(spec.Shape, spec.DataType)
		if err != nil {
			return unbounded
		}
		return n
	case *ShardingCodec:
		if c.prepare() != nil {
			return unbounded
		}
		perShard, err := c.perShard(spec.Shape)
		if err != nil {
			return unbounded
		}
		m, ok := elements(perShard)
		if !ok || int64(m) > maxStoredBytes/16 {
			return unbounded
		}
		index := c.index.bound(c.indexSpec(perShard))
		chunk := c.inner.bound(c.innerSpec(spec))
		if index == unbounded || chunk == unbounded || (m > 0 && chunk > (math.MaxInt64-index)/int64(m)) {
			return unbounded
		}
		return index + int64(m)*chunk
	}
	return unbounded
}

// bytesBound is the most bytes a bytes-to-bytes codec can encode n bytes to,
// or unbounded for a codec this package does not know.
func bytesBound(c BytesBytesCodec, n int64) int64 {
	switch c.(type) {
	case CRC32CCodec:
		return n + 4
	case GzipCodec:
		// Deflate's stored blocks add 5 bytes to every 65 535, and a gzip
		// header and trailer 18 and whatever names and comments a writer puts
		// in. This is well above both.
		return n + n/100 + 1024
	}
	return unbounded
}

// bound is the most bytes the pipeline can encode a chunk of spec to, or
// unbounded.
func (p pipeline) bound(spec ChunkSpec) int64 {
	n := encodedBound(p.array, spec)
	for _, c := range p.bytes {
		if n == unbounded || n > math.MaxInt64/2 {
			return unbounded
		}
		n = bytesBound(c, n)
	}
	return n
}
