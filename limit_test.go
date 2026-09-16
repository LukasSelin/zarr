package zarr

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
)

// arrayJSON is the metadata of an int16 array of shape in chunks of grid,
// through codecs.
func arrayJSON(shape, grid string, codecs string) []byte {
	return []byte(fmt.Sprintf(`{"zarr_format": 3, "node_type": "array", "shape": %s, "data_type": "int16",
  "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": %s}},
  "chunk_key_encoding": {"name": "default"}, "fill_value": 0, "codecs": %s}`, shape, grid, codecs))
}

const littleBytes = `[{"name": "bytes", "configuration": {"endian": "little"}}]`

func TestMetadataThatWouldAllocateWithoutBoundDoesNotOpen(t *testing.T) {
	for _, c := range []struct{ name, shape, grid, codecs string }{
		{"a shape of more elements than an int", fmt.Sprintf("[%d, %d]", math.MaxInt64, 4), "[1, 1]", littleBytes},
		{"a chunk of 0", "[4]", "[0]", littleBytes},
		{"a chunk of more elements than an int", "[4, 4]", fmt.Sprintf("[%d, %d]", 1<<62, 1<<62), littleBytes},
		{"a chunk of more than a chunk may be", "[4, 4]", "[65536, 65536]", littleBytes},
		{"a shard index of more than a chunk may be", "[4]", "[4294967296]",
			`[{"name": "sharding_indexed", "configuration": {"chunk_shape": [1], "codecs": ` + littleBytes + `,
			"index_codecs": [{"name": "bytes", "configuration": {"endian": "little"}}, "crc32c"]}}]`},
		{"a shape near the end of an int", fmt.Sprintf("[%d]", math.MaxInt64), "[1024]", littleBytes},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := NewMemoryStore()
			s.Set(ctx, "zarr.json", arrayJSON(c.shape, c.grid, c.codecs))
			a, err := OpenArray(ctx, s, "")
			if err != nil {
				return
			}
			// What opens must not allocate its way through a read.
			if _, err := Read[int16](ctx, a, nil, nil); err == nil {
				t.Errorf("read an array of %v in chunks of %v", a.Shape(), a.ChunkShape())
			}
		})
	}
}

func TestARegionPastTheEndOfAnIntIsAnError(t *testing.T) {
	s := NewMemoryStore()
	s.Set(ctx, "zarr.json", arrayJSON(fmt.Sprintf("[%d]", math.MaxInt64/2), "[64]", littleBytes))
	a := mustOpen(t, s, "")
	if _, err := Read[int16](ctx, a, []int{math.MaxInt64 / 4}, []int{math.MaxInt64}); err == nil {
		t.Error("read a region whose end overflows")
	}
	if n := a.NumChunks(); n[0] != (math.MaxInt64/2)/64+1 {
		t.Errorf("chunks %v", n)
	}
	if got, err := Read[int16](ctx, a, []int{math.MaxInt64/2 - 3}, []int{3}); err != nil || len(got) != 3 {
		t.Errorf("the last elements read %v: %v", got, err)
	}
}

func TestAChunkMayNotInflatePastItsSize(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{8}, ChunkShape: []int{8}, DataType: Int16,
		Codecs: []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 9}}})
	// Sixteen bytes is the chunk; a megabyte of zeros is a bomb.
	for _, n := range []int{17, 1 << 20} {
		s.Set(ctx, "c/0", must(GzipCodec{Level: 9}.EncodeBytes(make([]byte, n))))
		var got []int16
		if n := allocated(func() { got, _ = Read[int16](ctx, a, nil, nil) }); n > 1<<18 {
			t.Errorf("reading a chunk inflated to %d bytes allocated %d", n, n)
		}
		if _, err := Read[int16](ctx, a, nil, nil); err == nil || !strings.Contains(err.Error(), "inflates") {
			t.Errorf("a chunk of %d bytes read as %v: %v", n, got, err)
		}
	}
	s.Set(ctx, "c/0", must(GzipCodec{Level: 9}.EncodeBytes(make([]byte, 16))))
	if _, err := Read[int16](ctx, a, nil, nil); err != nil {
		t.Errorf("a chunk of its own size: %v", err)
	}

	// Within a shard, and a shard compressed whole, each to its own size.
	b := mustArray(t, s, "b", ArrayOptions{Shape: []int{8}, ChunkShape: []int{4}, DataType: Int16,
		Codecs: []Codec{&ShardingCodec{ChunkShape: []int{2}, Codecs: []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 1}}}, GzipCodec{Level: 1}}})
	if err := Write(ctx, b, nil, nil, []int16{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	shard := must(GzipCodec{}.DecodeBytes(must(s.Get(ctx, "b/c/0"))))
	s.Set(ctx, "b/c/0", must(GzipCodec{Level: 1}.EncodeBytes(append(make([]byte, 1<<20), shard...))))
	if _, err := Read[int16](ctx, b, nil, nil); err == nil || !strings.Contains(err.Error(), "inflates") {
		t.Errorf("a shard inflated past its size read: %v", err)
	}
}

func TestAShardIndexMustPutChunksInTheShardApart(t *testing.T) {
	s := NewMemoryStore()
	c := &ShardingCodec{ChunkShape: []int{2}, IndexCodecs: []Codec{BytesCodec{Endian: Little}}}
	mustArray(t, s, "", ArrayOptions{Shape: []int{4}, ChunkShape: []int{4}, DataType: Int16, Codecs: []Codec{c}})
	body := []byte{1, 0, 2, 0, 3, 0, 4, 0}
	shard := func(index ...uint64) []byte {
		b := bytes.Clone(body)
		for _, v := range index {
			b = binary.LittleEndian.AppendUint64(b, v)
		}
		return b
	}
	for _, store := range []Store{s, plainStore{s}} {
		a := mustOpen(t, store, "")
		s.Set(ctx, "c/0", shard(0, 4, 4, 4))
		if got, err := Read[int16](ctx, a, nil, nil); err != nil || got[3] != 4 {
			t.Errorf("a good shard read %v: %v", got, err)
		}
		for _, index := range [][]uint64{
			{0, 4, 2, 4},                  // overlapping
			{0, 4, 0, 4},                  // the same chunk twice
			{0, 4, 1 << 62, 4},            // past the end
			{math.MaxUint64 - 1, 4, 0, 4}, // past the end of an int64
			{noChunk, 4, 0, 4},            // half missing
		} {
			s.Set(ctx, "c/0", shard(index...))
			if got, err := Read[int16](ctx, a, nil, nil); err == nil {
				t.Errorf("index %v read %v", index, got)
			}
		}
	}
}
