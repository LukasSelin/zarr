package zarr

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// The fuzz tests feed what a store could hold to everything that decodes
// it: metadata, each codec, a shard and its index, and an array opened and
// read from a store of fuzzed keys. None may panic, hang, or allocate more
// than the metadata implies. Their corpus is seeded from the other tests and
// from every case in testdata/interop, and what they have found is kept in
// testdata/fuzz, which go test runs.
//
//	go test -run '^$' -fuzz '^FuzzOpenAndRead$' -fuzztime 10m .

// smallChunks lowers the most a chunk may be for the rest of a fuzz test, so
// that what the limit lets through is quick and what it does not is plain.
func smallChunks(f *testing.F) {
	old := maxStoredBytes
	maxStoredBytes = 1 << 16
	f.Cleanup(func() { maxStoredBytes = old })
}

// allocLimit is the most a fuzz input may make the package allocate, beyond
// a multiple of its own size: chunks of 1<<16 bytes are well within it, and
// allocation without bound well past.
const allocLimit = 256 << 20

func allocated(f func()) uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	before := m.TotalAlloc
	f()
	runtime.ReadMemStats(&m)
	return m.TotalAlloc - before
}

func checkAllocated(t *testing.T, inputBytes int, f func()) {
	t.Helper()
	if n := allocated(f); n > allocLimit+64*uint64(inputBytes) {
		t.Fatalf("allocated %d bytes for %d bytes of input", n, inputBytes)
	}
}

// seedStore is a store holding every interop case as this package writes
// it, and a few arrays more.
func seedStore(f *testing.F) *MemoryStore {
	s := NewMemoryStore()
	if _, err := CreateGroup(ctx, s, "", map[string]any{"seed": uint64(1<<64 - 1)}); err != nil {
		f.Fatal(err)
	}
	for _, c := range interopCases(f) {
		dispatch(f, c, func() { writeInterop[bool](f, s, c) }, func() { writeInterop[int8](f, s, c) },
			func() { writeInterop[int16](f, s, c) }, func() { writeInterop[int32](f, s, c) }, func() { writeInterop[int64](f, s, c) },
			func() { writeInterop[uint8](f, s, c) }, func() { writeInterop[uint16](f, s, c) }, func() { writeInterop[uint32](f, s, c) },
			func() { writeInterop[uint64](f, s, c) }, func() { writeInterop[float32](f, s, c) }, func() { writeInterop[float64](f, s, c) })
	}
	for name, o := range map[string]ArrayOptions{
		"x/gzip_shards": {Shape: []int{9, 7}, ChunkShape: []int{2, 3}, ShardShape: []int{4, 6}, DataType: Float64, FillValue: 0.5,
			Codecs: []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 1}}},
		"x/crc_start": {Shape: []int{10}, ChunkShape: []int{2}, DataType: Uint16,
			Codecs: []Codec{&ShardingCodec{ChunkShape: []int{2}, Codecs: []Codec{BytesCodec{Endian: Big}, CRC32CCodec{}}, IndexLocation: IndexStart}}},
		"x/v2": {Shape: []int{5, 5}, ChunkShape: []int{2, 2}, DataType: Int32, Separator: ".",
			Codecs: []Codec{BytesCodec{Endian: Little}, CRC32CCodec{}, GzipCodec{Level: 9}}},
	} {
		if _, err := OpenGroup(ctx, s, "x"); err != nil {
			if _, err := CreateGroup(ctx, s, "x", nil); err != nil {
				f.Fatal(err)
			}
		}
		a, err := CreateArray(ctx, s, name, o)
		if err != nil {
			f.Fatal(err)
		}
		data := make([]float64, product(o.Shape))
		for i := range data {
			data[i] = float64(i%7) * 1.5
		}
		if err := writeAs(a, data); err != nil {
			f.Fatal(err)
		}
	}
	return s
}

// writeAs writes data to the whole array, converted to its data type.
func writeAs(a *Array, data []float64) error {
	switch a.DataType() {
	case Float64:
		return Write(ctx, a, nil, nil, data)
	case Uint16:
		return Write(ctx, a, nil, nil, convert[uint16](data))
	case Int32:
		return Write(ctx, a, nil, nil, convert[int32](data))
	}
	return errors.New("no such type here")
}

func convert[T int32 | uint16](data []float64) []T {
	out := make([]T, len(data))
	for i, v := range data {
		out[i] = T(v)
	}
	return out
}

// arrays is every array in a store: its path, and the keys under it that are
// its own, relative to it.
func arrays(s *MemoryStore) map[string][]string {
	out := map[string][]string{}
	var paths []string
	for _, k := range s.Keys() {
		if path, ok := strings.CutSuffix(k, "zarr.json"); ok {
			b, _ := s.Get(ctx, k)
			if bytes.Contains(b, []byte(`"array"`)) {
				path = strings.TrimSuffix(path, "/")
				paths = append(paths, path)
				out[path] = nil
			}
		}
	}
	for _, k := range s.Keys() {
		for _, p := range paths {
			if rel, ok := strings.CutPrefix(k, p+"/"); ok && rel != "zarr.json" && !strings.Contains(rel, "zarr.json") {
				out[p] = append(out[p], rel)
			}
		}
	}
	return out
}

func FuzzMetadata(f *testing.F) {
	smallChunks(f)
	s := seedStore(f)
	for _, k := range s.Keys() {
		if strings.HasSuffix(k, "zarr.json") {
			f.Add(must(s.Get(ctx, k)))
		}
	}
	f.Add([]byte(`{"zarr_format": 3, "node_type": "array", "shape": [10000, 1000], "dimension_names": ["rows", null],
  "data_type": "float64", "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [1000, 100]}},
  "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
  "codecs": [{"name": "bytes", "configuration": {"endian": "big"}}, {"name": "gzip", "configuration": {"level": 1}}, "crc32c"],
  "fill_value": "NaN", "attributes": {"foo": 42}, "future": {"must_understand": false, "anything": 1}}`))
	f.Fuzz(func(t *testing.T, meta []byte) {
		s := NewMemoryStore()
		s.Set(ctx, "zarr.json", meta)
		checkAllocated(t, len(meta), func() {
			if g, err := OpenGroup(ctx, s, ""); err == nil {
				var v any
				g.Attribute("seed", &v)
			}
			a, err := OpenArray(ctx, s, "")
			if err != nil {
				return
			}
			// What opened writes as metadata that opens as the same array.
			again := NewMemoryStore()
			if err := writeMetadata(ctx, again, "", a.Metadata()); err != nil {
				t.Fatalf("metadata that opened does not write: %v", err)
			}
			b, err := OpenArray(ctx, again, "")
			if err != nil {
				t.Fatalf("metadata that opened does not open once written: %v\n%s", err, must(again.Get(ctx, "zarr.json")))
			}
			one, _ := json.Marshal(a.Metadata())
			two, _ := json.Marshal(b.Metadata())
			if !bytes.Equal(one, two) {
				t.Fatalf("metadata changed when written:\n%s\n%s", one, two)
			}
			a.NumChunks()
			// A store holding no chunks reads as the fill value.
			readSome(t, a, 0)
		})
	})
}

// readSome reads a few elements of the array from a place at picks, as its
// data type, and a chunk, and fails only on a panic, which it lets through.
func readSome(t *testing.T, a *Array, at uint64) {
	shape := a.Shape()
	start, n := make([]int, len(shape)), make([]int, len(shape))
	total := 1
	for k := range shape {
		if shape[k] == 0 {
			return
		}
		start[k] = int(at % uint64(shape[k]))
		at /= uint64(shape[k])
		n[k] = min(shape[k]-start[k], 2)
		if total*n[k] > 16 {
			n[k] = 1
		}
		total *= n[k]
	}
	chunk := make([]int, len(shape))
	for k := range chunk {
		chunk[k] = start[k] / a.chunks[k]
	}
	switch a.DataType() {
	case Bool:
		readTyped[bool](a, start, n, chunk)
	case Int8:
		readTyped[int8](a, start, n, chunk)
	case Int16:
		readTyped[int16](a, start, n, chunk)
	case Int32:
		readTyped[int32](a, start, n, chunk)
	case Int64:
		readTyped[int64](a, start, n, chunk)
	case Uint8:
		readTyped[uint8](a, start, n, chunk)
	case Uint16:
		readTyped[uint16](a, start, n, chunk)
	case Uint32:
		readTyped[uint32](a, start, n, chunk)
	case Uint64:
		readTyped[uint64](a, start, n, chunk)
	case Float32:
		readTyped[float32](a, start, n, chunk)
	case Float64:
		readTyped[float64](a, start, n, chunk)
	default:
		t.Fatalf("opened an array of %q", a.DataType())
	}
}

func readTyped[T Element](a *Array, start, n, chunk []int) {
	if v, err := Read[T](ctx, a, start, n); err == nil && len(v) != product(n) {
		panic("read the wrong number of elements")
	}
	if v, err := ReadChunk[T](ctx, a, chunk); err == nil && len(v) != product(a.chunks) {
		panic("read a chunk of the wrong number of elements")
	}
}

func FuzzOpenAndRead(f *testing.F) {
	smallChunks(f)
	s := seedStore(f)
	for path, keys := range arrays(s) {
		meta := must(s.Get(ctx, metadataKey(path)))
		for i, k := range keys {
			next := keys[(i+1)%len(keys)]
			f.Add(meta, k, must(s.Get(ctx, join(path, k))), next, must(s.Get(ctx, join(path, next))), uint64(i*7))
		}
		if len(keys) == 0 {
			f.Add(meta, "c/0", []byte{}, "", []byte{}, uint64(0))
		}
	}
	f.Fuzz(func(t *testing.T, meta []byte, k1 string, v1 []byte, k2 string, v2 []byte, at uint64) {
		s := NewMemoryStore()
		s.Set(ctx, "zarr.json", meta)
		s.Set(ctx, k1, v1)
		s.Set(ctx, k2, v2)
		checkAllocated(t, len(meta)+len(v1)+len(v2), func() {
			a, err := OpenArray(ctx, s, "")
			if err != nil {
				return
			}
			readSome(t, a, at)
			// And again with each shard read whole.
			if a, err = OpenArray(ctx, plainStore{s}, ""); err == nil {
				readSome(t, a, at)
			}
		})
	})
}

var fuzzTypes = []DataType{Bool, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64, Float32, Float64}

func FuzzBytesCodec(f *testing.F) {
	smallChunks(f)
	s := seedStore(f)
	for path, keys := range arrays(s) {
		a := must(OpenArray(ctx, s, path))
		if len(a.codecs.bytes) > 0 || a.shard != nil || len(keys) == 0 {
			continue
		}
		c := a.codecs.array.(BytesCodec)
		f.Add(must(s.Get(ctx, join(path, keys[0]))), uint8(slices.Index(fuzzTypes, a.DataType())), c.Endian == Big, uint16(len(a.grid)), uint32(product(a.grid)))
	}
	f.Fuzz(func(t *testing.T, data []byte, dtype uint8, big bool, dims uint16, n uint32) {
		d := fuzzTypes[int(dtype)%len(fuzzTypes)]
		c := BytesCodec{Endian: Little}
		if big {
			c.Endian = Big
		}
		// A shape of dims dimensions, all 1 but the first.
		shape := slices.Repeat([]int{1}, int(dims%64))
		if len(shape) > 0 {
			shape[0] = int(n)
		}
		spec := ChunkSpec{Shape: shape, DataType: d}
		checkAllocated(t, len(data), func() {
			v, err := c.DecodeArray(data, spec)
			if err != nil {
				return
			}
			back, err := c.EncodeArray(v, spec)
			if err != nil {
				t.Fatal(err)
			}
			if d != Bool && !bytes.Equal(back, data) {
				t.Fatalf("decoded and encoded again to\n%x\nnot\n%x", back, data)
			}
		})
	})
}

func FuzzGzipCodec(f *testing.F) {
	smallChunks(f)
	for _, level := range []int{0, 1, 5, 9} {
		f.Add(must(GzipCodec{Level: level}.EncodeBytes(bytes.Repeat([]byte("zarr"), 1000))), uint32(4000))
		f.Add(must(GzipCodec{Level: level}.EncodeBytes(nil)), uint32(0))
	}
	// A bomb: a megabyte of zeros in a thousand bytes.
	f.Add(must(GzipCodec{Level: 9}.EncodeBytes(make([]byte, 1<<20))), uint32(1<<16))
	f.Fuzz(func(t *testing.T, data []byte, limit uint32) {
		checkAllocated(t, len(data), func() {
			l := int64(limit % (1 << 17))
			out, err := GzipCodec{}.DecodeBytesLimit(data, l)
			if err == nil && int64(len(out)) > l {
				t.Fatalf("inflated to %d bytes, past a limit of %d", len(out), l)
			}
			if out, err := (GzipCodec{}).DecodeBytes(data); err == nil && int64(len(out)) > maxStoredBytes {
				t.Fatalf("inflated to %d bytes, past a chunk", len(out))
			}
		})
	})
}

func FuzzCRC32CCodec(f *testing.F) {
	f.Add(must(CRC32CCodec{}.EncodeBytes([]byte("zarr"))))
	f.Add(must(CRC32CCodec{}.EncodeBytes(nil)))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		body, err := CRC32CCodec{}.DecodeBytes(data)
		if err != nil {
			return
		}
		if back := must(CRC32CCodec{}.EncodeBytes(body)); !bytes.Equal(back, data) {
			t.Fatalf("checked and encoded again to %x, not %x", back, data)
		}
	})
}

// fuzzShardings are the sharding codecs FuzzShard decodes with, all of int16
// shards of 4 by 6 in chunks of 2 by 3.
var fuzzShardings = []func() *ShardingCodec{
	func() *ShardingCodec { return &ShardingCodec{ChunkShape: []int{2, 3}} },
	func() *ShardingCodec {
		return &ShardingCodec{ChunkShape: []int{2, 3}, IndexLocation: IndexStart,
			Codecs: []Codec{BytesCodec{Endian: Big}, GzipCodec{Level: 1}}}
	},
	func() *ShardingCodec {
		return &ShardingCodec{ChunkShape: []int{2, 3}, Codecs: []Codec{BytesCodec{Endian: Little}, CRC32CCodec{}},
			IndexCodecs: []Codec{BytesCodec{Endian: Big}}}
	},
}

func FuzzShard(f *testing.F) {
	smallChunks(f)
	spec := ChunkSpec{Shape: []int{4, 6}, DataType: Int16, Fill: int16(-1)}
	for i, make := range fuzzShardings {
		c := make()
		for _, data := range [][]int16{
			{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23},
			slices.Repeat([]int16{-1}, 24),
			append(slices.Repeat([]int16{-1}, 20), 1, 2, 3, 4),
		} {
			f.Add(must(c.EncodeArray(data, spec)), uint8(i))
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, which uint8) {
		c := fuzzShardings[int(which)%len(fuzzShardings)]()
		checkAllocated(t, len(data), func() {
			v, err := c.DecodeArray(data, spec)
			if err != nil {
				return
			}
			// What decodes encodes to a shard that decodes the same.
			back, err := c.EncodeArray(v, spec)
			if err != nil {
				t.Fatal(err)
			}
			again, err := c.DecodeArray(back, spec)
			if err != nil || !slices.Equal(again.([]int16), v.([]int16)) {
				t.Fatalf("decoded %v, then %v: %v", v, again, err)
			}
		})
	})
}

func FuzzShardIndex(f *testing.F) {
	f.Add(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, 0), 8), uint64(0), uint64(8))
	f.Add([]byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"), uint64(0), uint64(100))
	f.Add([]byte("\x10\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"), uint64(16), uint64(100))
	f.Fuzz(func(t *testing.T, raw []byte, lo, hi uint64) {
		index := make([]uint64, len(raw)/16*2)
		for i := range index {
			index[i] = binary.LittleEndian.Uint64(raw[8*i:])
		}
		err := checkIndex(index, lo, hi)
		// The same, one pair at a time.
		want := true
		for k := 0; k < len(index)/2; k++ {
			off, n := index[2*k], index[2*k+1]
			if off == noChunk && n == noChunk {
				continue
			}
			if off < lo || off > hi || n > hi-off {
				want = false
			}
			for j := 0; j < k; j++ {
				o, m := index[2*j], index[2*j+1]
				if o == noChunk && m == noChunk || o > hi || m > hi-o {
					continue
				}
				if off < o+m && o < off+n {
					want = false
				}
			}
		}
		if (err == nil) != want {
			t.Fatalf("index %v within %d to %d: %v", index, lo, hi, err)
		}
	})
}
