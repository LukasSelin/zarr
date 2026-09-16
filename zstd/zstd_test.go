package zstd

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/LukasSelin/zarr"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func allocated(f func()) uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	before := m.TotalAlloc
	f()
	runtime.ReadMemStats(&m)
	return m.TotalAlloc - before
}

// frame is a zstd frame of blocks run-length blocks of size bytes of v
// each, with no content size in its header and a window of 1<<windowLog:
// what a store could hold to make a reader allocate.
func frame(windowLog, blocks, size int, v byte) []byte {
	b := []byte{0x28, 0xb5, 0x2f, 0xfd, 0, byte(windowLog-10) << 3}
	for i := range blocks {
		h := uint32(size)<<3 | 1<<1 // run-length
		if i == blocks-1 {
			h |= 1
		}
		b = append(b, byte(h), byte(h>>8), byte(h>>16), v)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{0, 1, 1000, 1 << 17, 300_000} {
		random := make([]byte, n)
		for i := range random {
			random[i] = byte(r.Uint32())
		}
		repeated := bytes.Repeat([]byte("zarr"), n/4)
		for _, level := range []int{MinLevel, -5, 0, 1, 3, 7, 22} {
			for _, crc := range []bool{false, true} {
				c := Codec{Level: level, Checksum: crc}
				for _, data := range [][]byte{random, repeated} {
					enc := must(c.EncodeBytes(data))
					if int64(len(enc)) > c.EncodedBound(int64(len(data))) {
						t.Errorf("%+v: %d bytes encoded to %d, past the bound of %d", c, len(data), len(enc), c.EncodedBound(int64(len(data))))
					}
					dec, err := c.DecodeBytesLimit(enc, int64(len(data)))
					if err != nil || !bytes.Equal(dec, data) {
						t.Fatalf("%+v: %d bytes decoded to %d: %v", c, len(data), len(dec), err)
					}
					if len(data) > 0 {
						if _, err := c.DecodeBytesLimit(enc, int64(len(data)-1)); err == nil {
							t.Errorf("%+v: %d bytes decoded within a limit of one fewer", c, len(data))
						}
					}
				}
			}
		}
	}
}

func TestChecksum(t *testing.T) {
	enc := must(Codec{Checksum: true}.EncodeBytes(bytes.Repeat([]byte("zarr"), 100)))
	enc[len(enc)-1] ^= 1
	if _, err := (Codec{}).DecodeBytes(enc); err == nil {
		t.Fatal("a frame with a wrong checksum decoded")
	}
}

func TestParse(t *testing.T) {
	for _, cfg := range []string{``, `{}`, `{"level": 3}`, `{"checksum": false}`, `{"level": 23, "checksum": false}`,
		`{"level": -131073, "checksum": false}`, `{"level": "3", "checksum": false}`} {
		meta := `{"zarr_format": 3, "node_type": "array", "shape": [4], "data_type": "int32",
			"chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [2]}},
			"chunk_key_encoding": {"name": "default"}, "fill_value": 0,
			"codecs": [{"name": "bytes", "configuration": {"endian": "little"}}, {"name": "zstd"` + map[bool]string{true: "", false: `, "configuration": ` + cfg}[cfg == ""] + `}]}`
		s := zarr.NewMemoryStore()
		s.Set(ctx, "a/zarr.json", []byte(meta))
		if _, err := zarr.OpenArray(ctx, s, "a"); err == nil {
			t.Errorf("opened with configuration %q", cfg)
		}
	}
}

func TestArray(t *testing.T) {
	data := make([]float64, 9*7)
	for i := range data {
		data[i] = float64(i%11) * 0.5
	}
	for name, codecs := range map[string][]zarr.Codec{
		"chunks": {zarr.BytesCodec{Endian: zarr.Little}, Codec{Level: 3, Checksum: true}},
		"within shards": {&zarr.ShardingCodec{ChunkShape: []int{2, 3},
			Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Big}, Codec{Level: 1}}}},
		"after shards": {&zarr.ShardingCodec{ChunkShape: []int{2, 3}}, Codec{Level: 19}},
		"with crc32c":  {zarr.BytesCodec{Endian: zarr.Little}, Codec{}, zarr.CRC32CCodec{}},
	} {
		t.Run(name, func(t *testing.T) {
			s := zarr.NewMemoryStore()
			a, err := zarr.CreateArray(ctx, s, "a", zarr.ArrayOptions{Shape: []int{9, 7}, ChunkShape: []int{4, 6},
				DataType: zarr.Float64, FillValue: 0.5, Codecs: codecs})
			if err != nil {
				t.Fatal(err)
			}
			if err := zarr.Write(ctx, a, nil, nil, data); err != nil {
				t.Fatal(err)
			}
			meta, _ := s.Get(ctx, "a/zarr.json")
			if !strings.Contains(string(meta), `"name": "zstd"`) {
				t.Errorf("metadata has no zstd:\n%s", meta)
			}
			b, err := zarr.OpenArray(ctx, s, "a")
			if err != nil {
				t.Fatal(err)
			}
			if got, want := must(json.Marshal(b.Metadata().Codecs)), must(json.Marshal(a.Metadata().Codecs)); !bytes.Equal(got, want) {
				t.Errorf("codecs opened as %s, not %s", got, want)
			}
			got, err := zarr.Read[float64](ctx, b, nil, nil)
			if err != nil || !slices.Equal(got, data) {
				t.Fatalf("read %v, %v", got, err)
			}
		})
	}
}

func TestBomb(t *testing.T) {
	for name, data := range map[string][]byte{
		// Some 128 megabytes of zeros in 4 kilobytes, no size said.
		"blocks":     frame(17, 1000, 1<<17, 0),
		"big window": frame(31, 1000, 1<<17, 0),
		"encoded":    must(Codec{Level: 22}.EncodeBytes(make([]byte, 64<<20))),
	} {
		t.Run(name, func(t *testing.T) {
			var err error
			n := allocated(func() { _, err = Codec{}.DecodeBytesLimit(data, 1<<16) })
			if err == nil {
				t.Fatal("decoded past the limit")
			}
			if n > 16<<20 {
				t.Errorf("allocated %d bytes to find %v", n, err)
			}
		})
	}
	// Within the limit, a frame of no stated size decodes.
	small := frame(17, 3, 1000, 7)
	if out, err := (Codec{}).DecodeBytesLimit(small, 3000); err != nil || !bytes.Equal(out, bytes.Repeat([]byte{7}, 3000)) {
		t.Fatalf("decoded to %d bytes: %v", len(out), err)
	}
}

func TestArrayBomb(t *testing.T) {
	s := zarr.NewMemoryStore()
	a, err := zarr.CreateArray(ctx, s, "a", zarr.ArrayOptions{Shape: []int{8}, ChunkShape: []int{8}, DataType: zarr.Int32,
		Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, Codec{}}})
	if err != nil {
		t.Fatal(err)
	}
	s.Set(ctx, a.ChunkKey([]int{0}), frame(20, 1000, 1<<17, 1))
	n := allocated(func() { _, err = zarr.Read[int32](ctx, a, nil, nil) })
	if err == nil {
		t.Fatal("read a chunk of 32 bytes that decompresses to 128 megabytes")
	}
	if n > 16<<20 {
		t.Errorf("allocated %d bytes to find %v", n, err)
	}
}

func FuzzDecode(f *testing.F) {
	for _, level := range []int{-5, 0, 3, 22} {
		f.Add(must(Codec{Level: level, Checksum: true}.EncodeBytes(bytes.Repeat([]byte("zarr"), 1000))), uint32(4000))
		f.Add(must(Codec{Level: level}.EncodeBytes(nil)), uint32(0))
	}
	f.Add(frame(17, 1000, 1<<17, 0), uint32(1<<16))
	f.Add(frame(31, 2, 10, 0), uint32(20))
	f.Fuzz(func(t *testing.T, data []byte, limit uint32) {
		l := int64(limit % (1 << 17))
		var out []byte
		var err error
		n := allocated(func() { out, err = Codec{}.DecodeBytesLimit(data, l) })
		if err == nil && int64(len(out)) > l {
			t.Fatalf("decoded to %d bytes, past a limit of %d", len(out), l)
		}
		if n > 64<<20+64*uint64(len(data)) {
			t.Fatalf("allocated %d bytes for %d bytes of input", n, len(data))
		}
	})
}
