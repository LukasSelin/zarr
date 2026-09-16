package zarr

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

var ctx = context.Background()

func mustArray(t *testing.T, s Store, path string, o ArrayOptions) *Array {
	t.Helper()
	a, err := CreateArray(ctx, s, path, o)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// naive is the region of a C-order array of shape read one element at a time.
func naive(all []int32, shape, start, n []int) []int32 {
	var out []int32
	eachIndex(make([]int, len(n)), minus(n, []int{1, 1, 1}), func(p []int) error {
		i := 0
		for k := range p {
			i = i*shape[k] + start[k] + p[k]
		}
		out = append(out, all[i])
		return nil
	})
	return out
}

func TestRegionsReadBackWhatWasWritten(t *testing.T) {
	s := NewMemoryStore()
	shape := []int{7, 10, 3}
	a := mustArray(t, s, "", ArrayOptions{Shape: shape, ChunkShape: []int{3, 4, 2}, DataType: Int32, FillValue: -1})
	all := make([]int32, product(shape))
	for i := range all {
		all[i] = int32(i)
	}
	if err := Write(ctx, a, nil, nil, all); err != nil {
		t.Fatal(err)
	}
	got, err := Read[int32](ctx, a, nil, nil)
	if err != nil || !slices.Equal(got, all) {
		t.Fatalf("whole array read back wrong: %v", err)
	}
	for _, r := range [][2][]int{
		{{0, 0, 0}, {1, 1, 1}},
		{{2, 3, 1}, {4, 5, 2}},
		{{6, 9, 2}, {1, 1, 1}},
		{{1, 0, 0}, {6, 10, 3}},
		{{3, 4, 0}, {3, 4, 2}},
	} {
		got, err := Read[int32](ctx, a, r[0], r[1])
		if err != nil {
			t.Fatal(err)
		}
		if want := naive(all, shape, r[0], r[1]); !slices.Equal(got, want) {
			t.Errorf("region at %v of %v: got %v, want %v", r[0], r[1], got, want)
		}
	}

	// A region written over part of a fresh array leaves the rest as fill.
	b := mustArray(t, s, "b", ArrayOptions{Shape: shape, ChunkShape: []int{3, 4, 2}, DataType: Int32, FillValue: -1})
	start, n := []int{2, 3, 1}, []int{4, 5, 2}
	patch := naive(all, shape, start, n)
	if err := Write(ctx, b, start, n, patch); err != nil {
		t.Fatal(err)
	}
	got, err = Read[int32](ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Repeat([]int32{-1}, len(all))
	eachIndex([]int{0, 0, 0}, minus(n, []int{1, 1, 1}), func(p []int) error {
		i := ((start[0]+p[0])*shape[1]+start[1]+p[1])*shape[2] + start[2] + p[2]
		want[i] = all[i]
		return nil
	})
	if !slices.Equal(got, want) {
		t.Errorf("patched array:\n got %v\nwant %v", got, want)
	}
}

func TestAChunkNeverWrittenReadsAsTheFill(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "h", ArrayOptions{Shape: []int{5, 5}, ChunkShape: []int{2, 2}, DataType: Float32, FillValue: math.NaN()})
	if string(a.Metadata().FillValue) != `"NaN"` {
		t.Errorf("fill_value is %s", a.Metadata().FillValue)
	}
	got, err := Read[float32](ctx, a, []int{1, 1}, []int{3, 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range got {
		if v == v {
			t.Fatalf("read %v, not NaN", got)
		}
	}
}

func TestChunksOfNothingButFillAreNotKept(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "a", ArrayOptions{Shape: []int{4}, ChunkShape: []int{2}, DataType: Uint8})
	if err := Write(ctx, a, nil, nil, []uint8{0, 0, 0, 7}); err != nil {
		t.Fatal(err)
	}
	if keys := s.Keys(); !slices.Equal(keys, []string{"a/c/1", "a/zarr.json"}) {
		t.Errorf("keys %v", keys)
	}
	a.WriteEmptyChunks = true
	if err := Write(ctx, a, nil, nil, []uint8{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if keys := s.Keys(); !slices.Equal(keys, []string{"a/c/0", "a/c/1", "a/zarr.json"}) {
		t.Errorf("keys %v", keys)
	}
}

func TestChunkBytesAndKeys(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "big", ArrayOptions{Shape: []int{2}, ChunkShape: []int{2}, DataType: Int16, Codecs: []Codec{BytesCodec{Endian: Big}}})
	if err := Write(ctx, a, nil, nil, []int16{1, -2}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, "big/c/0"); !bytes.Equal(got, []byte{0, 1, 0xff, 0xfe}) {
		t.Errorf("big-endian chunk is % x", got)
	}

	dot := mustArray(t, s, "dot", ArrayOptions{Shape: []int{4, 4}, ChunkShape: []int{2, 2}, DataType: Uint8, Separator: "."})
	if got := dot.ChunkKey([]int{1, 0}); got != "dot/c.1.0" {
		t.Errorf("dot key %q", got)
	}

	m := a.Metadata()
	m.ChunkKeyEncoding = Named{Name: "v2"}
	v2, err := newArray(s, "v2", m)
	if err != nil {
		t.Fatal(err)
	}
	if got := v2.ChunkKey([]int{3}); got != "v2/3" {
		t.Errorf("v2 key %q", got)
	}
}

func TestGzipAndChecksum(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{
		Shape: []int{100, 100}, ChunkShape: []int{64, 64}, DataType: Float64,
		Codecs: []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 5}, CRC32CCodec{}},
	})
	data := make([]float64, 100*100)
	for i := range data {
		data[i] = math.Sin(float64(i) / 50)
	}
	if err := Write(ctx, a, nil, nil, data); err != nil {
		t.Fatal(err)
	}
	b, err := OpenArray(ctx, s, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Read[float64](ctx, b, nil, nil); err != nil || !slices.Equal(got, data) {
		t.Fatalf("read back wrong: %v", err)
	}
	raw, _ := s.Get(ctx, "c/1/1")
	raw[len(raw)/2] ^= 0xff
	s.Set(ctx, "c/1/1", raw)
	if _, err := Read[float64](ctx, b, nil, nil); err == nil {
		t.Error("a corrupted chunk read without error")
	}
}

func TestOpeningMetadataWrittenElsewhere(t *testing.T) {
	meta := `{
  "zarr_format": 3,
  "node_type": "array",
  "shape": [10000, 1000],
  "dimension_names": ["rows", null],
  "data_type": "float64",
  "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [1000, 100]}},
  "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
  "codecs": [{"name": "bytes", "configuration": {"endian": "big"}}, {"name": "gzip", "configuration": {"level": 1}}, "crc32c"],
  "fill_value": "NaN",
  "attributes": {"foo": 42, "bar": "apples"},
  "future": {"must_understand": false, "anything": 1}
}`
	s := NewMemoryStore()
	s.Set(ctx, "a/zarr.json", []byte(meta))
	a, err := OpenArray(ctx, s, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got := a.DimensionNames(); !slices.Equal(got, []string{"rows", ""}) {
		t.Errorf("dimension names %q", got)
	}
	if got := a.NumChunks(); !slices.Equal(got, []int{10, 10}) {
		t.Errorf("chunks %v", got)
	}
	var foo int
	if ok, err := a.Attribute("foo", &foo); !ok || err != nil || foo != 42 {
		t.Errorf("attribute foo: %v %v %v", foo, ok, err)
	}

	s.Set(ctx, "b/zarr.json", bytes.Replace([]byte(meta), []byte(`"must_understand": false, `), nil, 1))
	if _, err := OpenArray(ctx, s, "b"); !errors.Is(err, ErrUnsupported) {
		t.Errorf("a field that must be understood opened: %v", err)
	}
	s.Set(ctx, "c/zarr.json", bytes.Replace([]byte(meta), []byte(`"gzip"`), []byte(`"zstd"`), 1))
	if _, err := OpenArray(ctx, s, "c"); !errors.Is(err, ErrUnsupported) {
		t.Errorf("an unknown codec opened: %v", err)
	}
	if _, err := OpenGroup(ctx, s, "a"); err == nil {
		t.Error("an array opened as a group")
	}
}

func TestFillValues(t *testing.T) {
	for _, c := range []struct {
		d    DataType
		raw  string
		want any
	}{
		{Float64, `"Infinity"`, math.Inf(1)},
		{Float64, `"-Infinity"`, math.Inf(-1)},
		{Float64, `"0x7ff0000000000000"`, math.Inf(1)},
		{Float32, `"0x3f800000"`, float32(1)},
		{Float32, `0.5`, float32(0.5)},
		{Uint64, `18446744073709551615`, uint64(math.MaxUint64)},
		{Int8, `-128`, int8(-128)},
		{Bool, `true`, true},
	} {
		got, err := parseFill(c.d, json.RawMessage(c.raw))
		if err != nil || got != c.want {
			t.Errorf("%s %s: got %v (%T) %v, want %v", c.d, c.raw, got, got, err, c.want)
		}
		if back, _ := parseFill(c.d, formatFill(got)); back != got {
			t.Errorf("%s %s did not survive formatting: %s", c.d, c.raw, formatFill(got))
		}
	}
	for _, c := range []struct {
		d   DataType
		raw string
	}{{Int8, `128`}, {Uint8, `-1`}, {Int32, `1.5`}, {Float32, `"0x1"`}, {Bool, `0`}, {Int16, `null`}} {
		if _, err := parseFill(c.d, json.RawMessage(c.raw)); err == nil {
			t.Errorf("%s %s parsed", c.d, c.raw)
		}
	}
	for _, c := range []struct {
		d  DataType
		v  any
		ok bool
	}{{Uint8, 255, true}, {Uint8, 256, false}, {Int16, -1.0, true}, {Int16, 0.5, false}, {Float32, 3, true}, {Bool, 1, false}, {Uint64, uint64(math.MaxUint64), true}} {
		if _, err := fillFrom(c.d, c.v); (err == nil) != c.ok {
			t.Errorf("fill %v (%T) as %s: %v", c.v, c.v, c.d, err)
		}
	}
}

func TestADirectoryStoreLaysOutTheHierarchy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "world.zarr")
	s := NewDirStore(dir)
	root, err := CreateGroup(ctx, s, "", map[string]any{"seed": uint64(math.MaxUint64)})
	if err != nil {
		t.Fatal(err)
	}
	h, err := root.CreateArray(ctx, "height", ArrayOptions{Shape: []int{3, 3}, ChunkShape: []int{2, 2}, DataType: Float32, DimensionNames: []string{"y", "x"}})
	if err != nil {
		t.Fatal(err)
	}
	data := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	if err := Write(ctx, h, nil, nil, data); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"zarr.json", "height/zarr.json", "height/c/0/0", "height/c/1/1"} {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(f))); err != nil {
			t.Error(err)
		}
	}
	if _, err := root.CreateArray(ctx, "height", ArrayOptions{Shape: []int{1}, ChunkShape: []int{1}, DataType: Uint8}); !errors.Is(err, ErrExists) {
		t.Errorf("created over an array: %v", err)
	}

	again, err := OpenGroup(ctx, NewDirStore(dir), "")
	if err != nil {
		t.Fatal(err)
	}
	var seed uint64
	if _, err := again.Attribute("seed", &seed); err != nil || seed != math.MaxUint64 {
		t.Errorf("seed %d: %v", seed, err)
	}
	h2, err := again.OpenArray(ctx, "height")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Read[float32](ctx, h2, nil, nil); err != nil || !slices.Equal(got, data) {
		t.Errorf("read %v: %v", got, err)
	}
	if _, err := s.Get(ctx, "../outside"); err == nil {
		t.Error("a key reached outside the store")
	}
	if _, err := OpenArray(ctx, s, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a missing array: %v", err)
	}
}

func TestAScalar(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "x", ArrayOptions{DataType: Float64, Shape: []int{}, ChunkShape: []int{}})
	if err := Write(ctx, a, nil, nil, []float64{2.5}); err != nil {
		t.Fatal(err)
	}
	if keys := s.Keys(); !slices.Equal(keys, []string{"x/c", "x/zarr.json"}) {
		t.Errorf("keys %v", keys)
	}
	if got, err := Read[float64](ctx, a, nil, nil); err != nil || !slices.Equal(got, []float64{2.5}) {
		t.Errorf("read %v: %v", got, err)
	}
}

func TestTheElementTypeMustBeTheArrays(t *testing.T) {
	a := mustArray(t, NewMemoryStore(), "", ArrayOptions{Shape: []int{2}, ChunkShape: []int{2}, DataType: Int32})
	if _, err := Read[float64](ctx, a, nil, nil); err == nil {
		t.Error("read int32 as float64")
	}
	if err := Write(ctx, a, []int{1}, []int{2}, []int32{1, 2}); err == nil {
		t.Error("wrote past the end")
	}
}

func TestAKeptGzipWriterWritesWhatANewOneDoes(t *testing.T) {
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i * i >> 7)
	}
	for level := gzip.HuffmanOnly; level <= gzip.BestCompression; level++ {
		var fresh bytes.Buffer
		w := must(gzip.NewWriterLevel(&fresh, level))
		w.Write(data)
		w.Close()
		for range 3 {
			got := must(GzipCodec{Level: level}.EncodeBytes(data))
			if !bytes.Equal(got, fresh.Bytes()) {
				t.Fatalf("level %d: a kept writer wrote other bytes", level)
			}
			if back := must(GzipCodec{}.DecodeBytes(got)); !bytes.Equal(back, data) {
				t.Fatalf("level %d: read back wrong", level)
			}
		}
	}
	if _, err := (GzipCodec{Level: 10}).EncodeBytes(data); err == nil {
		t.Error("wrote gzip at level 10")
	}
}
