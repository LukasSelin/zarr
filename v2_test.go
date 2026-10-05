package zarr

import (
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// testdata/v2/store.zarr is what zarr-python 3.4.0 writes with
// zarr_format=2: a group with attributes, an array in it, and a group in it
// holding another array.
//
// testdata/v2/hand.zarr is written by hand: the metadata as text, and each
// chunk's bytes with encoding/binary, compress/zlib, compress/gzip and a
// shuffle of its own, element i of each array in C order being
// interopValue(i). It has every data type, in both byte orders where there
// are two, zlib, gzip, shuffle before zlib, Fortran order, both separators,
// a scalar, NaN and infinite fills, a null fill, a chunk missing from each
// array, and groups in groups with attributes. zarr-python 3.4.0 reads it as
// this test does.

// v2Store is a copy in memory of the store testdata/v2/name.
func v2Store(t *testing.T, name string) *MemoryStore {
	t.Helper()
	dir := NewDirStore(filepath.Join("testdata", "v2", name))
	s := NewMemoryStore()
	err := dir.List(ctx, "", func(key string) error {
		b, err := dir.Get(ctx, key)
		if err != nil {
			return err
		}
		return s.Set(ctx, key, b)
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestZarrV2IsRead(t *testing.T) {
	s := v2Store(t, "store.zarr")
	root := must(OpenGroup(ctx, s, ""))
	var title string
	if _, err := root.Attribute("title", &title); err != nil || !strings.Contains(title, "zarr_format=2") {
		t.Errorf("title %q: %v", title, err)
	}
	if root.Metadata().ZarrFormat != 2 {
		t.Errorf("zarr_format %d", root.Metadata().ZarrFormat)
	}
	temp := must(root.OpenArray(ctx, "temperature"))
	got := must(Read[float32](ctx, temp, nil, nil))
	for i, v := range got {
		if v != float32(i) {
			t.Fatalf("temperature %v", got)
		}
	}
	var units string
	if _, err := temp.Attribute("units", &units); err != nil || units != "K" {
		t.Errorf("units %q: %v", units, err)
	}
	rain := must(OpenArray(ctx, s, "forecast/rain"))
	if got := must(Read[int16](ctx, rain, nil, nil)); !slices.Equal(got, []int16{1, 2, 3}) {
		t.Errorf("rain %v", got)
	}
}

func TestZarrV2ReadsEveryArrayOfTheHandWrittenStore(t *testing.T) {
	s := v2Store(t, "hand.zarr")
	types := map[string]DataType{
		"b1": Bool, "i1": Int8, "u1": Uint8, "i2": Int16, "i4": Int32, "i8": Int64,
		"u2": Uint16, "u4": Uint32, "u8": Uint64, "f4": Float32, "f8": Float64,
	}
	var seen []string
	for _, g := range []string{"types", "codecs", "nested/deeper"} {
		group := must(OpenGroup(ctx, s, g))
		arrays := must(group.OpenArrays(ctx))
		for _, a := range arrays {
			seen = append(seen, a.Path())
			name := a.Path()[strings.LastIndex(a.Path(), "/")+1:]
			if d, ok := types[name[:2]]; ok && g == "types" && a.DataType() != d {
				t.Errorf("%s: %s", a.Path(), a.DataType())
			}
			c := interopCase{Name: a.Path(), DataType: a.DataType()}
			dispatch(t, c, func() { checkV2[bool](t, s, a) }, func() { checkV2[int8](t, s, a) },
				func() { checkV2[int16](t, s, a) }, func() { checkV2[int32](t, s, a) }, func() { checkV2[int64](t, s, a) },
				func() { checkV2[uint8](t, s, a) }, func() { checkV2[uint16](t, s, a) }, func() { checkV2[uint32](t, s, a) },
				func() { checkV2[uint64](t, s, a) }, func() { checkV2[float32](t, s, a) }, func() { checkV2[float64](t, s, a) })
		}
	}
	if len(seen) != 25 {
		t.Errorf("%d arrays: %v", len(seen), seen)
	}
	for path, fill := range map[string]any{
		"codecs/zlib":         math.NaN(),
		"codecs/gzip":         int32(-7),
		"codecs/shuffle_zlib": float32(math.Inf(-1)),
		"codecs/fortran":      uint16(0),
		"nested/deeper/grid":  float32(math.Inf(1)),
		"types/b1":            true,
		"types/f8_big":        -1.5,
		"types/u8_little":     uint64(3),
	} {
		got := must(OpenArray(ctx, s, path)).FillValue()
		if f, ok := fill.(float64); ok && math.IsNaN(f) {
			if g, ok := got.(float64); ok && math.IsNaN(g) {
				continue
			}
		}
		if got != fill {
			t.Errorf("%s: fill %v (%T), not %v", path, got, got, fill)
		}
	}
}

// checkV2 reads the whole array and each chunk, which must be interopValue
// of each element but in the chunks the store does not hold, which must be
// the fill value. Every array but a scalar has one such chunk.
func checkV2[T Element](t *testing.T, s Store, a *Array) {
	t.Helper()
	got, err := Read[T](ctx, a, nil, nil)
	if err != nil {
		t.Errorf("%s: %v", a.Path(), err)
		return
	}
	shape, fill := a.Shape(), a.FillValue().(T)
	want := make([]T, product(shape))
	missing := 0
	hi := minus(a.NumChunks(), slices.Repeat([]int{1}, len(shape)))
	eachIndex(make([]int, len(shape)), hi, func(idx []int) error {
		_, err := s.Get(ctx, a.ChunkKey(idx))
		stored := err == nil
		if !stored {
			missing++
		}
		at, n, _ := a.overlap(idx, make([]int, len(shape)), shape, a.chunks)
		eachIndex(at, minus(plus(at, n), slices.Repeat([]int{1}, len(n))), func(p []int) error {
			i := 0
			for k := range p {
				i = i*shape[k] + p[k]
			}
			want[i] = fill
			if stored {
				want[i] = interopValue[T](i)
			}
			return nil
		})
		chunk, err := ReadChunk[T](ctx, a, idx)
		if err != nil || len(chunk) != product(a.ChunkShape()) {
			t.Errorf("%s chunk %v: %d elements, %v", a.Path(), idx, len(chunk), err)
		}
		return nil
	})
	if want1 := min(len(shape), 1); missing != want1 {
		t.Errorf("%s: %d chunks missing, not %d", a.Path(), missing, want1)
	}
	if !sameData(got, want) {
		t.Errorf("%s:\n got %v\nwant %v", a.Path(), got, want)
	}
}

func TestZarrV2GroupsAttributesAndDimensionNames(t *testing.T) {
	s := v2Store(t, "hand.zarr")
	root := must(OpenGroup(ctx, s, ""))
	var seed uint64
	if _, err := root.Attribute("seed", &seed); err != nil || seed != math.MaxUint64 {
		t.Errorf("seed %d: %v", seed, err)
	}
	children := must(root.Children(ctx))
	if want := []Child{{"codecs", "group"}, {"nested", "group"}, {"types", "group"}}; !slices.Equal(children, want) {
		t.Errorf("children %v", children)
	}
	nested := must(root.OpenGroup(ctx, "nested"))
	children = must(nested.Children(ctx))
	if want := []Child{{"deeper", "group"}}; !slices.Equal(children, want) {
		t.Errorf("children of nested %v", children)
	}
	deeper := must(nested.OpenGroup(ctx, "deeper"))
	var level int
	if _, err := deeper.Attribute("level", &level); err != nil || level != 2 {
		t.Errorf("level %d: %v", level, err)
	}
	if children := must(deeper.Children(ctx)); !slices.Equal(children, []Child{{"grid", "array"}}) {
		t.Errorf("children of deeper %v", children)
	}
	grid := must(deeper.OpenArray(ctx, "grid"))
	if names := grid.DimensionNames(); !slices.Equal(names, []string{"y", "x"}) {
		t.Errorf("dimension names %q", names)
	}
	var units string
	var dims []string
	if _, err := grid.Attribute("units", &units); err != nil || units != "m" {
		t.Errorf("units %q: %v", units, err)
	}
	if _, err := grid.Attribute("_ARRAY_DIMENSIONS", &dims); err != nil || len(dims) != 2 {
		t.Errorf("the attribute is not kept: %v %v", dims, err)
	}
	if names := must(OpenArray(ctx, s, "codecs/zlib")).DimensionNames(); names != nil {
		t.Errorf("dimension names %q of an array with none", names)
	}
	// Dimension names that do not name every dimension are none.
	s.Set(ctx, "nested/deeper/grid/.zattrs", []byte(`{"_ARRAY_DIMENSIONS": ["y"]}`))
	if names := must(OpenArray(ctx, s, "nested/deeper/grid")).DimensionNames(); names != nil {
		t.Errorf("dimension names %q", names)
	}
}

func TestZarrV2MetadataIsVersion3s(t *testing.T) {
	s := v2Store(t, "hand.zarr")
	for _, path := range []string{"codecs/zlib", "codecs/gzip", "codecs/shuffle_zlib", "codecs/scalar", "types/i8_big", "nested/deeper/grid"} {
		a := must(OpenArray(ctx, s, path))
		m := a.Metadata()
		if m.ZarrFormat != 2 || m.NodeType != "array" {
			t.Errorf("%s: zarr_format %d, node type %q", path, m.ZarrFormat, m.NodeType)
		}
		// Written as version 3, it opens as the same array.
		m.ZarrFormat = 3
		again := NewMemoryStore()
		if err := writeMetadata(ctx, again, path, m); err != nil {
			t.Fatal(err)
		}
		b, err := OpenArray(ctx, again, path)
		if err != nil {
			t.Errorf("%s: %v\n%s", path, err, must(again.Get(ctx, metadataKey(path))))
			continue
		}
		for _, k := range must(listAll(s, path+"/")) {
			if !strings.HasPrefix(k[len(path)+1:], ".z") {
				again.Set(ctx, k, must(s.Get(ctx, k)))
			}
		}
		c := interopCase{Name: path, DataType: a.DataType()}
		dispatch(t, c, func() { sameRead[bool](t, a, b) }, func() { sameRead[int8](t, a, b) },
			func() { sameRead[int16](t, a, b) }, func() { sameRead[int32](t, a, b) }, func() { sameRead[int64](t, a, b) },
			func() { sameRead[uint8](t, a, b) }, func() { sameRead[uint16](t, a, b) }, func() { sameRead[uint32](t, a, b) },
			func() { sameRead[uint64](t, a, b) }, func() { sameRead[float32](t, a, b) }, func() { sameRead[float64](t, a, b) })
	}
	// An order of F is a transpose before bytes.
	m := must(OpenArray(ctx, s, "codecs/fortran")).Metadata()
	if len(m.Codecs) != 3 || m.Codecs[0].Name != "transpose" || string(m.Codecs[0].Configuration) != `{"order":[2,1,0]}` ||
		m.Codecs[1].Name != "bytes" || m.Codecs[2].Name != "numcodecs.zlib" {
		t.Errorf("codecs %+v", m.Codecs)
	}
}

// sameRead fails unless a and b read the same.
func sameRead[T Element](t *testing.T, a, b *Array) {
	one, two := must(Read[T](ctx, a, nil, nil)), must(Read[T](ctx, b, nil, nil))
	if !sameData(one, two) {
		t.Errorf("%s: %v read as version 3 is %v", a.Path(), one, two)
	}
}

func listAll(s Store, prefix string) ([]string, error) {
	var keys []string
	err := s.List(ctx, prefix, func(k string) error { keys = append(keys, k); return nil })
	return keys, err
}

func TestZarrV2IsNotWritten(t *testing.T) {
	s := v2Store(t, "hand.zarr")
	before := s.Keys()
	a := must(OpenArray(ctx, s, "codecs/zlib"))
	g := must(OpenGroup(ctx, s, "codecs"))
	data := make([]float64, 20)
	for name, err := range map[string]error{
		"Write":               Write(ctx, a, nil, nil, data),
		"WriteChunk":          WriteChunk(ctx, a, []int{0, 0}, data[:6]),
		"Resize":              a.Resize(ctx, []int{9, 9}),
		"Append":              func() error { _, err := Append(ctx, a, 0, data[:5]); return err }(),
		"Array.SetAttributes": a.SetAttributes(ctx, map[string]any{"x": 1}),
		"Group.SetAttributes": g.SetAttributes(ctx, map[string]any{"x": 1}),
		"Group.CreateArray": func() error {
			_, err := g.CreateArray(ctx, "new", ArrayOptions{Shape: []int{1}, ChunkShape: []int{1}, DataType: Int8})
			return err
		}(),
		"Group.CreateGroup": func() error { _, err := g.CreateGroup(ctx, "new", nil); return err }(),
	} {
		if !errors.Is(err, ErrZarrV2) || !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "version 2") {
			t.Errorf("%s: %v, not ErrZarrV2", name, err)
		}
	}
	if !slices.Equal(s.Keys(), before) {
		t.Errorf("the store changed")
	}
	// Reading is untouched by all that.
	if _, err := Read[float64](ctx, a, nil, nil); err != nil {
		t.Error(err)
	}
	if err := a.Refresh(ctx); err != nil || a.Metadata().ZarrFormat != 2 {
		t.Errorf("refresh: %v", err)
	}
}

func TestZarrV2NodeOfTheWrongKind(t *testing.T) {
	s := v2Store(t, "store.zarr")
	for _, c := range []struct {
		path, says string
		open       func(path string) error
	}{
		{"temperature", "is an array", func(p string) error { _, err := OpenGroup(ctx, s, p); return err }},
		{"forecast", "is a group", func(p string) error { _, err := OpenArray(ctx, s, p); return err }},
	} {
		err := c.open(c.path)
		if err == nil || errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), c.says) || !strings.Contains(err.Error(), "version 2") {
			t.Errorf("%q: %v", c.path, err)
		}
	}
}

func TestANodeThatIsNotThereCostsThreeReads(t *testing.T) {
	s := &countingStore{MemoryStore: NewMemoryStore()}
	// Version 2 metadata beside the path, or under it, is not the node's,
	// and a .zattrs alone is not a node.
	for _, key := range []string{"ab/.zarray", "a/b/.zgroup", ".zgroup", "a/.zattrs"} {
		s.Set(ctx, key, []byte(`{"zarr_format": 2}`))
	}
	for name, open := range map[string]func() error{
		"array": func() error { _, err := OpenArray(ctx, s, "a"); return err },
		"group": func() error { _, err := OpenGroup(ctx, s, "a"); return err },
	} {
		s.gets = 0
		err := open()
		if !errors.Is(err, ErrNotFound) || errors.Is(err, ErrZarrV2) {
			t.Errorf("%s: %v, not ErrNotFound alone", name, err)
		}
		if s.gets != 3 {
			t.Errorf("%s: %d reads, not zarr.json, .zarray and .zgroup", name, s.gets)
		}
	}
	// Version 3 reads its zarr.json and nothing else.
	must(CreateArray(ctx, s, "v3", ArrayOptions{Shape: []int{1}, ChunkShape: []int{1}, DataType: Int8}))
	s.gets = 0
	must(OpenArray(ctx, s, "v3"))
	if s.gets != 1 {
		t.Errorf("a version 3 array took %d reads", s.gets)
	}
	// Version 2 reads the zarr.json that is not there, the .zarray and the
	// .zattrs.
	s.Set(ctx, "v2/.zarray", []byte(`{"zarr_format": 2, "shape": [1], "chunks": [1], "dtype": "|i1"}`))
	s.gets = 0
	must(OpenArray(ctx, s, "v2"))
	if s.gets != 3 {
		t.Errorf("a version 2 array took %d reads", s.gets)
	}
}

// zarray is a .zarray of an int16 array of 4 in chunks of 2, with fields
// laid over it.
func zarrayWith(fields string) []byte {
	b := `{"zarr_format": 2, "shape": [4], "chunks": [2], "dtype": "<i2", "fill_value": 0, "order": "C",
		"filters": null, "compressor": null, "dimension_separator": "."`
	if fields != "" {
		b += ", " + fields
	}
	return []byte(b + "}")
}

func TestZarrV2RefusesWhatItCannotRead(t *testing.T) {
	for _, c := range []struct {
		fields, says string
		unsupported  bool
	}{
		{`"compressor": {"id": "blosc", "cname": "lz4", "clevel": 5, "shuffle": 1, "blocksize": 0}`, `compressor "blosc", which no codec registered as "numcodecs.blosc" reads`, true},
		{`"filters": [{"id": "delta", "dtype": "<i2"}]`, `filter "delta", which no codec registered as "numcodecs.delta" reads`, true},
		{`"compressor": {"id": "zlib"}`, "zlib codec needs a level", false},
		{`"compressor": {"level": 1}`, "not an object with an id", false},
		{`"compressor": "zlib"`, "not an object with an id", false},
		{`"dtype": "<f2"`, `dtype "<f2"`, true},
		{`"dtype": "<c8"`, `dtype "<c8"`, true},
		{`"dtype": "|i2"`, `dtype "|i2"`, true},
		{`"dtype": "=i2"`, `dtype "=i2"`, true},
		{`"dtype": [["a", "<i2"]]`, `dtype [["a", "<i2"]]`, true},
		{`"order": "K"`, `order "K"`, false},
		{`"dimension_separator": "-"`, `dimension separator "-"`, false},
		{`"zarr_format": 3`, "zarr_format 3", true},
		{`"fill_value": "NaN"`, "fill_value", false},
		{`"fill_value": 1.5`, "fill_value", false},
		{`"shape": [-1]`, "shape", false},
		{`"chunks": [0]`, "chunk shape", false},
		{`"chunks": [2, 2]`, "differ in dimensions", false},
		{`"shape": [4611686018427387904, 4], "chunks": [1, 1]`, "more elements than an int", false},
		{`"chunks": [1099511627776]`, "more than the", false},
	} {
		s := NewMemoryStore()
		s.Set(ctx, "a/.zarray", zarrayWith(c.fields))
		_, err := OpenArray(ctx, s, "a")
		if err == nil || !strings.Contains(err.Error(), c.says) || errors.Is(err, ErrUnsupported) != c.unsupported {
			t.Errorf("%s: %v", c.fields, err)
		}
	}
	s := NewMemoryStore()
	s.Set(ctx, "a/.zarray", []byte(`{"zarr_format": 2, "shape": [4], "dtype": "<i2"}`))
	if _, err := OpenArray(ctx, s, "a"); err == nil || !strings.Contains(err.Error(), "no shape or no chunks") {
		t.Errorf("no chunks: %v", err)
	}
	s.Set(ctx, "a/.zarray", zarrayWith(""))
	s.Set(ctx, "a/.zattrs", []byte(`[]`))
	if _, err := OpenArray(ctx, s, "a"); err == nil || !strings.Contains(err.Error(), "a/.zattrs") {
		t.Errorf(".zattrs not an object: %v", err)
	}
	s.Set(ctx, "g/.zgroup", []byte(`{"zarr_format": 3}`))
	if _, err := OpenGroup(ctx, s, "g"); !errors.Is(err, ErrUnsupported) {
		t.Errorf(".zgroup of zarr_format 3: %v", err)
	}
}

func TestZarrV2Fills(t *testing.T) {
	for _, c := range []struct {
		dtype, fill string
		want        any
	}{
		{"<f4", `"NaN"`, float32(math.NaN())},
		{">f8", `"Infinity"`, math.Inf(1)},
		{"<f8", `"-Infinity"`, math.Inf(-1)},
		{"<f8", `null`, 0.0},
		{"<i4", `null`, int32(0)},
		{"|b1", `null`, false},
		{"|b1", `true`, true},
		{"<u8", `18446744073709551615`, uint64(math.MaxUint64)},
		// An integer written as the float it is.
		{"<i8", `-2.0`, int64(-2)},
		{"<u2", `1e3`, uint16(1000)},
	} {
		s := NewMemoryStore()
		s.Set(ctx, "a/.zarray", zarrayWith(`"dtype": "`+c.dtype+`", "fill_value": `+c.fill))
		a, err := OpenArray(ctx, s, "a")
		if err != nil {
			t.Errorf("%s %s: %v", c.dtype, c.fill, err)
			continue
		}
		got := a.FillValue()
		if f, ok := got.(float32); ok && f != f && c.fill == `"NaN"` {
			continue
		}
		if got != c.want {
			t.Errorf("%s %s: %v (%T)", c.dtype, c.fill, got, got)
		}
	}
}

// reverseCodec reverses bytes: a codec of a test's own, registered as
// numcodecs reads it.
type reverseCodec struct{ cfg json.RawMessage }

func (reverseCodec) Name() string         { return "numcodecs.test-reverse" }
func (c reverseCodec) Configuration() any { return c.cfg }
func (reverseCodec) EncodeBytes(b []byte) ([]byte, error) {
	out := slices.Clone(b)
	slices.Reverse(out)
	return out, nil
}
func (c reverseCodec) DecodeBytes(b []byte) ([]byte, error) { return c.EncodeBytes(b) }

func TestZarrV2CodecsAreFoundAsNumcodecs(t *testing.T) {
	RegisterCodec("numcodecs.test-reverse", func(cfg json.RawMessage, _ DataType) (Codec, error) {
		return reverseCodec{cfg}, nil
	})
	s := NewMemoryStore()
	s.Set(ctx, "a/.zarray", zarrayWith(`"compressor": {"id": "test-reverse", "x": 1}`))
	s.Set(ctx, "a/0", []byte{0, 2, 0, 1})
	a := must(OpenArray(ctx, s, "a"))
	if got := must(Read[int16](ctx, a, nil, nil)); !slices.Equal(got, []int16{1, 2, 0, 0}) {
		t.Errorf("read %v", got)
	}
	if c := a.codecs.bytes[0].(reverseCodec); string(c.cfg) != `{"x":1}` {
		t.Errorf("configuration %s, not the compressor without its id", c.cfg)
	}

	// zstd is the codec registered as zstd, which version 2 writes with no
	// checksum.
	codecsMu.Lock()
	old, had := codecs["zstd"]
	codecsMu.Unlock()
	t.Cleanup(func() {
		codecsMu.Lock()
		defer codecsMu.Unlock()
		if had {
			codecs["zstd"] = old
		} else {
			delete(codecs, "zstd")
		}
	})
	var got json.RawMessage
	RegisterCodec("zstd", func(cfg json.RawMessage, _ DataType) (Codec, error) {
		got = cfg
		return reverseCodec{cfg}, nil
	})
	s.Set(ctx, "a/.zarray", zarrayWith(`"compressor": {"id": "zstd", "level": 3}`))
	must(OpenArray(ctx, s, "a"))
	if string(got) != `{"checksum":false,"level":3}` {
		t.Errorf("zstd configured with %s", got)
	}
}

func TestZarrV2IsDeletedFromADirectory(t *testing.T) {
	dir := t.TempDir()
	s := NewDirStore(dir)
	src := v2Store(t, "hand.zarr")
	for _, k := range src.Keys() {
		if err := s.Set(ctx, k, must(src.Get(ctx, k))); err != nil {
			t.Fatal(err)
		}
	}
	// A DirStore lists the metadata of version 2, which begins with a dot,
	// and so a delete finds it.
	keys := must(listAll(s, "nested/"))
	if !slices.Contains(keys, "nested/deeper/grid/.zarray") || !slices.Contains(keys, "nested/.zattrs") {
		t.Fatalf("listed %v", keys)
	}
	names := map[string]bool{}
	ListDir(ctx, s, "nested/", func(name string) error { names[name] = true; return nil })
	if !names[".zgroup"] || !names[".zattrs"] || !names["deeper/"] {
		t.Errorf("one level: %v", names)
	}
	must(OpenArray(ctx, s, "codecs/zlib")).Delete(ctx)
	if _, err := OpenArray(ctx, s, "codecs/zlib"); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleted, and opens: %v", err)
	}
	if err := Delete(ctx, s, "nested"); err != nil {
		t.Fatal(err)
	}
	if keys := must(listAll(s, "nested/")); len(keys) != 0 {
		t.Errorf("left %v", keys)
	}
	if _, err := OpenArray(ctx, s, "nested/deeper/grid"); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleted, and opens: %v", err)
	}
	// Its metadata goes first.
	f := failingDelete{NewMemoryStore(), "codecs/gzip/0/0"}
	for _, k := range src.Keys() {
		f.Set(ctx, k, must(src.Get(ctx, k)))
	}
	if err := Delete(ctx, f, "codecs/gzip"); err == nil {
		t.Fatal("a delete that fails did not")
	}
	if _, err := OpenArray(ctx, f, "codecs/gzip"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a delete that failed part way left an array that opens: %v", err)
	}
}

func TestZlibCodec(t *testing.T) {
	data := []byte(strings.Repeat("zarr", 1000))
	for level := range 10 {
		c := ZlibCodec{Level: level}
		b := must(c.EncodeBytes(data))
		if b[0]&0x0f != 8 {
			t.Errorf("level %d: not a zlib stream: %x", level, b[:2])
		}
		if back := must(c.DecodeBytes(b)); string(back) != string(data) {
			t.Errorf("level %d: decoded to %d bytes", level, len(back))
		}
		if _, err := c.DecodeBytesLimit(b, int64(len(data))-1); err == nil {
			t.Errorf("level %d: inflated past its limit", level)
		}
		if int64(len(b)) > c.EncodedBound(int64(len(data))) {
			t.Errorf("level %d: %d bytes, past its bound", level, len(b))
		}
	}
	// A gzip stream is not a zlib one.
	if _, err := (ZlibCodec{}).DecodeBytes(must(GzipCodec{Level: 1}.EncodeBytes(data))); err == nil {
		t.Error("decoded gzip as zlib")
	}
}
