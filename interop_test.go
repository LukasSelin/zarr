package zarr

import (
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The interop test runs testdata/interop/interop.py under the Python named by
// ZARR_PYTHON, which must have zarr and numpy installed, and is skipped
// without it:
//
//	python -m venv .venv && .venv/bin/pip install zarr numpy
//	ZARR_PYTHON=.venv/bin/python go test -run ZarrPython
//
// zarr-python writes every case in cases.json and this package reads them,
// then this package writes them and zarr-python reads them. interop.py says
// how the data of each case is made.

type interopCase struct {
	Name           string          `json:"name"`
	DataType       DataType        `json:"dtype"`
	Shape          []int           `json:"shape"`
	Chunks         []int           `json:"chunks"`
	Fill           json.RawMessage `json:"fill"`
	Endian         Endian          `json:"endian"`
	Compressors    []string        `json:"compressors"`
	Separator      string          `json:"separator"`
	DimensionNames []*string       `json:"dimension_names"`
	Sharding       *struct {
		Shape         []int         `json:"shape"`
		IndexLocation IndexLocation `json:"index_location"`
		After         []string      `json:"after"`
	} `json:"sharding"`
}

func interopCases(t testing.TB) []interopCase {
	b, err := os.ReadFile(filepath.Join("testdata", "interop", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []interopCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// interopValue is element i of a case, as interop.py makes it.
func interopValue[T Element](i int) T {
	var v any
	switch any(*new(T)).(type) {
	case bool:
		v = i%3 == 0
	case uint64:
		v = uint64(math.MaxUint64) - uint64(i)
	case uint8:
		v = uint8(i * 7 % 251)
	case uint16:
		v = uint16(i * 7 % 251)
	case uint32:
		v = uint32(i * 7 % 251)
	case int8:
		v = int8(i*7%200 - 100)
	case int16:
		v = int16(i*7%200 - 100)
	case int32:
		v = int32(i*7%200 - 100)
	case int64:
		v = int64(i*7%200 - 100)
	case float32:
		v = float32(float64(i)*0.25 - 3)
	case float64:
		v = float64(i)*0.25 - 3
	}
	return v.(T)
}

func interopData[T Element](t testing.TB, c interopCase) []T {
	fill, err := parseFill(c.DataType, c.Fill)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]T, product(c.Shape))
	i := 0
	eachIndex(make([]int, len(c.Shape)), minus(c.Shape, slices.Repeat([]int{1}, len(c.Shape))), func(p []int) error {
		data[i] = interopValue[T](i)
		first := len(p) > 0
		for k := range p {
			first = first && p[k] < c.Chunks[k]
		}
		if first {
			data[i] = fill.(T)
		}
		i++
		return nil
	})
	return data
}

func sameData[T Element](got, want []T) bool {
	return slices.EqualFunc(got, want, func(a, b T) bool { return a == b || (a != a && b != b) })
}

func checkInterop[T Element](t *testing.T, s Store, c interopCase) {
	a, err := OpenArray(ctx, s, c.Name)
	if err != nil {
		t.Errorf("%s: %v", c.Name, err)
		return
	}
	got, err := Read[T](ctx, a, nil, nil)
	if err != nil {
		t.Errorf("%s: %v", c.Name, err)
		return
	}
	if want := interopData[T](t, c); !sameData(got, want) {
		t.Errorf("%s:\n got %v\nwant %v", c.Name, got, want)
	}
	if !slices.Equal(a.ChunkShape(), c.Chunks) {
		t.Errorf("%s: chunks %v", c.Name, a.ChunkShape())
	}
	if c.Sharding != nil {
		if !slices.Equal(a.ShardShape(), c.Sharding.Shape) || a.shard.location() != c.Sharding.IndexLocation {
			t.Errorf("%s: shards %v, index at the %s", c.Name, a.ShardShape(), a.shard.location())
		}
	}
	if c.DimensionNames != nil {
		want := make([]string, len(c.DimensionNames))
		for i, n := range c.DimensionNames {
			if n != nil {
				want[i] = *n
			}
		}
		if got := a.DimensionNames(); !slices.Equal(got, want) {
			t.Errorf("%s: dimension names %q", c.Name, got)
		}
	}
}

func writeInterop[T Element](t testing.TB, s Store, c interopCase) {
	codecs := []Codec{BytesCodec{Endian: c.Endian}}
	for _, name := range c.Compressors {
		if name == "gzip" {
			codecs = append(codecs, GzipCodec{Level: 5})
		} else {
			codecs = append(codecs, CRC32CCodec{})
		}
	}
	grid := c.Chunks
	if c.Sharding != nil {
		codecs = []Codec{&ShardingCodec{ChunkShape: c.Chunks, Codecs: codecs, IndexLocation: c.Sharding.IndexLocation}}
		for range c.Sharding.After {
			codecs = append(codecs, GzipCodec{Level: 5})
		}
		grid = c.Sharding.Shape
	}
	fill, err := parseFill(c.DataType, c.Fill)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, n := range c.DimensionNames {
		if n == nil {
			names = append(names, "")
		} else {
			names = append(names, *n)
		}
	}
	if dir, _, ok := strings.Cut(c.Name, "/"); ok {
		if _, err := OpenGroup(ctx, s, dir); err != nil {
			if _, err := CreateGroup(ctx, s, dir, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	a, err := CreateArray(ctx, s, c.Name, ArrayOptions{
		Shape: c.Shape, ChunkShape: grid, DataType: c.DataType, FillValue: fill,
		Codecs: codecs, Separator: c.Separator, DimensionNames: names,
	})
	if err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
	if err := Write(ctx, a, nil, nil, interopData[T](t, c)); err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
}

func runPython(t *testing.T, py string, args ...string) {
	t.Helper()
	cmd := exec.Command(py, append([]string{filepath.Join("testdata", "interop", "interop.py")}, args...)...)
	out, err := cmd.CombinedOutput()
	t.Logf("zarr-python %s: %s", args[0], strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("zarr-python %s failed: %v", args[0], err)
	}
}

func TestZarrPython(t *testing.T) {
	py := os.Getenv("ZARR_PYTHON")
	if py == "" {
		t.Skip("ZARR_PYTHON is not set")
	}
	cases := interopCases(t)
	dir := t.TempDir()

	t.Run("python writes, Go reads", func(t *testing.T) {
		path := filepath.Join(dir, "python.zarr")
		runPython(t, py, "write", path)
		s := NewDirStore(path)
		root, err := OpenGroup(ctx, s, "")
		if err != nil {
			t.Fatal(err)
		}
		var seed uint64
		if _, err := root.Attribute("seed", &seed); err != nil || seed != math.MaxUint64 {
			t.Errorf("seed %d: %v", seed, err)
		}
		for _, c := range cases {
			dispatch(t, c, func() { checkInterop[bool](t, s, c) }, func() { checkInterop[int8](t, s, c) },
				func() { checkInterop[int16](t, s, c) }, func() { checkInterop[int32](t, s, c) }, func() { checkInterop[int64](t, s, c) },
				func() { checkInterop[uint8](t, s, c) }, func() { checkInterop[uint16](t, s, c) }, func() { checkInterop[uint32](t, s, c) },
				func() { checkInterop[uint64](t, s, c) }, func() { checkInterop[float32](t, s, c) }, func() { checkInterop[float64](t, s, c) })
		}
	})

	t.Run("Go writes, python reads", func(t *testing.T) {
		path := filepath.Join(dir, "go.zarr")
		s := NewDirStore(path)
		if _, err := CreateGroup(ctx, s, "", map[string]any{"seed": uint64(math.MaxUint64), "name": "terra"}); err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			dispatch(t, c, func() { writeInterop[bool](t, s, c) }, func() { writeInterop[int8](t, s, c) },
				func() { writeInterop[int16](t, s, c) }, func() { writeInterop[int32](t, s, c) }, func() { writeInterop[int64](t, s, c) },
				func() { writeInterop[uint8](t, s, c) }, func() { writeInterop[uint16](t, s, c) }, func() { writeInterop[uint32](t, s, c) },
				func() { writeInterop[uint64](t, s, c) }, func() { writeInterop[float32](t, s, c) }, func() { writeInterop[float64](t, s, c) })
		}
		runPython(t, py, "read", path)
	})
}

// dispatch calls the one of fs for the case's data type, in the order of the
// DataType constants.
func dispatch(t testing.TB, c interopCase, fs ...func()) {
	for i, d := range []DataType{Bool, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64, Float32, Float64} {
		if d == c.DataType {
			fs[i]()
			return
		}
	}
	t.Fatalf("%s: no data type %q", c.Name, c.DataType)
}
