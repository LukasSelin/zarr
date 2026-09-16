package zstd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/LukasSelin/zarr"
)

// TestZarrPython has zarr-python write arrays compressed with zstd for this
// package to read, and read back what this package writes. It is skipped
// unless ZARR_PYTHON names a Python with zarr and numpy, as package zarr's
// interop test is.
func TestZarrPython(t *testing.T) {
	python := os.Getenv("ZARR_PYTHON")
	if python == "" {
		t.Skip("ZARR_PYTHON is not set")
	}
	dir := t.TempDir()
	want := make([]float64, 9*7)
	for i := range want {
		want[i] = float64(i)*0.25 - 3
	}
	type array struct {
		name     string
		level    int
		checksum bool
		sharded  bool
	}
	arrays := []array{{"l0", 0, false, false}, {"l3_crc", 3, true, false}, {"l22", 22, false, false},
		{"neg", -3, true, false}, {"shards", 5, true, true}}

	// This package writes; zarr-python reads.
	s := zarr.NewDirStore(filepath.Join(dir, "go.zarr"))
	if _, err := zarr.CreateGroup(ctx, s, "", nil); err != nil {
		t.Fatal(err)
	}
	for _, c := range arrays {
		o := zarr.ArrayOptions{Shape: []int{9, 7}, ChunkShape: []int{4, 6}, DataType: zarr.Float64,
			Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, Codec{Level: c.level, Checksum: c.checksum}}}
		if c.sharded {
			o.ChunkShape, o.ShardShape = []int{2, 3}, []int{4, 6}
		}
		a, err := zarr.CreateArray(ctx, s, c.name, o)
		if err != nil {
			t.Fatal(err)
		}
		if err := zarr.Write(ctx, a, nil, nil, want); err != nil {
			t.Fatal(err)
		}
	}

	// zarr-python writes; this package reads.
	script := fmt.Sprintf(`
import numpy as np, sys, zarr
from zarr.codecs import ZstdCodec
want = (np.arange(63, dtype="<f8") * 0.25 - 3).reshape(9, 7)
g = zarr.open_group(sys.argv[1], mode="r")
for name in %q.split():
    got = g[name][:]
    assert np.array_equal(got, want), (name, got)
out = zarr.open_group(sys.argv[2], mode="w")
for name, level, checksum, shards in [("l0", 0, False, None), ("l3_crc", 3, True, None), ("l19", 19, False, None), ("shards", 1, True, (4, 6))]:
    a = out.create_array(name, shape=(9, 7), chunks=(2, 3) if shards else (4, 6), shards=shards, dtype="<f8",
                         compressors=ZstdCodec(level=level, checksum=checksum))
    a[:] = want
`, "l0 l3_crc l22 neg shards")
	cmd := exec.Command(python, "-c", script, filepath.Join(dir, "go.zarr"), filepath.Join(dir, "py.zarr"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("zarr-python: %v\n%s", err, out)
	}
	p := zarr.NewDirStore(filepath.Join(dir, "py.zarr"))
	for _, name := range []string{"l0", "l3_crc", "l19", "shards"} {
		a, err := zarr.OpenArray(ctx, p, name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := zarr.Read[float64](ctx, a, nil, nil)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("%s: read %v, %v", name, got, err)
		}
	}
}
