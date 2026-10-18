package geozarr

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LukasSelin/zarr"
)

// The interop test runs testdata/interop/geozarr.py under the Python named by
// GEOZARR_PYTHON, which must have zarr, xarray, rioxarray and rasterio
// installed, and is skipped without it:
//
//	python -m venv .venv && .venv/bin/pip install zarr xarray rioxarray rasterio
//	GEOZARR_PYTHON=.venv/bin/python go test -run GeoPython ./geozarr
//
// This package writes rasters and xarray, rioxarray and GDAL's Zarr driver
// (through rasterio) read them, then rioxarray writes one and OpenRaster reads
// it. geozarr.py says what it checks; the grids and the pixel values are the
// ones below, and the CRS is testdata/interop/epsg32633.wkt.

// interopRasters are the rasters this package writes, each in a store of its
// own under the directory geozarr.py is given.
var interopRasters = []struct {
	store, name string
	o           RasterOptions
}{
	{"plain.zarr", "elevation", RasterOptions{
		Width: 70, Height: 50, DataType: zarr.Uint16, FillValue: uint16(65535),
		Transform: NorthUp(500000, 6600000, 10, 20), ChunkSize: 16,
	}},
	{"sharded.zarr", "bands", RasterOptions{
		Width: 48, Height: 40, Leading: []Dim{{Name: "band", Size: 2}}, DataType: zarr.Float32,
		Transform: NorthUp(400000, 6500000, 30, 30), ChunkSize: 8, ShardSize: 16,
	}},
}

// interopPixel is the pixel at (band, row, col) of every raster here, as
// geozarr.py makes it.
func interopPixel(band, row, col int) float64 { return float64(band*100_000 + row*1000 + col) }

func interopCRS(t testing.TB) CRS {
	wkt, err := os.ReadFile(filepath.Join("..", "testdata", "interop", "epsg32633.wkt"))
	if err != nil {
		t.Fatal(err)
	}
	return CRS{EPSG: 32633, WKT2: string(wkt)}
}

func writeInteropRaster[T float32 | uint16](t *testing.T, dir string, store, name string, o RasterOptions) {
	ctx := context.Background()
	g, err := zarr.CreateGroup(ctx, zarr.NewDirStore(filepath.Join(dir, store)), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	o.CRS, o.Coordinates = interopCRS(t), true
	r, err := CreateRaster(ctx, g, name, o)
	if err != nil {
		t.Fatal(err)
	}
	err = Ingest(ctx, r, func(b Block, dst []T) error {
		bands, band0 := 1, 0
		if len(b.Count) > 0 {
			bands, band0 = b.Count[0], b.Start[0]
		}
		i := 0
		for band := range bands {
			for row := range b.Height {
				for col := range b.Width {
					dst[i] = T(interopPixel(band0+band, b.Row+row, b.Col+col))
					i++
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func runGeoPython(t *testing.T, py string, args ...string) {
	t.Helper()
	cmd := exec.Command(py, append([]string{filepath.Join("..", "testdata", "interop", "geozarr.py")}, args...)...)
	out, err := cmd.CombinedOutput()
	t.Logf("geozarr.py %s: %s", args[0], strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("geozarr.py %s failed: %v", args[0], err)
	}
}

func TestGeoPython(t *testing.T) {
	py := os.Getenv("GEOZARR_PYTHON")
	if py == "" {
		t.Skip("GEOZARR_PYTHON is not set")
	}
	dir := t.TempDir()

	t.Run("Go writes, python reads", func(t *testing.T) {
		for _, c := range interopRasters {
			switch c.o.DataType {
			case zarr.Uint16:
				writeInteropRaster[uint16](t, dir, c.store, c.name, c.o)
			case zarr.Float32:
				writeInteropRaster[float32](t, dir, c.store, c.name, c.o)
			}
		}
		runGeoPython(t, py, "read", dir)
	})

	t.Run("python writes, Go reads", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(dir, "rioxarray.zarr")
		runGeoPython(t, py, "write", path)
		g, err := zarr.OpenGroup(ctx, zarr.NewDirStore(path), "")
		if err != nil {
			t.Fatal(err)
		}
		r, err := OpenRaster(ctx, g, "elevation")
		if err != nil {
			t.Fatal(err)
		}
		if want := NorthUp(500000, 6600000, 10, 20); r.Transform != want {
			t.Errorf("transform %+v, not %+v", r.Transform, want)
		}
		// rioxarray writes crs_wkt in WKT1, which is read as its EPSG code.
		if r.CRS.EPSG != 32633 || r.CRS.WKT2 != "" {
			t.Errorf("CRS %+v, not EPSG:32633", r.CRS)
		}
		if r.Height() != 50 || r.Width() != 70 || len(r.Leading()) != 0 {
			t.Fatalf("%v by %v, leading %v", r.Height(), r.Width(), r.Leading())
		}
		got, err := ReadWindow[float32](ctx, r, nil, Window{Height: 50, Width: 70})
		if err != nil {
			t.Fatal(err)
		}
		for i, v := range got {
			if want := float32(interopPixel(0, i/70, i%70)); v != want {
				t.Fatalf("pixel %d is %v, not %v", i, v, want)
			}
		}
	})
}
