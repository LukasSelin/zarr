package geozarr

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/LukasSelin/zarr"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// utm is a 10 m grid in UTM 33N with its top left corner at (500000, 6000000).
var (
	utm      = CRS{EPSG: 32633, WKT2: `PROJCRS["WGS 84 / UTM zone 33N"]`}
	utmGrid  = NorthUp(500000, 6000000, 10, 10)
	testSide = 300
)

func root(t testing.TB) *zarr.Group {
	t.Helper()
	g, err := zarr.CreateGroup(context.Background(), zarr.NewMemoryStore(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func cell(band, row, col int) float32 { return float32(band*1_000_000 + row*1000 + col) }

func TestTransform(t *testing.T) {
	for _, tr := range []Transform{utmGrid, {A: 2, B: 0.5, C: 10, D: -0.25, E: -3, F: 50}} {
		for _, p := range [][2]float64{{0, 0}, {3.5, 7.25}, {-2, 100}} {
			x, y := tr.Apply(p[0], p[1])
			c, r := tr.Invert(x, y)
			if math.Abs(c-p[0]) > 1e-9 || math.Abs(r-p[1]) > 1e-9 {
				t.Errorf("%v: %v went to (%v, %v) and back to (%v, %v)", tr, p, x, y, c, r)
			}
		}
		if got := FromGDAL(tr.GDAL()); got != tr {
			t.Errorf("GDAL round trip: %v, not %v", got, tr)
		}
	}
}

func TestWindow(t *testing.T) {
	tr := utmGrid
	for _, c := range []struct {
		name string
		b    BBox
		want Window
	}{
		{"pixel edges", BBox{500000, 6000000 - 30, 500050, 6000000}, Window{0, 0, 3, 5}},
		{"inside one pixel", BBox{500012, 5999985, 500013, 5999986}, Window{1, 1, 1, 1}},
		{"a point on an edge", BBox{500010, 5999990, 500010, 5999990}, Window{1, 1, 1, 1}},
		{"clipped", BBox{499000, 5999000, 500025, 6001000}, Window{0, 0, 100, 3}},
		{"missing", BBox{0, 0, 1, 1}, Window{100, 0, 0, 0}},
	} {
		if got := tr.window(c.b, 100, 100); got != c.want && !(got.Empty() && c.want.Empty()) {
			t.Errorf("%s: %+v, not %+v", c.name, got, c.want)
		}
	}
}

func TestCRSAttributes(t *testing.T) {
	ctx := context.Background()
	g := root(t)
	c := CRS{EPSG: 4326, WKT2: "GEOGCRS[...]", PROJJSON: []byte(`{"type":"GeographicCRS"}`)}
	attrs := map[string]any{}
	c.attributes(attrs)
	sub, err := g.CreateGroup(ctx, "a", attrs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := crsFrom(sub.Attribute)
	if err != nil {
		t.Fatal(err)
	}
	if got.EPSG != 4326 || got.WKT2 != c.WKT2 || string(got.PROJJSON) != string(c.PROJJSON) {
		t.Errorf("%+v, not %+v", got, c)
	}
	// GDAL's _CRS alone.
	gdal, err := g.CreateGroup(ctx, "b", map[string]any{"_CRS": attrs["_CRS"]})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := crsFrom(gdal.Attribute); err != nil || got.EPSG != 4326 || got.WKT2 != c.WKT2 {
		t.Errorf("from _CRS: %+v, %v", got, err)
	}
}

func newBands(t testing.TB, g *zarr.Group, o RasterOptions) *Raster {
	t.Helper()
	ctx := context.Background()
	if o.Width == 0 {
		o.Width, o.Height = testSide, testSide-17
	}
	o.Leading = []Dim{{Name: "band", Size: 3}}
	o.DataType = zarr.Float32
	o.FillValue = math.NaN()
	o.CRS = utm
	o.Transform = utmGrid
	r, err := CreateRaster(ctx, g, "refl", o)
	if err != nil {
		t.Fatal(err)
	}
	err = Ingest(ctx, r, func(b Block, dst []float32) error {
		i := 0
		for band := b.Start[0]; band < b.Start[0]+b.Count[0]; band++ {
			for row := b.Row; row < b.Row+b.Height; row++ {
				for col := b.Col; col < b.Col+b.Width; col++ {
					dst[i] = cell(band, row, col)
					i++
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRaster(t *testing.T) {
	for _, o := range []RasterOptions{
		{ChunkSize: 64},
		{ChunkSize: 32, ShardSize: 128, Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.GzipCodec{Level: 1}}},
		{ChunkSize: 64, Coordinates: true},
	} {
		ctx := context.Background()
		g := root(t)
		newBands(t, g, o)

		r, err := OpenRaster(ctx, g, "refl")
		if err != nil {
			t.Fatal(err)
		}
		if r.CRS.EPSG != 32633 || r.Transform != utmGrid || r.Height() != testSide-17 || r.Width() != testSide {
			t.Fatalf("opened as %+v %v by %v", r.CRS, r.Height(), r.Width())
		}
		if b := r.Bounds(); b != (BBox{500000, 6000000 - 10*float64(testSide-17), 500000 + 10*float64(testSide), 6000000}) {
			t.Errorf("bounds %v", b)
		}

		b := BBox{500000 + 1234, 6000000 - 2000, 500000 + 1600, 6000000 - 777}
		v, w, err := ReadBBox[float32](ctx, r, []int{2}, b)
		if err != nil {
			t.Fatal(err)
		}
		if w != (Window{Row: 77, Col: 123, Height: 123, Width: 37}) {
			t.Fatalf("window %+v", w)
		}
		for i, got := range v {
			if want := cell(2, w.Row+i/w.Width, w.Col+i%w.Width); got != want {
				t.Fatalf("pixel %d: %v, not %v", i, got, want)
			}
		}
		if wt := r.WindowTransform(w); wt.C != 501230 || wt.F != 5999230 {
			t.Errorf("window transform %v", wt)
		}

		if o.Coordinates {
			xs, err := g.OpenArray(ctx, "x")
			if err != nil {
				t.Fatal(err)
			}
			x, err := zarr.Read[float64](ctx, xs, []int{2}, []int{1})
			if err != nil || x[0] != 500025 {
				t.Errorf("x[2] = %v, %v", x, err)
			}
			sr, err := g.OpenArray(ctx, "spatial_ref")
			if err != nil {
				t.Fatal(err)
			}
			var gt string
			if _, err := sr.Attribute("GeoTransform", &gt); err != nil || gt != "500000 10 0 6000000 0 -10" {
				t.Errorf("GeoTransform %q, %v", gt, err)
			}
		}
	}
}

// A raster written by rioxarray has no spatial: attributes, only a
// grid_mapping naming an array with a GeoTransform.
func TestOpenGridMapping(t *testing.T) {
	ctx := context.Background()
	g := root(t)
	if _, err := g.CreateArray(ctx, "spatial_ref", zarr.ArrayOptions{
		Shape: []int{}, ChunkShape: []int{}, DataType: zarr.Int64,
		Attributes: map[string]any{"crs_wkt": "PROJCRS[]", "GeoTransform": "500000.0 10.0 0.0 6000000.0 0.0 -10.0"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.CreateArray(ctx, "dem", zarr.ArrayOptions{
		Shape: []int{10, 10}, ChunkShape: []int{10, 10}, DataType: zarr.Float32,
		DimensionNames: []string{"y", "x"}, Attributes: map[string]any{"grid_mapping": "spatial_ref"},
	}); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRaster(ctx, g, "dem")
	if err != nil {
		t.Fatal(err)
	}
	if r.Transform != utmGrid || r.CRS.WKT2 != "PROJCRS[]" {
		t.Errorf("%v %+v", r.Transform, r.CRS)
	}
}

func TestWKTCRS(t *testing.T) {
	for _, c := range []struct {
		name, wkt string
		epsg      int
		wkt2      bool
	}{
		{"WKT2", `PROJCRS["UTM 33N",BASEGEOGCRS["WGS 84",ID["EPSG",4326]],ID["EPSG",32633]]`, 32633, true},
		{"WKT1", `PROJCS["UTM 33N",GEOGCS["WGS 84",AUTHORITY["EPSG","4326"]],AUTHORITY["EPSG","32633"]]`, 32633, false},
		{"WKT1 without a code", `PROJCS["UTM 33N",GEOGCS["WGS 84",AUTHORITY["EPSG","4326"]]]`, 0, false},
		{"brackets in a name", `GEOGCRS["a ]""[ b",ID["epsg",4326,URI["urn:x"]]]`, 4326, true},
		{"another authority", `GEOGCRS["x",ID["ESRI",102100]]`, 0, true},
		{"not WKT", "EPSG:4326", 0, false},
	} {
		want := CRS{EPSG: c.epsg}
		if c.wkt2 {
			want.WKT2 = c.wkt
		}
		if got := wktCRS(c.wkt); got.EPSG != want.EPSG || got.WKT2 != want.WKT2 {
			t.Errorf("%s: %+v, not %+v", c.name, got, want)
		}
	}
}

func TestIngestError(t *testing.T) {
	ctx := context.Background()
	r, err := CreateRaster(ctx, root(t), "r", RasterOptions{
		Width: 1000, Height: 1000, ChunkSize: 10, DataType: zarr.Uint8, Transform: utmGrid,
	})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	var calls sync.Map
	err = Ingest(ctx, r, func(b Block, _ []uint8) error {
		calls.Store(b.Window, true)
		if b.Row == 500 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Errorf("%v, not boom", err)
	}
}

func TestSampler(t *testing.T) {
	ctx := context.Background()
	g := root(t)
	r := newBands(t, g, RasterOptions{ChunkSize: 32, ShardSize: 64})
	s, err := NewSampler[float32](r, 4)
	if err != nil {
		t.Fatal(err)
	}
	var xs, ys []float64
	var want []float32
	for i := range 500 {
		row, col := (i*37)%(testSide-17), (i*91)%testSide
		x, y := utmGrid.Apply(float64(col)+0.3, float64(row)+0.9)
		xs, ys, want = append(xs, x), append(ys, y), append(want, cell(1, row, col))
	}
	xs, ys = append(xs, 0), append(ys, 0) // off the raster
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, in, err := s.Points(ctx, []int{1}, xs, ys)
			if err != nil {
				t.Error(err)
				return
			}
			if !slices.Equal(got[:len(want)], want) || slices.Contains(in[:len(want)], false) {
				t.Error("points read wrong")
			}
			if in[len(want)] || !math.IsNaN(float64(got[len(want)])) {
				t.Errorf("off the raster: %v %v", got[len(want)], in[len(want)])
			}
		}()
	}
	wg.Wait()
	v, ok, err := s.At(ctx, []int{0}, 500000+55, 6000000-25)
	if err != nil || !ok || v != cell(0, 2, 5) {
		t.Errorf("At: %v %v %v", v, ok, err)
	}
	if s.lru.Len() > 4 {
		t.Errorf("cache holds %d chunks, more than 4", s.lru.Len())
	}
}

func testUnits(t testing.TB, g *zarr.Group, n int) *Units {
	t.Helper()
	ids := make([]int64, n)
	boxes := make([]BBox, n)
	for i := range ids {
		ids[i] = int64(1000 + 7*i)
		x, y := float64(i%100), float64(i/100)
		boxes[i] = BBox{x, y, x + 1, y + 1}
	}
	u, err := CreateUnits(context.Background(), g, "regions", UnitsOptions{IDs: ids, Boxes: boxes, CRS: CRS{EPSG: 3006}})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestUnits(t *testing.T) {
	ctx := context.Background()
	g := root(t)
	const n, days = 1000, 40
	u := testUnits(t, g, n)
	v, err := u.CreateVariable(ctx, "rain", VariableOptions{
		Leading: []Dim{{Name: "time", Size: days}}, DataType: zarr.Float64, UnitChunk: 16, UnitShard: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	all := make([]float64, days*n)
	for d := range days {
		for i := range n {
			all[d*n+i] = float64(d)*1e6 + float64(u.IDs[i])
		}
	}
	if err := zarr.Write(ctx, v.Array, nil, nil, all); err != nil {
		t.Fatal(err)
	}

	u, err = OpenUnits(ctx, g, "regions")
	if err != nil {
		t.Fatal(err)
	}
	if u.CRS.EPSG != 3006 || len(u.IDs) != n || len(u.Boxes) != n {
		t.Fatalf("opened %+v with %d ids and %d boxes", u.CRS, len(u.IDs), len(u.Boxes))
	}
	if v, err = u.OpenVariable(ctx, "rain"); err != nil {
		t.Fatal(err)
	}
	ids := []int64{1000 + 7*999, 1000, 1000 + 7*17, 1000 + 7*18, 1000}
	got, err := ReadUnits[float64](ctx, v, ids)
	if err != nil {
		t.Fatal(err)
	}
	for k, id := range ids {
		for d, x := range got[k] {
			if want := float64(d)*1e6 + float64(id); x != want {
				t.Fatalf("unit %d day %d: %v, not %v", id, d, x, want)
			}
		}
	}
	if _, err := ReadUnit[float64](ctx, v, 3); err == nil {
		t.Error("read a unit that is not there")
	}
	if w := u.Within(BBox{10.5, 2.5, 11.5, 2.6}); !slices.Equal(w, []int64{1000 + 7*210, 1000 + 7*211}) {
		t.Errorf("within: %v", w)
	}
}

func TestZonal(t *testing.T) {
	ctx := context.Background()
	g := root(t)
	vals := newBands(t, g, RasterOptions{ChunkSize: 50})
	zones, err := CreateRaster(ctx, g, "zones", RasterOptions{
		Width: vals.Width(), Height: vals.Height(), DataType: zarr.Int32, FillValue: -1,
		Transform: utmGrid, ChunkSize: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	zoneOf := func(row, col int) int32 {
		if row < 5 {
			return -1 // no zone
		}
		return int32(row/40*10 + col/70)
	}
	if err := Ingest(ctx, zones, func(b Block, dst []int32) error {
		for i := range dst {
			dst[i] = zoneOf(b.Row+i/b.Width, b.Col+i%b.Width)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	want := map[int64]*Stats{}
	for row := range vals.Height() {
		for col := range vals.Width() {
			z := zoneOf(row, col)
			if z < 0 {
				continue
			}
			if want[int64(z)] == nil {
				want[int64(z)] = &Stats{}
			}
			want[int64(z)].add(float64(cell(1, row, col)))
		}
	}
	got, err := Zonal[float32, int32](ctx, vals, []int{1}, zones)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("%d zones, not %d", len(got), len(want))
	}
	for id, w := range want {
		gs := got[id]
		if gs.Count != w.Count || gs.Min != w.Min || gs.Max != w.Max || math.Abs(gs.Sum-w.Sum) > 1e-6*math.Abs(w.Sum) {
			t.Errorf("zone %d: %+v, not %+v", id, gs, *w)
		}
	}

	u, err := CreateUnits(ctx, g, "zones_units", UnitsOptions{IDs: []int64{0, 1, 999}})
	if err != nil {
		t.Fatal(err)
	}
	col := u.Column(got, Stats.Mean, math.NaN())
	if col[0] != got[0].Mean() || col[1] != got[1].Mean() || !math.IsNaN(col[2]) {
		t.Errorf("column %v", col)
	}
}

func TestCancel(t *testing.T) {
	g := root(t)
	r := newBands(t, g, RasterOptions{ChunkSize: 16})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := ReadBBox[float32](ctx, r, []int{0}, r.Bounds()); !errors.Is(err, context.Canceled) {
		t.Errorf("ReadBBox: %v", err)
	}
	if err := Ingest(ctx, r, func(Block, []float32) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("Ingest: %v", err)
	}
}
