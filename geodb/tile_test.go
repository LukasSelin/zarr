package geodb

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LukasSelin/zarr"
)

// The tile the tests use: 120 by 90 pixels of 10 m in UTM 33N, near Malmö,
// in small chunks so that every read and write crosses some.
var (
	testGrid = Grid{CRS: "EPSG:32633", X0: 380000, Y0: 6170000, Dx: 10, Dy: 10, Width: 120, Height: 90}
	testVars = []Variable{
		{Name: "ndvi", Scale: 1e-4, Composite: "max"},
		{Name: "evi", Scale: 1e-4, Offset: -1},
	}
	origin = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
)

func testTileOptions(g Grid) TileOptions {
	return TileOptions{
		Grid: g, Variables: testVars, MapChunk: 32,
		CompositePeriod: 10 * 24 * time.Hour, CompositeOrigin: origin,
		CompositeTime: 4, CompositeSide: 16, StatsParcels: 4, StatsScenes: 3,
	}
}

// inverse is the longitude and latitude p takes to (x, y), found by Newton's
// method from near (lon, lat).
func inverse(p Projection, x, y, lon, lat float64) (float64, float64) {
	for range 50 {
		fx, fy := p(lon, lat)
		ex, ey := fx-x, fy-y
		if math.Hypot(ex, ey) < 1e-7 {
			break
		}
		const h = 1e-7
		ax, ay := p(lon+h, lat)
		bx, by := p(lon, lat+h)
		j11, j21, j12, j22 := (ax-fx)/h, (ay-fy)/h, (bx-fx)/h, (by-fy)/h
		det := j11*j22 - j12*j21
		lon -= (j22*ex - j12*ey) / det
		lat -= (-j21*ex + j11*ey) / det
	}
	return lon, lat
}

// parcelFromPixels is a parcel given in the pixel coordinates of g - columns
// and rows, fractional - as longitudes and latitudes.
func parcelFromPixels(g Grid, id int64, rings ...[]Point) Parcel {
	p, _ := projectionTo(g.CRS)
	out := Parcel{ID: id}
	for _, r := range rings {
		ring := make([]Point, len(r))
		for i, q := range r {
			x, y := g.X0+q.X*g.Dx, g.Y0-q.Y*g.Dy
			ring[i].X, ring[i].Y = inverse(p, x, y, 13.1, 55.65)
		}
		out.Rings = append(out.Rings, ring)
	}
	return out
}

func rect(c0, r0, c1, r1 float64) []Point {
	return []Point{{c0, r0}, {c1, r0}, {c1, r1}, {c0, r1}}
}

func testParcels(g Grid) []Parcel {
	return []Parcel{
		parcelFromPixels(g, 101, rect(2.3, 3.1, 20.7, 15.2)),
		parcelFromPixels(g, 102, rect(30, 30, 70, 60), rect(40, 40, 50, 50)), // a hole
		parcelFromPixels(g, 103, []Point{{60, 5}, {110, 20}, {75, 45}}),      // a triangle across chunks
		parcelFromPixels(g, 104, rect(100, 70, 140, 100)),                    // off the edge of the tile
		parcelFromPixels(g, 105, rect(10.2, 80.2, 10.4, 80.4)),               // smaller than a pixel
		parcelFromPixels(g, 106, rect(200, 200, 210, 210)),                   // not in the tile at all
		parcelFromPixels(g, 107, rect(0, 50, 25, 70), rect(5, 75, 15, 85)),   // in two parts
		parcelFromPixels(g, 108, []Point{{33, 0.2}, {64, 0.2}, {64, 31.8}}),  // in the chunk under cloud
		parcelFromPixels(g, 109, rect(85.5, 60.5, 95.5, 88.5), rect(87, 62, 89, 64)),
	}
}

// testScene is scene i: smooth fields with the time in them, under clouds
// that are random, and one whole map chunk of cloud in scene 2.
func testScene(g Grid, i int) Scene {
	r := rand.New(rand.NewPCG(uint64(i), 99))
	days := []int{2, 5, 9, 12, 16, 21, 30}[i]
	ndvi := make([]float32, g.Width*g.Height)
	evi := make([]float32, g.Width*g.Height)
	for row := range g.Height {
		for col := range g.Width {
			k := row*g.Width + col
			cloud := r.Float64() < 0.2 || (i == 2 && row < 32 && col >= 32 && col < 64)
			if cloud {
				ndvi[k], evi[k] = float32(math.NaN()), float32(math.NaN())
				continue
			}
			ndvi[k] = float32(0.2 + 0.004*float64(col) - 0.002*float64(row) + 0.03*float64(i) + 0.01*r.Float64())
			evi[k] = float32(-0.5 + 0.01*float64(row) + 0.02*float64(i))
		}
	}
	return Scene{Time: origin.Add(time.Duration(days) * 24 * time.Hour), Values: map[string][]float32{"ndvi": ndvi, "evi": evi}}
}

// wantStats is the statistics of a parcel in a scene, the slow way: every
// pixel whose centre is inside it, by the even-odd rule on its projected
// rings.
func wantStats(t *testing.T, g Grid, p Parcel, v Variable, values []float32) (npix int, st [numStats]float64) {
	t.Helper()
	proj, _ := projectionTo(g.CRS)
	rings := make([][]Point, len(p.Rings))
	for i, r := range p.Rings {
		rings[i] = make([]Point, len(r))
		for k, q := range r {
			rings[i][k].X, rings[i][k].Y = proj(q.X, q.Y)
		}
	}
	px := bruteForce(g, rings)
	if len(px) == 0 {
		if col, row, ok := g.Pixel(ringsBBox(rings).center()); ok {
			px = map[[2]int]bool{{col, row}: true}
		}
	}
	var vals []float64
	for c := range px {
		f := v.decode(v.encode(values[c[1]*g.Width+c[0]]))
		if !math.IsNaN(float64(f)) {
			vals = append(vals, float64(v.encode(values[c[1]*g.Width+c[0]])))
		}
	}
	for k := range st {
		st[k] = math.NaN()
	}
	if len(px) == 0 {
		return 0, st
	}
	st[StatValidFraction] = float64(len(vals)) / float64(len(px))
	if len(vals) == 0 {
		return len(px), st
	}
	slices.Sort(vals)
	var sum float64
	for _, x := range vals {
		sum += x*v.Scale + v.Offset
	}
	mean := sum / float64(len(vals))
	var ss float64
	for _, x := range vals {
		d := x*v.Scale + v.Offset - mean
		ss += d * d
	}
	pct := func(q float64) float64 {
		pos := q * float64(len(vals)-1)
		lo := math.Floor(pos)
		hi := math.Min(lo+1, float64(len(vals)-1))
		x := vals[int(lo)]*(1-(pos-lo)) + vals[int(hi)]*(pos-lo)
		return x*v.Scale + v.Offset
	}
	st[StatMean], st[StatStd] = mean, math.Sqrt(ss/float64(len(vals)))
	st[StatP10], st[StatP50], st[StatP90] = pct(0.1), pct(0.5), pct(0.9)
	return len(px), st
}

func near(a float32, b float64) bool {
	if math.IsNaN(b) {
		return a != a
	}
	return math.Abs(float64(a)-b) <= 1e-4+1e-5*math.Abs(b)
}

func checkSeries(t *testing.T, db *DB, g Grid, scenes []Scene) {
	t.Helper()
	ctx := context.Background()
	for _, p := range testParcels(g) {
		for _, v := range testVars {
			obs, err := db.ParcelSeries(ctx, p.ID, v.Name)
			if err != nil {
				t.Fatal(err)
			}
			npix, _ := wantStats(t, g, p, v, scenes[0].Values[v.Name])
			if npix == 0 {
				if len(obs) != 0 {
					t.Errorf("parcel %d is not in the tile and has %d observations", p.ID, len(obs))
				}
				continue
			}
			if len(obs) != len(scenes) {
				t.Fatalf("parcel %d %s: %d observations, want %d", p.ID, v.Name, len(obs), len(scenes))
			}
			for i, o := range obs {
				_, want := wantStats(t, g, p, v, scenes[i].Values[v.Name])
				got := [numStats]float32{o.ValidFraction, o.Mean, o.Std, o.P10, o.P50, o.P90}
				for k := range got {
					if !near(got[k], want[k]) {
						t.Errorf("parcel %d %s scene %d %s = %g, want %g", p.ID, v.Name, i, StatNames[k], got[k], want[k])
					}
				}
				if o.Pixels != npix || !o.Time.Equal(scenes[i].Time) {
					t.Errorf("parcel %d scene %d: %d pixels at %v, want %d at %v", p.ID, i, o.Pixels, o.Time, npix, scenes[i].Time)
				}
			}
		}
	}
}

func newDB(t *testing.T, s zarr.Store) *DB {
	t.Helper()
	db, err := Create(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestParcelStatisticsAreWhatEveryPixelGives(t *testing.T) {
	ctx := context.Background()
	scenes := make([]Scene, 7)
	for i := range scenes {
		scenes[i] = testScene(testGrid, i)
	}

	// Parcels first: the statistics come with each scene.
	s1 := zarr.NewMemoryStore()
	db1 := newDB(t, s1)
	if err := db1.PutParcels(ctx, testParcels(testGrid)); err != nil {
		t.Fatal(err)
	}
	tile, err := db1.CreateTile(ctx, "33UUB", testTileOptions(testGrid))
	if err != nil {
		t.Fatal(err)
	}
	if n := tile.state.Load().parcels.len(); n != 8 {
		t.Errorf("the tile covers %d parcels, want 8", n)
	}
	for i, sc := range scenes {
		at, err := tile.AppendScene(ctx, sc)
		if err != nil || at != i {
			t.Fatalf("AppendScene = %d, %v", at, err)
		}
	}
	checkSeries(t, db1, testGrid, scenes)

	// Scenes first: the statistics are worked out from the store when the
	// parcels come.
	s2 := zarr.NewMemoryStore()
	db2 := newDB(t, s2)
	tile2, err := db2.CreateTile(ctx, "33UUB", testTileOptions(testGrid))
	if err != nil {
		t.Fatal(err)
	}
	for _, sc := range scenes {
		if _, err := tile2.AppendScene(ctx, sc); err != nil {
			t.Fatal(err)
		}
	}
	if err := db2.PutParcels(ctx, testParcels(testGrid)); err != nil {
		t.Fatal(err)
	}
	checkSeries(t, db2, testGrid, scenes)

	// And from the store, opened afresh.
	db3, err := Open(ctx, s1)
	if err != nil {
		t.Fatal(err)
	}
	checkSeries(t, db3, testGrid, scenes)
	if got := db3.NumParcels(); got != 9 {
		t.Errorf("%d parcels", got)
	}
}

func TestScenesReadBackAsTheyWereWritten(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	tile, err := db.CreateTile(ctx, "t", testTileOptions(testGrid))
	if err != nil {
		t.Fatal(err)
	}
	scenes := make([]Scene, 7)
	for i := range scenes {
		scenes[i] = testScene(testGrid, i)
		if _, err := tile.AppendScene(ctx, scenes[i]); err != nil {
			t.Fatal(err)
		}
	}
	g := testGrid
	v := testVars[0]
	w := Window{Col: 30, Row: 20, Width: 40, Height: 25}
	got, err := tile.SceneWindow(ctx, "ndvi", 2, w)
	if err != nil {
		t.Fatal(err)
	}
	for r := range w.Height {
		for c := range w.Width {
			want := v.decode(v.encode(scenes[2].Values["ndvi"][(w.Row+r)*g.Width+w.Col+c]))
			if g := got[r*w.Width+c]; !(g == want || g != g && want != want) {
				t.Fatalf("pixel (%d, %d) = %g, want %g", w.Col+c, w.Row+r, g, want)
			}
		}
	}
	hist, err := tile.PixelHistory(ctx, "ndvi", 77, 44)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 7 {
		t.Fatalf("%d samples", len(hist))
	}
	for i, s := range hist {
		want := v.decode(v.encode(scenes[i].Values["ndvi"][44*g.Width+77]))
		if !(s.Value == want || s.Value != s.Value && want != want) || !s.Time.Equal(scenes[i].Time) {
			t.Errorf("scene %d: %v, want %g at %v", i, s, want, scenes[i].Time)
		}
	}
	if times := tile.Times(); len(times) != 7 || !times[6].Equal(scenes[6].Time) {
		t.Errorf("Times = %v", times)
	}
	if _, err := tile.SceneWindow(ctx, "ndvi", 7, w); !errors.Is(err, ErrNotFound) {
		t.Errorf("a scene there is not: %v", err)
	}
	if _, err := tile.PixelHistory(ctx, "lai", 0, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("a variable there is not: %v", err)
	}
	// The chunk under cloud in scene 2 was never written.
	key := strings.Join([]string{"tiles/t/scenes/ndvi/c/2/0/1"}, "")
	if _, err := db.store.Get(ctx, key); !errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("the chunk of nothing but cloud is stored: %v", err)
	}
}

func TestCompositesAreTheReducedValidValuesOfEachPeriod(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	tile, err := db.CreateTile(ctx, "t", testTileOptions(testGrid))
	if err != nil {
		t.Fatal(err)
	}
	scenes := make([]Scene, 7)
	for i := range scenes {
		scenes[i] = testScene(testGrid, i)
		if _, err := tile.AppendScene(ctx, scenes[i]); err != nil {
			t.Fatal(err)
		}
	}
	// In two goes, the second over a period the first made already.
	if err := tile.UpdateComposites(ctx, origin, origin.Add(15*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := tile.UpdateComposites(ctx, origin.Add(10*24*time.Hour), origin.Add(365*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	g := testGrid
	for _, v := range testVars {
		for _, px := range [][2]int{{0, 0}, {40, 10}, {77, 44}, {119, 89}, {16, 31}} {
			hist, err := tile.CompositeHistory(ctx, v.Name, px[0], px[1])
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) != 4 {
				t.Fatalf("%d periods, want 4", len(hist))
			}
			for p, s := range hist {
				var vals []int16
				for _, sc := range scenes {
					if int(sc.Time.Sub(origin)/(240*time.Hour)) == p {
						if x := v.encode(sc.Values[v.Name][px[1]*g.Width+px[0]]); x != fill16 {
							vals = append(vals, x)
						}
					}
				}
				want := v.decode(fill16)
				if len(vals) > 0 {
					slices.Sort(vals)
					switch v.Composite {
					case "max":
						want = v.decode(vals[len(vals)-1])
					default:
						m := len(vals) / 2
						if len(vals)%2 == 1 {
							want = v.decode(vals[m])
						} else {
							want = v.decode(int16((int32(vals[m-1]) + int32(vals[m])) / 2))
						}
					}
				}
				if !(s.Value == want || s.Value != s.Value && want != want) {
					t.Errorf("%s pixel %v period %d = %g, want %g of %v", v.Name, px, p, s.Value, want, vals)
				}
				if wantT := origin.Add(time.Duration(p) * 240 * time.Hour); !s.Time.Equal(wantT) {
					t.Errorf("period %d starts %v, want %v", p, s.Time, wantT)
				}
			}
		}
	}
}

func TestReduce(t *testing.T) {
	for _, c := range []struct {
		how  string
		vals []int16
		want int16
	}{
		{"median", nil, fill16},
		{"median", []int16{5}, 5},
		{"median", []int16{3, 1, 2}, 2},
		{"median", []int16{4, 1, 3, 2}, 2},
		{"median", []int16{-3, -4}, -3},
		{"max", []int16{-3, 7, 2}, 7},
		{"mean", []int16{1, 2}, 2},
		{"mean", []int16{-1, -2}, -2},
		{"mean", []int16{32767, 32767, 32767}, 32767},
	} {
		if got := reduce(c.how, slices.Clone(c.vals)); got != c.want {
			t.Errorf("reduce(%s, %v) = %d, want %d", c.how, c.vals, got, c.want)
		}
	}
}

func TestAScenesTimeMustBeAfterTheLast(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	tile, _ := db.CreateTile(ctx, "t", testTileOptions(testGrid))
	sc := testScene(testGrid, 3)
	if _, err := tile.AppendScene(ctx, sc); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Scene{
		sc,
		testScene(testGrid, 1),
		{Time: sc.Time.Add(time.Hour), Values: map[string][]float32{"ndvi": sc.Values["ndvi"]}},
		{Time: sc.Time.Add(time.Hour), Values: map[string][]float32{"ndvi": sc.Values["ndvi"], "evi": sc.Values["evi"][1:]}},
		{Time: sc.Time.Add(time.Hour), Values: map[string][]float32{"ndvi": sc.Values["ndvi"], "evi": sc.Values["evi"], "lai": sc.Values["evi"]}},
	} {
		if _, err := tile.AppendScene(ctx, bad); err == nil {
			t.Errorf("a scene at %v of %d variables was taken", bad.Time, len(bad.Values))
		}
	}
	if n := len(tile.Times()); n != 1 {
		t.Errorf("%d scenes", n)
	}
}

func TestBadTilesAreNotMade(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	good := testTileOptions(testGrid)
	for name, o := range map[string]func(*TileOptions){
		"no variables":                func(o *TileOptions) { o.Variables = nil },
		"a zero scale":                func(o *TileOptions) { o.Variables = []Variable{{Name: "x"}} },
		"a bad composite":             func(o *TileOptions) { o.Variables = []Variable{{Name: "x", Scale: 1, Composite: "mode"}} },
		"a kept name":                 func(o *TileOptions) { o.Variables = []Variable{{Name: "time", Scale: 1}} },
		"twice":                       func(o *TileOptions) { o.Variables = []Variable{testVars[0], testVars[0]} },
		"an unknown CRS":              func(o *TileOptions) { o.Grid.CRS = "EPSG:3006" },
		"no pixels":                   func(o *TileOptions) { o.Grid.Width = 0 },
		"a side that does not divide": func(o *TileOptions) { o.CompositeSide = 24 },
	} {
		o2 := good
		o2.Variables = slices.Clone(good.Variables)
		o(&o2)
		if _, err := db.CreateTile(ctx, "t", o2); err == nil {
			t.Errorf("a tile with %s was made", name)
		}
	}
	for _, name := range []string{"", "a/b", "..", "_x", strings.Repeat("x", 65)} {
		if _, err := db.CreateTile(ctx, name, good); err == nil {
			t.Errorf("a tile named %q was made", name)
		}
	}
	if _, err := db.CreateTile(ctx, "t", good); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateTile(ctx, "t", good); !errors.Is(err, zarr.ErrExists) {
		t.Errorf("a second tile t: %v", err)
	}
}

// failing fails the Sets of keys with a prefix while armed.
type failing struct {
	zarr.Store
	prefix string
	armed  atomic.Bool
}

var errInjected = errors.New("injected")

func (f *failing) Set(ctx context.Context, k string, v []byte) error {
	if f.armed.Load() && strings.HasPrefix(k, f.prefix) {
		return errInjected
	}
	return f.Store.Set(ctx, k, v)
}

func (f *failing) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	return f.Store.(zarr.RangeGetter).GetRange(ctx, k, off, n)
}

func TestAnAppendThatFailsLeavesNothingAndTheNextOneCutsItBack(t *testing.T) {
	ctx := context.Background()
	for _, prefix := range []string{"tiles/t/scenes/time/", "tiles/t/stats/evi/", "tiles/t/scenes/evi/"} {
		t.Run(prefix, func(t *testing.T) {
			fs := &failing{Store: zarr.NewMemoryStore(), prefix: prefix}
			db := newDB(t, fs)
			if err := db.PutParcels(ctx, testParcels(testGrid)); err != nil {
				t.Fatal(err)
			}
			tile, err := db.CreateTile(ctx, "t", testTileOptions(testGrid))
			if err != nil {
				t.Fatal(err)
			}
			scenes := make([]Scene, 4)
			for i := range scenes {
				scenes[i] = testScene(testGrid, i)
			}
			for _, sc := range scenes[:2] {
				if _, err := tile.AppendScene(ctx, sc); err != nil {
					t.Fatal(err)
				}
			}
			fs.armed.Store(true)
			if _, err := tile.AppendScene(ctx, scenes[2]); !errors.Is(err, errInjected) {
				t.Fatalf("the append that should fail: %v", err)
			}
			fs.armed.Store(false)
			// A reader sees the two scenes before, in the tile and the
			// statistics.
			other, err := Open(ctx, fs)
			if err != nil {
				t.Fatal(err)
			}
			if n := len(other.Tile("t").Times()); n != 2 {
				t.Fatalf("a reader sees %d scenes", n)
			}
			obs, err := other.ParcelSeries(ctx, 101, "ndvi")
			if err != nil || len(obs) != 2 {
				t.Fatalf("a reader sees %d observations: %v", len(obs), err)
			}
			for _, sc := range scenes[2:] {
				if _, err := tile.AppendScene(ctx, sc); err != nil {
					t.Fatal(err)
				}
			}
			checkSeries(t, db, testGrid, scenes)
		})
	}
}

func TestOverlappingTilesGiveOneObservationAPass(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	// Two tiles a column of chunks apart, both over parcel 102.
	a := testGrid
	b := testGrid
	b.X0 += 200
	ta, _ := db.CreateTile(ctx, "a", testTileOptions(a))
	tb, _ := db.CreateTile(ctx, "b", testTileOptions(b))
	if err := db.PutParcels(ctx, testParcels(a)); err != nil {
		t.Fatal(err)
	}
	// Tile b's pass of scene 0 is cloudy over the parcel; at scene 1 tile a's is.
	sa0, sb0 := testScene(a, 0), testScene(b, 0)
	clear := func(sc Scene) Scene {
		for _, v := range sc.Values {
			for k := range v {
				v[k] = float32(math.NaN())
			}
		}
		return sc
	}
	sb0 = clear(sb0)
	sb0.Time = sa0.Time.Add(20 * time.Second)
	sa1, sb1 := clear(testScene(a, 1)), testScene(b, 1)
	for _, c := range []struct {
		t  *Tile
		sc Scene
	}{{ta, sa0}, {tb, sb0}, {ta, sa1}, {tb, sb1}} {
		if _, err := c.t.AppendScene(ctx, c.sc); err != nil {
			t.Fatal(err)
		}
	}
	obs, err := db.ParcelSeries(ctx, 102, "ndvi")
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 2 || obs[0].Tile != "a" || obs[1].Tile != "b" {
		t.Fatalf("observations %+v", obs)
	}
	for _, o := range obs {
		if !(o.ValidFraction > 0.5) {
			t.Errorf("the observation kept from %s has the valid fraction %g", o.Tile, o.ValidFraction)
		}
	}
}

func TestARefreshSeesWhatAnotherProcessAppended(t *testing.T) {
	ctx := context.Background()
	s := zarr.NewMemoryStore()
	writer := newDB(t, s)
	tile, _ := writer.CreateTile(ctx, "t", testTileOptions(testGrid))
	if err := writer.PutParcels(ctx, testParcels(testGrid)); err != nil {
		t.Fatal(err)
	}
	if _, err := tile.AppendScene(ctx, testScene(testGrid, 0)); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(ctx, NewCachedStore(s, CacheOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tile.AppendScene(ctx, testScene(testGrid, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.CreateTile(ctx, "u", testTileOptions(testGrid)); err != nil {
		t.Fatal(err)
	}
	if n := len(reader.Tile("t").Times()); n != 1 {
		t.Fatalf("before a refresh the reader sees %d scenes", n)
	}
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(reader.Tile("t").Times()); n != 2 {
		t.Errorf("after a refresh the reader sees %d scenes", n)
	}
	if obs, _ := reader.ParcelSeries(ctx, 101, "ndvi"); len(obs) != 2 {
		t.Errorf("after a refresh the reader sees %d observations", len(obs))
	}
	if reader.Tile("u") == nil {
		t.Error("after a refresh the reader does not see the new tile")
	}
}

func TestParcelsAreFoundByBoxAndPoint(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	ps := testParcels(testGrid)
	if err := db.PutParcels(ctx, ps); err != nil {
		t.Fatal(err)
	}
	all := db.ParcelsIn(BBox{-180, -90, 180, 90})
	slices.Sort(all)
	if want := []int64{101, 102, 103, 104, 105, 106, 107, 108, 109}; !slices.Equal(all, want) {
		t.Errorf("every parcel: %v", all)
	}
	p102, ok := db.GetParcel(102)
	if !ok || len(p102.Rings) != 2 {
		t.Fatalf("parcel 102: %v %v", p102, ok)
	}
	b := ringsBBox(p102.Rings)
	if got := db.ParcelsIn(b); !slices.Contains(got, 102) || slices.Contains(got, 106) {
		t.Errorf("parcels in the box of 102: %v", got)
	}
	// The middle of 102 is its hole; a point between hole and edge is in it.
	in := parcelFromPixels(testGrid, 0, []Point{{35, 35}, {35, 35}, {35, 35}}).Rings[0][0]
	hole := parcelFromPixels(testGrid, 0, []Point{{45, 45}, {45, 45}, {45, 45}}).Rings[0][0]
	if got := db.ParcelAt(in); !slices.Equal(got, []int64{102}) {
		t.Errorf("ParcelAt a point in 102: %v", got)
	}
	if got := db.ParcelAt(hole); len(got) != 0 {
		t.Errorf("ParcelAt the hole of 102: %v", got)
	}
	for _, bad := range [][]Parcel{
		{{ID: 1, Rings: [][]Point{{{0, 0}, {1, 0}}}}},
		{{ID: 1}},
		{{ID: 1, Rings: [][]Point{{{0, 0}, {1, 0}, {1, 91}}}}},
		{ps[0], ps[0]},
	} {
		if err := db.PutParcels(ctx, bad); err == nil {
			t.Errorf("the parcels %v were put", bad)
		}
	}
	if db.NumParcels() != 9 {
		t.Errorf("a PutParcels that was refused changed the parcels")
	}
	if _, err := db.ParcelSeries(ctx, 999, "ndvi"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a parcel there is not: %v", err)
	}
}
