package geodb

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/LukasSelin/zarr"
)

var (
	weatherGrid  = Grid{CRS: "EPSG:4326", X0: 10, Y0: 60, Dx: 0.25, Dy: 0.25, Width: 24, Height: 16}
	weatherStart = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
)

func weatherOptions() WeatherOptions {
	return WeatherOptions{
		Grid: weatherGrid, Start: weatherStart, Step: time.Hour,
		Variables: []string{"wind", "ghi"}, ChunkSteps: 24, ChunkSide: 8, ParcelChunk: 4,
	}
}

// field is linear in longitude, latitude and time, which bilinear
// interpolation gives back exactly between cell centres.
func field(v string, step int, lon, lat float64) float64 {
	if v == "wind" {
		return 2*lon + 3*lat + 0.1*float64(step)
	}
	return 100*lat - 50*lon + float64(step)
}

func weatherSteps(from, n int) map[string][]float32 {
	g := weatherGrid
	out := map[string][]float32{}
	for _, v := range []string{"wind", "ghi"} {
		vals := make([]float32, n*g.Width*g.Height)
		for s := range n {
			for row := range g.Height {
				for col := range g.Width {
					c := g.Center(col, row)
					vals[(s*g.Height+row)*g.Width+col] = float32(field(v, from+s, c.X, c.Y))
				}
			}
		}
		out[v] = vals
	}
	return out
}

func weatherParcels() []Parcel {
	var ps []Parcel
	for i, c := range []Point{{11.3, 58.2}, {12.05, 59.1}, {14.7, 56.6}, {15.2, 57.77}, {10.9, 59.4}, {13.33, 57.01}} {
		ps = append(ps, Parcel{ID: int64(500 + i), Rings: [][]Point{{
			{c.X - 0.01, c.Y - 0.01}, {c.X + 0.01, c.Y - 0.01}, {c.X + 0.01, c.Y + 0.01}, {c.X - 0.01, c.Y + 0.01},
		}}})
	}
	return ps
}

func appendWeather(t *testing.T, w *Weather, pieces ...int) int {
	t.Helper()
	n := 0
	for _, k := range pieces {
		if err := w.Append(context.Background(), k, weatherSteps(n, k)); err != nil {
			t.Fatal(err)
		}
		n += k
	}
	return n
}

func checkParcelWeather(t *testing.T, db *DB, steps int) {
	t.Helper()
	ctx := context.Background()
	for _, p := range weatherParcels() {
		c := ringsBBox(p.Rings).center()
		for _, v := range []string{"wind", "ghi"} {
			got, err := db.ParcelWeather(ctx, "era5", v, p.ID, weatherStart, weatherStart.Add(1000*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != steps {
				t.Fatalf("parcel %d %s: %d samples, want %d", p.ID, v, len(got), steps)
			}
			for s, x := range got {
				if want := field(v, s, c.X, c.Y); math.Abs(float64(x.Value)-want) > 1e-3*math.Max(1, math.Abs(want)) {
					t.Fatalf("parcel %d %s step %d = %g, want %g", p.ID, v, s, x.Value, want)
				}
				if want := weatherStart.Add(time.Duration(s) * time.Hour); !x.Time.Equal(want) {
					t.Fatalf("step %d at %v, want %v", s, x.Time, want)
				}
			}
		}
	}
}

func TestParcelWeatherIsTheFieldAtEachParcel(t *testing.T) {
	ctx := context.Background()

	// Parcels first: the samples come from the steps in memory.
	db := newDB(t, zarr.NewMemoryStore())
	if err := db.PutParcels(ctx, weatherParcels()); err != nil {
		t.Fatal(err)
	}
	w, err := db.CreateWeather(ctx, "era5", weatherOptions())
	if err != nil {
		t.Fatal(err)
	}
	n := appendWeather(t, w, 10, 30, 7)
	checkParcelWeather(t, db, n)

	// Steps first: the samples are read back a chunk at a time.
	db2 := newDB(t, zarr.NewMemoryStore())
	w2, err := db2.CreateWeather(ctx, "era5", weatherOptions())
	if err != nil {
		t.Fatal(err)
	}
	appendWeather(t, w2, 25, 22)
	if err := db2.PutParcels(ctx, weatherParcels()); err != nil {
		t.Fatal(err)
	}
	checkParcelWeather(t, db2, n)
	if s := db2.WeatherSource("era5").state.Load(); s.pwSteps != n {
		t.Errorf("%d steps sampled, want %d", s.pwSteps, n)
	}

	// A range of steps.
	got, err := db.ParcelWeather(ctx, "era5", "wind", 501, weatherStart.Add(5*time.Hour), weatherStart.Add(9*time.Hour+30*time.Minute))
	if err != nil || len(got) != 5 || !got[0].Time.Equal(weatherStart.Add(5*time.Hour)) {
		t.Errorf("steps 5 to 9: %v, %v", got, err)
	}
	if got, _ := db.ParcelWeather(ctx, "era5", "wind", 501, weatherStart.Add(-time.Hour), weatherStart.Add(-time.Minute)); len(got) != 0 {
		t.Errorf("before the first step: %v", got)
	}

	for _, c := range []struct {
		source, v string
		id        int64
	}{{"gfs", "wind", 501}, {"era5", "rain", 501}, {"era5", "wind", 9}} {
		if _, err := db.ParcelWeather(ctx, c.source, c.v, c.id, weatherStart, weatherStart); !errors.Is(err, ErrNotFound) {
			t.Errorf("%+v: %v", c, err)
		}
	}
}

func TestASeriesAtAPointIsInterpolated(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	w, err := db.CreateWeather(ctx, "era5", weatherOptions())
	if err != nil {
		t.Fatal(err)
	}
	n := appendWeather(t, w, 48)
	if w.Steps() != n {
		t.Fatalf("%d steps", w.Steps())
	}
	got, err := w.Series(ctx, "ghi", 13.01, 57.49, weatherStart, weatherStart.Add(time.Duration(n)*time.Hour))
	if err != nil || len(got) != n {
		t.Fatalf("%d samples: %v", len(got), err)
	}
	for s, x := range got {
		if want := field("ghi", s, 13.01, 57.49); math.Abs(float64(x.Value)-want) > 1e-2 {
			t.Fatalf("step %d = %g, want %g", s, x.Value, want)
		}
	}
	// Past the outer centres the edge cells are held.
	edge, err := w.Series(ctx, "wind", 9, 61, weatherStart, weatherStart)
	if err != nil || len(edge) != 1 {
		t.Fatal(edge, err)
	}
	if c := weatherGrid.Center(0, 0); math.Abs(float64(edge[0].Value)-field("wind", 0, c.X, c.Y)) > 1e-3 {
		t.Errorf("off the north-west corner: %g", edge[0].Value)
	}
}

func TestInterpolationLeavesOutCellsWithNoValue(t *testing.T) {
	ws := [4]cellWeight{{0, 0, 0.25}, {0, 1, 0.25}, {1, 0, 0.25}, {1, 1, 0.25}}
	vals := map[[2]int]float32{{0, 0}: 1, {0, 1}: float32(math.NaN()), {1, 0}: 3, {1, 1}: 5}
	if got := interpolate(ws, func(r, c int) float32 { return vals[[2]int{r, c}] }); got != 3 {
		t.Errorf("interpolate = %g, want 3", got)
	}
	if got := interpolate(ws, func(int, int) float32 { return float32(math.NaN()) }); got == got {
		t.Errorf("interpolate of nothing = %g", got)
	}
}

func TestWeatherAppendsThatCannotBe(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	w, err := db.CreateWeather(ctx, "era5", weatherOptions())
	if err != nil {
		t.Fatal(err)
	}
	good := weatherSteps(0, 2)
	for name, vals := range map[string]map[string][]float32{
		"short":           {"wind": good["wind"][1:], "ghi": good["ghi"]},
		"missing a value": {"wind": good["wind"]},
		"one too many":    {"wind": good["wind"], "ghi": good["ghi"], "rain": good["ghi"]},
	} {
		if err := w.Append(ctx, 2, vals); err == nil {
			t.Errorf("an append %s was taken", name)
		}
	}
	if err := w.Append(ctx, 0, nil); err == nil {
		t.Error("an append of no steps was taken")
	}
	for name, o := range map[string]func(*WeatherOptions){
		"in UTM":          func(o *WeatherOptions) { o.Grid.CRS = "EPSG:32633" },
		"a step of 1.5 s": func(o *WeatherOptions) { o.Step = 1500 * time.Millisecond },
		"no variables":    func(o *WeatherOptions) { o.Variables = nil },
		"a variable twice": func(o *WeatherOptions) {
			o.Variables = []string{"wind", "wind"}
		},
	} {
		wo := weatherOptions()
		o(&wo)
		if _, err := db.CreateWeather(ctx, "x", wo); err == nil {
			t.Errorf("a source %s was made", name)
		}
	}
	if _, err := db.CreateWeather(ctx, "era5", weatherOptions()); !errors.Is(err, zarr.ErrExists) {
		t.Errorf("a second era5: %v", err)
	}
}

func TestStepRange(t *testing.T) {
	m := weatherMeta{Start: 1000, Step: 10}
	at := func(s int64) time.Time { return time.Unix(s, 0) }
	for _, c := range []struct {
		from, to int64
		n        int
		s0, s1   int
	}{
		{1000, 1090, 100, 0, 10},
		{1001, 1090, 100, 1, 10},
		{1000, 1089, 100, 0, 9},
		{0, 5000, 20, 0, 20},
		{2000, 3000, 20, 20, 20},
		{1095, 1091, 100, 10, 10},
	} {
		if s0, s1 := m.stepRange(at(c.from), at(c.to), c.n); s0 != c.s0 || s1 != c.s1 {
			t.Errorf("stepRange(%d, %d, %d) = %d, %d; want %d, %d", c.from, c.to, c.n, s0, s1, c.s0, c.s1)
		}
	}
}
