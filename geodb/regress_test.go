package geodb

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LukasSelin/zarr"
)

// A reader through a cache sees, after a Refresh, what was rewritten in
// place since: the chunk of scene times, statistics, weather steps.
func TestARefreshThroughACacheReadsWhatChanged(t *testing.T) {
	ctx := context.Background()
	s := zarr.NewMemoryStore()
	writer := newDB(t, s)
	if err := writer.PutParcels(ctx, append(testParcels(testGrid), weatherParcels()...)); err != nil {
		t.Fatal(err)
	}
	tile, _ := writer.CreateTile(ctx, "t", testTileOptions(testGrid))
	w, _ := writer.CreateWeather(ctx, "era5", weatherOptions())
	scenes := []Scene{testScene(testGrid, 0), testScene(testGrid, 1)}
	if _, err := tile.AppendScene(ctx, scenes[0]); err != nil {
		t.Fatal(err)
	}
	appendWeather(t, w, 5)

	reader, err := Open(ctx, NewCachedStore(s, CacheOptions{Metadata: true}))
	if err != nil {
		t.Fatal(err)
	}
	if obs, err := reader.ParcelSeries(ctx, 101, "ndvi"); err != nil || len(obs) != 1 {
		t.Fatal(obs, err)
	}
	if ss, err := reader.ParcelWeather(ctx, "era5", "wind", 500, weatherStart, weatherStart.Add(99*time.Hour)); err != nil || len(ss) != 5 {
		t.Fatal(ss, err)
	}

	if _, err := tile.AppendScene(ctx, scenes[1]); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ctx, 3, weatherSteps(5, 3)); err != nil {
		t.Fatal(err)
	}
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	times := reader.Tile("t").Times()
	if len(times) != 2 || !times[1].Equal(scenes[1].Time) {
		t.Errorf("after a refresh the times are %v", times)
	}
	checkSeries(t, reader, testGrid, scenes)
	ss, err := reader.ParcelWeather(ctx, "era5", "wind", 500, weatherStart, weatherStart.Add(99*time.Hour))
	if err != nil || len(ss) != 8 || ss[7].Value != ss[7].Value {
		t.Errorf("after a refresh the weather at a parcel is %v, %v", ss, err)
	}
}

// A reader that has not refreshed since another process put new parcels
// goes on reading what was made for the parcels it has, not another
// parcel's rows; once it refreshes it has the new parcels.
func TestAReaderOfTheParcelsBeforeReadsWhatWasMadeForThem(t *testing.T) {
	ctx := context.Background()
	s := zarr.NewMemoryStore()
	writer := newDB(t, s)
	ps := testParcels(testGrid)
	if err := writer.PutParcels(ctx, ps); err != nil {
		t.Fatal(err)
	}
	tile, _ := writer.CreateTile(ctx, "t", testTileOptions(testGrid))
	scenes := []Scene{testScene(testGrid, 0), testScene(testGrid, 1)}
	for _, sc := range scenes {
		if _, err := tile.AppendScene(ctx, sc); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := Open(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	before, err := reader.ParcelSeries(ctx, 101, "ndvi")
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.PutParcels(ctx, ps[1:]); err != nil { // without 101
		t.Fatal(err)
	}
	after, err := reader.ParcelSeries(ctx, 101, "ndvi")
	if err != nil || len(after) != len(before) {
		t.Fatalf("a reader before the refresh: %v, %v", after, err)
	}
	for i := range after {
		if after[i].Mean != before[i].Mean || after[i].Pixels != before[i].Pixels {
			t.Errorf("scene %d: the reader read %+v, which was %+v", i, after[i], before[i])
		}
	}
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ParcelSeries(ctx, 101, "ndvi"); !errors.Is(err, ErrNotFound) {
		t.Errorf("after the refresh, parcel 101: %v", err)
	}

	// A third PutParcels takes away the first generation, and keeps the second.
	if err := writer.PutParcels(ctx, ps); err != nil {
		t.Fatal(err)
	}
	var gens []string
	_ = zarr.ListDir(ctx, s, "tiles/t/stats/", func(n string) error {
		if strings.HasPrefix(n, "g") {
			gens = append(gens, n)
		}
		return nil
	})
	if len(gens) != 2 {
		t.Errorf("generations of statistics kept: %v", gens)
	}
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	checkSeries(t, reader, testGrid, scenes)
}

// A PutParcels that stops before it has indexed a tile is finished by the
// next append, or by BackfillStats.
func TestATileLeftUnindexedIsIndexedByTheNextWrite(t *testing.T) {
	ctx := context.Background()
	for _, repair := range []string{"append", "backfill"} {
		t.Run(repair, func(t *testing.T) {
			fs := &failing{Store: zarr.NewMemoryStore(), prefix: "tiles/t/stats/"}
			db := newDB(t, fs)
			tile, _ := db.CreateTile(ctx, "t", testTileOptions(testGrid))
			scenes := []Scene{testScene(testGrid, 0), testScene(testGrid, 1), testScene(testGrid, 2)}
			for _, sc := range scenes[:2] {
				if _, err := tile.AppendScene(ctx, sc); err != nil {
					t.Fatal(err)
				}
			}
			fs.armed.Store(true)
			if err := db.PutParcels(ctx, testParcels(testGrid)); !errors.Is(err, errInjected) {
				t.Fatalf("the PutParcels that should fail: %v", err)
			}
			fs.armed.Store(false)
			db, err := Open(ctx, fs)
			if err != nil {
				t.Fatal(err)
			}
			tile = db.Tile("t")
			if repair == "append" {
				if _, err := tile.AppendScene(ctx, scenes[2]); err != nil {
					t.Fatal(err)
				}
			} else {
				scenes = scenes[:2]
				if err := tile.BackfillStats(ctx); err != nil {
					t.Fatal(err)
				}
			}
			checkSeries(t, db, testGrid, scenes)
		})
	}
}

// A Get that begins after a Set has returned does not join a read of the
// value from before the Set.
func TestAGetAfterASetDoesNotJoinAReadFromBefore(t *testing.T) {
	ctx := context.Background()
	inner := &counting{Store: zarr.NewMemoryStore(), hold: make(chan struct{})}
	_ = inner.Store.Set(ctx, "k", []byte("old"))
	c := NewCachedStore(inner, CacheOptions{})
	first := make(chan []byte)
	go func() {
		b, _ := c.Get(ctx, "k")
		first <- b
	}()
	for inner.gets.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if err := c.Set(ctx, "k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	second := make(chan []byte)
	go func() {
		b, _ := c.Get(ctx, "k")
		second <- b
	}()
	time.Sleep(10 * time.Millisecond)
	close(inner.hold)
	<-first
	if b := <-second; string(b) != "new" {
		t.Errorf("a Get after the Set read %q", b)
	}
	if b, _ := c.Get(ctx, "k"); string(b) != "new" {
		t.Errorf("the cache keeps %q", b)
	}
}

// Tiles made while the DB is refreshed are one tile, not two.
func TestATileMadeDuringARefreshIsTheOneInTheDB(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := db.Refresh(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	for _, name := range []string{"a", "b", "c", "d"} {
		tile, err := db.CreateTile(ctx, name, testTileOptions(testGrid))
		if err != nil {
			t.Fatal(err)
		}
		if db.Tile(name) != tile {
			t.Errorf("tile %s in the DB is not the one made", name)
		}
	}
	close(stop)
	wg.Wait()
}

// A parcel off a weather source's grid has no samples, not those of the
// nearest edge.
func TestAParcelOffTheWeatherGridHasNoSamples(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	off := Parcel{ID: 1, Rings: [][]Point{{{30, 30}, {30.1, 30}, {30.1, 30.1}}}}
	if err := db.PutParcels(ctx, append(weatherParcels(), off)); err != nil {
		t.Fatal(err)
	}
	w, _ := db.CreateWeather(ctx, "era5", weatherOptions())
	appendWeather(t, w, 3)
	ss, err := db.ParcelWeather(ctx, "era5", "wind", 1, weatherStart, weatherStart.Add(9*time.Hour))
	if err != nil || len(ss) != 3 {
		t.Fatal(ss, err)
	}
	for _, x := range ss {
		if !math.IsNaN(float64(x.Value)) {
			t.Errorf("a parcel off the grid has the sample %v", x)
		}
	}
	checkParcelWeather(t, db, 3)
}

// Composites made for later periods first leave none out before them.
func TestCompositesLeaveNoPeriodOut(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, zarr.NewMemoryStore())
	o := testTileOptions(testGrid)
	o.CompositeTime = 2
	tile, _ := db.CreateTile(ctx, "t", o)
	for i := range 7 {
		if _, err := tile.AppendScene(ctx, testScene(testGrid, i)); err != nil {
			t.Fatal(err)
		}
	}
	// Only the last scene's period was asked for; the three before it are
	// made too.
	last := tile.Times()[6]
	if err := tile.UpdateComposites(ctx, last, last); err != nil {
		t.Fatal(err)
	}
	hist, err := tile.CompositeHistory(ctx, "ndvi", 100, 80)
	if err != nil || len(hist) != 4 {
		t.Fatal(hist, err)
	}
	for p, s := range hist[:3] {
		if s.Value != s.Value {
			t.Errorf("period %d was left out", p)
		}
	}
}

func TestOnlyChunksOfScenesAndParcelsAreWrittenOnce(t *testing.T) {
	for key, want := range map[string]bool{
		"tiles/t/scenes/ndvi/c/3/0/1":       true,
		"tiles/t/scenes/ndvi/zarr.json":     false,
		"tiles/t/scenes/time/c/0":           false,
		"tiles/t/composites/ndvi/c/0/0/0":   false,
		"tiles/t/stats/g5/ndvi/c/0/0/0":     false,
		"parcels/g5/coords/c/0/0":           true,
		"parcels/g5/zarr.json":              false,
		"parcels/zarr.json":                 false,
		"weather/era5/wind/c/0/0/0":         false,
		"parcel_weather/era5/g5/wind/c/0/0": false,
		strings.Repeat("tiles/", 3) + "c/0": false,
	} {
		if got := writtenOnce(key); got != want {
			t.Errorf("writtenOnce(%q) = %v", key, got)
		}
	}
}
