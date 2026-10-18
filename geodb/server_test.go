package geodb

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/LukasSelin/zarr"
)

func TestTheHandlerServesTheDB(t *testing.T) {
	ctx := context.Background()
	db := newDB(t, NewCachedStore(zarr.NewMemoryStore(), CacheOptions{}))
	parcels := append(testParcels(testGrid), weatherParcels()...)
	if err := db.PutParcels(ctx, parcels); err != nil {
		t.Fatal(err)
	}
	tile, err := db.CreateTile(ctx, "33UUB", testTileOptions(testGrid))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		sc := testScene(testGrid, i)
		if i == 1 { // every pixel under cloud
			for _, v := range sc.Values {
				for k := range v {
					v[k] = float32(math.NaN())
				}
			}
		}
		if _, err := tile.AppendScene(ctx, sc); err != nil {
			t.Fatal(err)
		}
	}
	if err := tile.UpdateComposites(ctx, origin, origin.Add(30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	w, err := db.CreateWeather(ctx, "era5", weatherOptions())
	if err != nil {
		t.Fatal(err)
	}
	appendWeather(t, w, 30)

	srv := httptest.NewServer(NewHandler(db))
	defer srv.Close()
	get := func(path string, wantCode int, v any) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+path, nil)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != wantCode {
			t.Fatalf("GET %s: %d %s, want %d", path, resp.StatusCode, body, wantCode)
		}
		if v != nil {
			if err := json.Unmarshal(body, v); err != nil {
				t.Fatalf("GET %s: %v in %s", path, err, body)
			}
		}
	}

	var tiles []struct {
		Name    string `json:"name"`
		Scenes  int    `json:"scenes"`
		Parcels int    `json:"parcels"`
	}
	get("/v1/tiles", 200, &tiles)
	if len(tiles) != 1 || tiles[0].Name != "33UUB" || tiles[0].Scenes != 3 || tiles[0].Parcels != 8 {
		t.Errorf("tiles: %+v", tiles)
	}
	var sources []struct {
		Name    string `json:"name"`
		Steps   int    `json:"steps"`
		Sampled int    `json:"sampled_at_parcels"`
	}
	get("/v1/weather", 200, &sources)
	if len(sources) != 1 || sources[0].Steps != 30 || sources[0].Sampled != 30 {
		t.Errorf("weather: %+v", sources)
	}

	var ids struct{ IDs []int64 }
	get("/v1/parcels?bbox=10,56,16,60", 200, &ids)
	if len(ids.IDs) != 6 {
		t.Errorf("parcels in the weather grid: %v", ids.IDs)
	}
	get("/v1/parcels?bbox=1,2,3", 400, nil)
	get("/v1/parcels/at?lon=11.3&lat=58.2", 200, &ids)
	if len(ids.IDs) != 1 || ids.IDs[0] != 500 {
		t.Errorf("parcels at a point: %v", ids.IDs)
	}
	var parcel struct {
		ID    int64          `json:"id"`
		Rings [][][2]float64 `json:"rings"`
	}
	get("/v1/parcels/102", 200, &parcel)
	if parcel.ID != 102 || len(parcel.Rings) != 2 || len(parcel.Rings[0]) != 4 {
		t.Errorf("parcel 102: %+v", parcel)
	}
	get("/v1/parcels/7", 404, nil)
	get("/v1/parcels/x", 400, nil)

	var obs []map[string]any
	get("/v1/parcels/101/series?var=ndvi", 200, &obs)
	if len(obs) != 3 {
		t.Fatalf("series: %v", obs)
	}
	if obs[1]["mean"] != nil || obs[1]["valid_fraction"] != 0.0 {
		t.Errorf("the scene under cloud: %v", obs[1])
	}
	if m, ok := obs[0]["mean"].(float64); !ok || !(m > 0) {
		t.Errorf("the first scene: %v", obs[0])
	}
	get("/v1/parcels/101/series?var=ndvi&min_valid=0.5", 200, &obs)
	if len(obs) != 2 {
		t.Errorf("series with min_valid: %d", len(obs))
	}
	get("/v1/parcels/101/series", 400, nil)
	get("/v1/parcels/101/series?var=lai", 404, nil)

	var samples []struct {
		Time  time.Time `json:"time"`
		Value *float64  `json:"value"`
	}
	q := url.Values{"var": {"wind"}, "from": {"2024-06-01T05:00:00Z"}, "to": {"2024-06-01T09:00:00Z"}}
	get("/v1/parcels/500/weather/era5?"+q.Encode(), 200, &samples)
	if len(samples) != 5 || samples[0].Value == nil {
		t.Errorf("parcel weather: %+v", samples)
	}
	get("/v1/parcels/500/weather/gfs?var=wind", 404, nil)
	get("/v1/parcels/500/weather/era5?var=wind&from=yesterday", 400, nil)
	get("/v1/weather/era5/series?var=ghi&lon=12&lat=58", 200, &samples)
	if len(samples) != 30 {
		t.Errorf("weather at a point: %d", len(samples))
	}

	lon, lat := inverse(mustProj(t, testGrid.CRS), testGrid.X0+405, testGrid.Y0-405, 13.1, 55.65)
	pt := url.Values{"var": {"ndvi"}, "lon": {ftoa(lon)}, "lat": {ftoa(lat)}}
	get("/v1/tiles/33UUB/history?"+pt.Encode(), 200, &samples)
	if len(samples) != 1 { // three scenes in the first ten days
		t.Errorf("composite history: %+v", samples)
	}
	pt.Set("scenes", "1")
	get("/v1/tiles/33UUB/history?"+pt.Encode(), 200, &samples)
	if len(samples) != 3 || samples[1].Value != nil {
		t.Errorf("scene history: %+v", samples)
	}
	get("/v1/tiles/33UUB/history?var=ndvi&lon=0&lat=0", 400, nil)
	get("/v1/tiles/31UFT/history?var=ndvi&lon=0&lat=0", 404, nil)

	var stats CacheStats
	get("/v1/cache", 200, &stats)
	if stats.Hits == 0 {
		t.Errorf("cache: %+v", stats)
	}
	get("/v1/nothing", 404, nil)
}

func mustProj(t *testing.T, crs string) Projection {
	p, err := projectionTo(crs)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func ftoa(f float64) string {
	b, _ := json.Marshal(f)
	return strings.TrimSpace(string(b))
}
