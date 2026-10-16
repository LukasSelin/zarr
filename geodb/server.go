package geodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// NewHandler serves the DB over HTTP as JSON. Every route is a GET:
//
//	/v1/tiles                                 the tiles, their grids and scenes
//	/v1/weather                               the weather sources
//	/v1/parcels?bbox=W,S,E,N                  IDs of the parcels whose boxes meet a box
//	/v1/parcels/at?lon=&lat=                  IDs of the parcels a point is in
//	/v1/parcels/{id}                          a parcel's rings
//	/v1/parcels/{id}/series?var=&min_valid=   a parcel's statistics in every scene
//	/v1/parcels/{id}/weather/{source}?var=&from=&to=
//	                                          a weather source's samples at a parcel
//	/v1/tiles/{tile}/history?var=&lon=&lat=&scenes=1
//	                                          a pixel's composites, or with scenes=1 every scene's value
//	/v1/weather/{source}/series?var=&lon=&lat=&from=&to=
//	                                          a weather source at a point
//	/v1/cache                                 how the cache has done, if the store is a CachedStore
//
// Times are RFC 3339, and a value that is not there - NaN - is null.
func NewHandler(db *DB) http.Handler {
	h := &handler{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tiles", h.tiles)
	mux.HandleFunc("GET /v1/weather", h.weatherSources)
	mux.HandleFunc("GET /v1/parcels", h.parcelsIn)
	mux.HandleFunc("GET /v1/parcels/at", h.parcelsAt)
	mux.HandleFunc("GET /v1/parcels/{id}", h.parcel)
	mux.HandleFunc("GET /v1/parcels/{id}/series", h.parcelSeries)
	mux.HandleFunc("GET /v1/parcels/{id}/weather/{source}", h.parcelWeather)
	mux.HandleFunc("GET /v1/tiles/{tile}/history", h.history)
	mux.HandleFunc("GET /v1/weather/{source}/series", h.weatherSeries)
	mux.HandleFunc("GET /v1/cache", h.cache)
	return mux
}

type handler struct{ db *DB }

// maxParcels is the most IDs a box answers with.
const maxParcels = 100_000

// num is a float32 that is null in JSON when it is NaN.
type num float32

func (n num) MarshalJSON() ([]byte, error) {
	f := float64(n)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return []byte("null"), nil
	}
	return strconv.AppendFloat(nil, f, 'g', -1, 32), nil
}

type badRequest struct{ error }

func bad(format string, args ...any) error { return badRequest{fmt.Errorf(format, args...)} }

func reply(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		code := http.StatusInternalServerError
		var br badRequest
		switch {
		case errors.As(err, &br):
			code = http.StatusBadRequest
		case errors.Is(err, ErrNotFound):
			code = http.StatusNotFound
		case errors.Is(err, context.Canceled):
			code = 499 // the client went away
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func floats(r *http.Request, names ...string) ([]float64, error) {
	out := make([]float64, len(names))
	for i, n := range names {
		s := r.URL.Query().Get(n)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || !finite(f) {
			return nil, bad("%s=%q is not a number", n, s)
		}
		out[i] = f
	}
	return out, nil
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		return 0, bad("parcel %q is not an integer", r.PathValue("id"))
	}
	return id, nil
}

// span is the from and to of a request, all of time where they are left out.
func timeSpan(r *http.Request) (time.Time, time.Time, error) {
	from, to := time.Unix(math.MinInt32, 0), time.Unix(math.MaxInt32*4, 0)
	for _, p := range []struct {
		name string
		t    *time.Time
	}{{"from", &from}, {"to", &to}} {
		if s := r.URL.Query().Get(p.name); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return from, to, bad("%s=%q is not an RFC 3339 time", p.name, s)
			}
			*p.t = t
		}
	}
	return from, to, nil
}

func variable(r *http.Request) (string, error) {
	v := r.URL.Query().Get("var")
	if v == "" {
		return "", bad("say which variable with var=")
	}
	return v, nil
}

type jsonSample struct {
	Time  time.Time `json:"time"`
	Value num       `json:"value"`
}

func samples(ss []Sample) []jsonSample {
	out := make([]jsonSample, len(ss))
	for i, s := range ss {
		out[i] = jsonSample{s.Time, num(s.Value)}
	}
	return out
}

func (h *handler) tiles(w http.ResponseWriter, _ *http.Request) {
	type tile struct {
		Name      string     `json:"name"`
		Grid      Grid       `json:"grid"`
		Variables []Variable `json:"variables"`
		Scenes    int        `json:"scenes"`
		First     *time.Time `json:"first,omitempty"`
		Last      *time.Time `json:"last,omitempty"`
		Parcels   int        `json:"parcels"`
	}
	out := []tile{}
	for _, t := range h.db.Tiles() {
		st := t.state.Load()
		x := tile{Name: t.name, Grid: st.meta.Grid, Variables: st.meta.Variables, Scenes: len(st.times), Parcels: st.parcels.len()}
		if n := len(st.times); n > 0 {
			f, l := time.Unix(st.times[0], 0).UTC(), time.Unix(st.times[n-1], 0).UTC()
			x.First, x.Last = &f, &l
		}
		out = append(out, x)
	}
	reply(w, out, nil)
}

func (h *handler) weatherSources(w http.ResponseWriter, _ *http.Request) {
	type source struct {
		Name      string    `json:"name"`
		Grid      Grid      `json:"grid"`
		Variables []string  `json:"variables"`
		Start     time.Time `json:"start"`
		Step      string    `json:"step"`
		Steps     int       `json:"steps"`
		Sampled   int       `json:"sampled_at_parcels"`
	}
	out := []source{}
	for _, ws := range h.db.WeatherSources() {
		st := ws.state.Load()
		out = append(out, source{
			Name: ws.name, Grid: st.meta.Grid, Variables: st.meta.Variables,
			Start: time.Unix(st.meta.Start, 0).UTC(), Step: (time.Duration(st.meta.Step) * time.Second).String(),
			Steps: st.steps, Sampled: st.pwSteps,
		})
	}
	reply(w, out, nil)
}

func (h *handler) parcelsIn(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Query().Get("bbox"), ",")
	if len(parts) != 4 {
		reply(w, nil, bad("bbox is west,south,east,north"))
		return
	}
	var b [4]float64
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil || !finite(f) {
			reply(w, nil, bad("bbox is west,south,east,north"))
			return
		}
		b[i] = f
	}
	ids := h.db.ParcelsIn(BBox{b[0], b[1], b[2], b[3]})
	if len(ids) > maxParcels {
		reply(w, nil, bad("the box holds %d parcels, more than %d: ask for a smaller one", len(ids), maxParcels))
		return
	}
	if ids == nil {
		ids = []int64{}
	}
	reply(w, map[string]any{"ids": ids}, nil)
}

func (h *handler) parcelsAt(w http.ResponseWriter, r *http.Request) {
	ll, err := floats(r, "lon", "lat")
	if err != nil {
		reply(w, nil, err)
		return
	}
	ids := h.db.ParcelAt(Point{ll[0], ll[1]})
	if ids == nil {
		ids = []int64{}
	}
	reply(w, map[string]any{"ids": ids}, nil)
}

func (h *handler) parcel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	p, ok := h.db.GetParcel(id)
	if !ok {
		reply(w, nil, fmt.Errorf("%w: parcel %d", ErrNotFound, id))
		return
	}
	rings := make([][][2]float64, len(p.Rings))
	for i, ring := range p.Rings {
		rings[i] = make([][2]float64, len(ring))
		for k, pt := range ring {
			rings[i][k] = [2]float64{pt.X, pt.Y}
		}
	}
	reply(w, map[string]any{"id": id, "rings": rings}, nil)
}

func (h *handler) parcelSeries(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	v, err := variable(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	minValid := 0.0
	if s := r.URL.Query().Get("min_valid"); s != "" {
		if minValid, err = strconv.ParseFloat(s, 64); err != nil || !(minValid >= 0 && minValid <= 1) {
			reply(w, nil, bad("min_valid=%q is not a fraction", s))
			return
		}
	}
	obs, err := h.db.ParcelSeries(r.Context(), id, v)
	if err != nil {
		reply(w, nil, err)
		return
	}
	type jsonObs struct {
		Time          time.Time `json:"time"`
		Tile          string    `json:"tile"`
		Pixels        int       `json:"pixels"`
		ValidFraction num       `json:"valid_fraction"`
		Mean          num       `json:"mean"`
		Std           num       `json:"std"`
		P10           num       `json:"p10"`
		P50           num       `json:"p50"`
		P90           num       `json:"p90"`
	}
	out := []jsonObs{}
	for _, o := range obs {
		if minValid > 0 && !(float64(o.ValidFraction) >= minValid) {
			continue
		}
		out = append(out, jsonObs{o.Time, o.Tile, o.Pixels, num(o.ValidFraction), num(o.Mean), num(o.Std), num(o.P10), num(o.P50), num(o.P90)})
	}
	reply(w, out, nil)
}

func (h *handler) parcelWeather(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	v, err := variable(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	from, to, err := timeSpan(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	ss, err := h.db.ParcelWeather(r.Context(), r.PathValue("source"), v, id, from, to)
	reply(w, samples(ss), err)
}

func (h *handler) history(w http.ResponseWriter, r *http.Request) {
	t := h.db.Tile(r.PathValue("tile"))
	if t == nil {
		reply(w, nil, fmt.Errorf("%w: tile %s", ErrNotFound, r.PathValue("tile")))
		return
	}
	v, err := variable(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	ll, err := floats(r, "lon", "lat")
	if err != nil {
		reply(w, nil, err)
		return
	}
	col, row, ok := t.Pixel(ll[0], ll[1])
	if !ok {
		reply(w, nil, bad("(%g, %g) is not in tile %s", ll[0], ll[1], t.name))
		return
	}
	var ss []Sample
	if r.URL.Query().Get("scenes") == "1" {
		ss, err = t.PixelHistory(r.Context(), v, col, row)
	} else {
		ss, err = t.CompositeHistory(r.Context(), v, col, row)
	}
	reply(w, samples(ss), err)
}

func (h *handler) weatherSeries(w http.ResponseWriter, r *http.Request) {
	ws := h.db.WeatherSource(r.PathValue("source"))
	if ws == nil {
		reply(w, nil, fmt.Errorf("%w: weather source %s", ErrNotFound, r.PathValue("source")))
		return
	}
	v, err := variable(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	ll, err := floats(r, "lon", "lat")
	if err != nil {
		reply(w, nil, err)
		return
	}
	from, to, err := timeSpan(r)
	if err != nil {
		reply(w, nil, err)
		return
	}
	ss, err := ws.Series(r.Context(), v, ll[0], ll[1], from, to)
	reply(w, samples(ss), err)
}

func (h *handler) cache(w http.ResponseWriter, _ *http.Request) {
	c, ok := h.db.store.(*CachedStore)
	if !ok {
		reply(w, nil, fmt.Errorf("%w: cache, as the store is not a CachedStore", ErrNotFound))
		return
	}
	reply(w, c.Stats(), nil)
}
