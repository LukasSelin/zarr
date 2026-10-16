package geodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LukasSelin/zarr"
)

// WeatherOptions sets up a weather source.
type WeatherOptions struct {
	// Grid is in longitude and latitude: its CRS is EPSG:4326.
	Grid Grid
	// Start is the time of the first step, and Step the time between steps.
	Start time.Time
	Step  time.Duration
	// Variables are what the source has: wind speed at hub height, global
	// horizontal irradiance, and so on.
	Variables []string
	// ChunkSteps and ChunkSide are the chunks of the source: ChunkSteps
	// deep, 720 by default (thirty days of hours), and ChunkSide square, 32
	// by default. Append a chunk's steps at a time where you can: an append
	// of fewer rewrites the chunks it lands in.
	ChunkSteps int
	ChunkSide  int
	// ParcelChunk is how many parcels a chunk of the parcel samples holds,
	// 256 by default; they are ChunkSteps deep.
	ParcelChunk int
}

type weatherMeta struct {
	Grid        Grid     `json:"grid"`
	Start       int64    `json:"start"` // Unix seconds
	Step        int64    `json:"step"`  // seconds
	Variables   []string `json:"variables"`
	ChunkSteps  int      `json:"chunk_steps"`
	ChunkSide   int      `json:"chunk_side"`
	ParcelChunk int      `json:"parcel_chunk"`
}

// Weather is a gridded source of regular series in longitude and latitude,
// with samples of it at every parcel.
type Weather struct {
	db   *DB
	name string
	path string

	state    atomic.Pointer[weatherState]
	writeMu  sync.Mutex
	reloadMu sync.Mutex // one reload at a time, so that none undoes a later one
}

type weatherState struct {
	meta   weatherMeta
	steps  int
	arrays map[string]*zarr.Array
	// The samples at the parcels, if there are parcels and they have been
	// set up for the parcels there are now.
	pwSteps int
	pwGen   int64
	pwSet   *parcelSet // the parcels the samples are of
	pw      map[string]*zarr.Array
}

// pwPath is where the samples at a generation of parcels are.
func (w *Weather) pwPath(gen int64) string {
	return joinPath("parcel_weather", w.name, genName(gen))
}

// CreateWeather makes a weather source.
func (db *DB) CreateWeather(ctx context.Context, name string, o WeatherOptions) (*Weather, error) {
	if err := validName("weather source", name); err != nil {
		return nil, err
	}
	m := weatherMeta{
		Grid: o.Grid, Start: o.Start.Unix(), Step: int64(o.Step / time.Second),
		Variables: slices.Clone(o.Variables), ChunkSteps: o.ChunkSteps, ChunkSide: o.ChunkSide, ParcelChunk: o.ParcelChunk,
	}
	if m.ChunkSteps == 0 {
		m.ChunkSteps = 720
	}
	if m.ChunkSide == 0 {
		m.ChunkSide = 32
	}
	if m.ParcelChunk == 0 {
		m.ParcelChunk = 256
	}
	switch c := strings.ToUpper(m.Grid.CRS); {
	case c != "EPSG:4326" && c != "OGC:CRS84":
		return nil, fmt.Errorf("geodb: a weather source is in EPSG:4326, not %s", m.Grid.CRS)
	case m.Step <= 0 || o.Step%time.Second != 0:
		return nil, errors.New("geodb: the step of a weather source is a whole number of seconds")
	case len(m.Variables) == 0:
		return nil, errors.New("geodb: a weather source needs a variable")
	case m.ChunkSteps <= 0 || m.ChunkSide <= 0 || m.ParcelChunk <= 0:
		return nil, errors.New("geodb: chunk sizes must be positive")
	}
	if err := m.Grid.validate(); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, v := range m.Variables {
		if err := validName("variable", v); err != nil {
			return nil, err
		}
		if seen[v] {
			return nil, fmt.Errorf("geodb: variable %s is given twice", v)
		}
		seen[v] = true
	}

	db.adminMu.Lock()
	defer db.adminMu.Unlock()
	if db.WeatherSource(name) != nil {
		return nil, fmt.Errorf("geodb: weather source %s: %w", name, zarr.ErrExists)
	}
	w := &Weather{db: db, name: name, path: joinPath("weather", name)}
	g, err := zarr.CreateGroup(ctx, db.store, w.path, nil)
	if err != nil {
		return nil, err
	}
	for _, v := range m.Variables {
		if _, err := g.CreateArray(ctx, v, zarr.ArrayOptions{
			Shape:          []int{0, m.Grid.Height, m.Grid.Width},
			ChunkShape:     []int{m.ChunkSteps, min(m.ChunkSide, m.Grid.Height), min(m.ChunkSide, m.Grid.Width)},
			DataType:       zarr.Float32,
			FillValue:      float32NaN(),
			Codecs:         codecs(zarr.Float32),
			DimensionNames: []string{"time", "latitude", "longitude"},
		}); err != nil {
			return nil, err
		}
	}
	if err := g.SetAttributes(ctx, map[string]any{"geodb_weather": m, "steps": 0}); err != nil {
		return nil, err
	}
	if err := db.register(ctx, "", name); err != nil {
		return nil, err
	}
	if err := w.reload(ctx); err != nil {
		return nil, err
	}
	db.mu.Lock()
	db.weather[name] = w
	db.mu.Unlock()
	if db.parcels.Load().len() > 0 {
		if err := w.SampleParcels(ctx); err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (w *Weather) reload(ctx context.Context) error {
	w.reloadMu.Lock()
	defer w.reloadMu.Unlock()
	s := w.db.store
	g, err := zarr.OpenGroup(ctx, s, w.path)
	if err != nil {
		return err
	}
	st := &weatherState{arrays: map[string]*zarr.Array{}}
	ok, err := g.Attribute("geodb_weather", &st.meta)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("the weather source was not finished being made")
	}
	if _, err := g.Attribute("steps", &st.steps); err != nil {
		return err
	}
	for _, v := range st.meta.Variables {
		if st.arrays[v], err = g.OpenArray(ctx, v); err != nil {
			return err
		}
		st.steps = min(st.steps, st.arrays[v].Shape()[0])
	}
	set := w.db.parcels.Load()
	if set.len() == 0 {
		w.state.Store(st)
		return nil
	}
	pg, err := zarr.OpenGroup(ctx, s, w.pwPath(set.generation))
	if isNotFound(err) {
		w.state.Store(st)
		return nil
	}
	if err != nil {
		return err
	}
	var gen int64
	if ok, err = pg.Attribute("generation", &gen); err != nil {
		return err
	}
	if ok && gen == set.generation {
		if _, err := pg.Attribute("steps", &st.pwSteps); err != nil {
			return err
		}
		st.pwSteps = min(st.pwSteps, st.steps)
		st.pwGen, st.pwSet = gen, set
		st.pw = map[string]*zarr.Array{}
		for _, v := range st.meta.Variables {
			if st.pw[v], err = pg.OpenArray(ctx, v); err != nil {
				return err
			}
		}
	}
	w.state.Store(st)
	return nil
}

// Name is the source's name.
func (w *Weather) Name() string { return w.name }

// Steps is how many steps the source holds.
func (w *Weather) Steps() int { return w.state.Load().steps }

// Grid is the source's grid.
func (w *Weather) Grid() Grid { return w.state.Load().meta.Grid }

// Variables are the source's variables.
func (w *Weather) Variables() []string { return slices.Clone(w.state.Load().meta.Variables) }

// TimeOf is the time of step i.
func (w *Weather) TimeOf(i int) time.Time {
	m := w.state.Load().meta
	return time.Unix(m.Start+int64(i)*m.Step, 0).UTC()
}

// stepRange is the steps from the first at or after from to the last at or
// before to, of n there are.
func (m weatherMeta) stepRange(from, to time.Time, n int) (int, int) {
	f := float64(from.Unix()-m.Start) / float64(m.Step)
	t := float64(to.Unix()-m.Start) / float64(m.Step)
	s0 := int(math.Max(0, math.Min(float64(n), math.Ceil(f))))
	s1 := int(math.Max(0, math.Min(float64(n), math.Floor(t)+1)))
	return s0, max(s0, s1)
}

// Append adds steps after the last, steps of them, of every variable:
// values[v] is steps by Height by Width, in C order, NaN where there is no
// value. If the parcel samples are up to date, the new steps are sampled at
// every parcel too, from the values in memory.
func (w *Weather) Append(ctx context.Context, steps int, values map[string][]float32) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if err := w.reload(ctx); err != nil {
		return err
	}
	st := w.state.Load()
	g := st.meta.Grid
	if steps <= 0 {
		return errors.New("geodb: append at least one step")
	}
	for _, v := range st.meta.Variables {
		if len(values[v]) != steps*g.Width*g.Height {
			return fmt.Errorf("geodb: %d values of %s for %d steps of %d by %d", len(values[v]), v, steps, g.Width, g.Height)
		}
	}
	if len(values) != len(st.meta.Variables) {
		return fmt.Errorf("geodb: weather source %s has the variables %v, not those given", w.name, st.meta.Variables)
	}
	n := st.steps
	for _, v := range st.meta.Variables {
		a, err := zarr.OpenArray(ctx, w.db.store, st.arrays[v].Path())
		if err != nil {
			return err
		}
		if err := appendAt(ctx, a, 0, n, values[v]); err != nil {
			return err
		}
	}
	sampled := false
	if st.pw != nil && st.pwSteps == n {
		at := func(v string) (func(s, row, col int) float32, error) {
			vals := values[v]
			return func(s, row, col int) float32 { return vals[(s*g.Height+row)*g.Width+col] }, nil
		}
		if err := w.appendSamples(ctx, st, n, steps, at); err != nil {
			return err
		}
		sampled = true
	}
	g0, err := zarr.OpenGroup(ctx, w.db.store, w.path)
	if err != nil {
		return err
	}
	if err := g0.SetAttributes(ctx, map[string]any{"steps": n + steps}); err != nil {
		return err
	}
	if !sampled && w.db.parcels.Load().len() > 0 {
		// The samples are not set up for the parcels there are, or fell
		// behind: bring them up to date from the store.
		return w.sampleLocked(ctx)
	}
	return w.reload(ctx)
}

// cellWeight is one of the four cells a point is interpolated from.
type cellWeight struct {
	row, col int
	w        float64
}

// weights are the cells and weights that interpolate the grid bilinearly
// at p, between the centres of the cells around it; between the outer
// centres and the edge of the grid the edge cells are held. It is false for
// a point off the grid.
func (g Grid) weights(p Point) ([4]cellWeight, bool) {
	if !g.Bounds().Contains(p) {
		return [4]cellWeight{}, false
	}
	fc := (p.X-g.X0)/g.Dx - 0.5
	fr := (g.Y0-p.Y)/g.Dy - 0.5
	c0 := int(math.Max(0, math.Min(float64(g.Width-1), math.Floor(fc))))
	r0 := int(math.Max(0, math.Min(float64(g.Height-1), math.Floor(fr))))
	c1, r1 := min(c0+1, g.Width-1), min(r0+1, g.Height-1)
	wx := math.Max(0, math.Min(1, fc-float64(c0)))
	wy := math.Max(0, math.Min(1, fr-float64(r0)))
	return [4]cellWeight{
		{r0, c0, (1 - wx) * (1 - wy)},
		{r0, c1, wx * (1 - wy)},
		{r1, c0, (1 - wx) * wy},
		{r1, c1, wx * wy},
	}, true
}

// interpolate is the weighted mean of the values of the cells with a value,
// or NaN if none has.
func interpolate(ws [4]cellWeight, at func(row, col int) float32) float32 {
	var sum, wsum float64
	for _, c := range ws {
		if c.w == 0 {
			continue
		}
		v := at(c.row, c.col)
		if v != v {
			continue
		}
		sum += c.w * float64(v)
		wsum += c.w
	}
	if wsum == 0 {
		return float32NaN()
	}
	return float32(sum / wsum)
}

// parcelWeights are the cells each parcel samples, at the centre of its box.
// A parcel off the grid samples none, and its samples are NaN.
func (st *weatherState) parcelWeights(set *parcelSet) [][4]cellWeight {
	out := make([][4]cellWeight, set.len())
	for i, b := range set.boxes {
		out[i], _ = st.meta.Grid.weights(b.center())
	}
	return out
}

// appendSamples samples steps n to n+k at every parcel, from at, and
// appends them to the parcel samples.
func (w *Weather) appendSamples(ctx context.Context, st *weatherState, n, k int, at func(v string) (func(s, row, col int) float32, error)) error {
	set := st.pwSet
	ws := st.parcelWeights(set)
	for _, v := range st.meta.Variables {
		get, err := at(v)
		if err != nil {
			return err
		}
		out := make([]float32, set.len()*k)
		for p := range ws {
			for s := range k {
				out[p*k+s] = interpolate(ws[p], func(row, col int) float32 { return get(s, row, col) })
			}
		}
		a, err := zarr.OpenArray(ctx, w.db.store, st.pw[v].Path())
		if err != nil {
			return err
		}
		if err := appendAt(ctx, a, 1, n, out); err != nil {
			return err
		}
	}
	pg, err := zarr.OpenGroup(ctx, w.db.store, w.pwPath(st.pwGen))
	if err != nil {
		return err
	}
	return pg.SetAttributes(ctx, map[string]any{"steps": n + k})
}

// SampleParcels brings the samples at the parcels up to the steps the
// source holds, setting them up first if the parcels are new. It reads the
// steps it samples a chunk deep at a time, a variable at a time, and of each
// only the chunks some parcel samples.
func (w *Weather) SampleParcels(ctx context.Context) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	return w.sampleLocked(ctx)
}

func (w *Weather) sampleLocked(ctx context.Context) error {
	if err := w.reload(ctx); err != nil {
		return err
	}
	set := w.db.parcels.Load()
	if set.len() == 0 {
		return nil
	}
	st := w.state.Load()
	if st.pw == nil || st.pwGen != set.generation {
		if err := w.setUpSamples(ctx, st, set); err != nil {
			return err
		}
		if err := w.reload(ctx); err != nil {
			return err
		}
		st = w.state.Load()
	}
	m := st.meta
	g := m.Grid
	ws := st.parcelWeights(st.pwSet)
	// The chunks of the grid any parcel samples.
	chunkOf := func(row, col int) [2]int { return [2]int{row / m.ChunkSide, col / m.ChunkSide} }
	need := map[[2]int]bool{}
	for _, cw := range ws {
		for _, c := range cw {
			if c.w != 0 {
				need[chunkOf(c.row, c.col)] = true
			}
		}
	}
	for n := st.pwSteps; n < st.steps; {
		k := min(st.steps-n, m.ChunkSteps-n%m.ChunkSteps)
		at := func(v string) (func(s, row, col int) float32, error) {
			chunks := map[[2]int][]float32{}
			for c := range need {
				r0, c0 := c[0]*m.ChunkSide, c[1]*m.ChunkSide
				h, wd := min(m.ChunkSide, g.Height-r0), min(m.ChunkSide, g.Width-c0)
				data, err := zarr.Read[float32](ctx, st.arrays[v], []int{n, r0, c0}, []int{k, h, wd})
				if err != nil {
					return nil, err
				}
				chunks[c] = data
			}
			return func(s, row, col int) float32 {
				c := chunkOf(row, col)
				r0, c0 := c[0]*m.ChunkSide, c[1]*m.ChunkSide
				h, wd := min(m.ChunkSide, g.Height-r0), min(m.ChunkSide, g.Width-c0)
				return chunks[c][(s*h+row-r0)*wd+col-c0]
			}, nil
		}
		if err := w.appendSamples(ctx, st, n, k, at); err != nil {
			return err
		}
		n += k
	}
	return w.reload(ctx)
}

// setUpSamples makes the arrays of samples at set, under a path of its
// generation, and deletes those of generations before the one before.
func (w *Weather) setUpSamples(ctx context.Context, st *weatherState, set *parcelSet) error {
	s := w.db.store
	parent := joinPath("parcel_weather", w.name)
	if _, err := zarr.OpenGroup(ctx, s, parent); isNotFound(err) {
		if _, err := zarr.CreateGroup(ctx, s, parent, nil); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	path := w.pwPath(set.generation)
	if err := zarr.Delete(ctx, s, path); err != nil {
		return err
	}
	g, err := zarr.CreateGroup(ctx, s, path, nil)
	if err != nil {
		return err
	}
	p := set.len()
	for _, v := range st.meta.Variables {
		if _, err := g.CreateArray(ctx, v, zarr.ArrayOptions{
			Shape:          []int{p, 0},
			ChunkShape:     []int{max(1, min(st.meta.ParcelChunk, p)), st.meta.ChunkSteps},
			DataType:       zarr.Float32,
			FillValue:      float32NaN(),
			Codecs:         codecs(zarr.Float32),
			DimensionNames: []string{"parcel", "time"},
		}); err != nil {
			return err
		}
	}
	if err := g.SetAttributes(ctx, map[string]any{"generation": set.generation, "steps": 0}); err != nil {
		return err
	}
	return dropGenerations(ctx, s, parent, set)
}

// Series is the source's values at a longitude and latitude, interpolated
// bilinearly, from the first step at or after from to the last at or before
// to. It reads the chunks of the four cells around the point.
func (w *Weather) Series(ctx context.Context, variable string, lon, lat float64, from, to time.Time) ([]Sample, error) {
	st := w.state.Load()
	a, ok := st.arrays[variable]
	if !ok {
		return nil, fmt.Errorf("%w: variable %s of weather source %s", ErrNotFound, variable, w.name)
	}
	m := st.meta
	s0, s1 := m.stepRange(from, to, st.steps)
	if s0 == s1 {
		return nil, nil
	}
	ws, ok := m.Grid.weights(Point{lon, lat})
	if !ok {
		return nil, fmt.Errorf("%w: (%g, %g) is off the grid of weather source %s", ErrOffGrid, lon, lat, w.name)
	}
	r0, c0 := ws[0].row, ws[0].col
	h, wd := ws[3].row-r0+1, ws[3].col-c0+1
	data, err := zarr.Read[float32](ctx, a, []int{s0, r0, c0}, []int{s1 - s0, h, wd})
	if err != nil {
		return nil, err
	}
	out := make([]Sample, s1-s0)
	for s := range out {
		out[s] = Sample{
			Time: time.Unix(m.Start+int64(s0+s)*m.Step, 0).UTC(),
			Value: interpolate(ws, func(row, col int) float32 {
				return data[(s*h+row-r0)*wd+col-c0]
			}),
		}
	}
	return out, nil
}

// ParcelWeather is a weather source's samples at a parcel, from the first
// step at or after from to the last at or before to that has been sampled.
// It reads a chunk for every ChunkSteps steps.
func (db *DB) ParcelWeather(ctx context.Context, source, variable string, id int64, from, to time.Time) ([]Sample, error) {
	w := db.WeatherSource(source)
	if w == nil {
		return nil, fmt.Errorf("%w: weather source %s", ErrNotFound, source)
	}
	set := db.parcels.Load()
	i, ok := set.byIDOK(id)
	if !ok {
		return nil, fmt.Errorf("%w: parcel %d", ErrNotFound, id)
	}
	st := w.state.Load()
	a, ok := st.pw[variable]
	if ok && st.pwGen != set.generation {
		// The snapshot of the source is of other parcels than the one of
		// the parcels: a PutParcels or Refresh is under way.
		return nil, fmt.Errorf("geodb: weather source %s has not been sampled at the parcels there are now", source)
	}
	if !ok {
		if _, known := st.arrays[variable]; known {
			return nil, fmt.Errorf("geodb: weather source %s has not been sampled at the parcels", source)
		}
		return nil, fmt.Errorf("%w: variable %s of weather source %s", ErrNotFound, variable, source)
	}
	m := st.meta
	s0, s1 := m.stepRange(from, to, st.pwSteps)
	if s0 == s1 {
		return nil, nil
	}
	data, err := zarr.Read[float32](ctx, a, []int{i, s0}, []int{1, s1 - s0})
	if err != nil {
		return nil, err
	}
	out := make([]Sample, len(data))
	for s, v := range data {
		out[s] = Sample{time.Unix(m.Start+int64(s0+s)*m.Step, 0).UTC(), v}
	}
	return out, nil
}

func (s *parcelSet) byIDOK(id int64) (int, bool) {
	if s.len() == 0 {
		return 0, false
	}
	i, ok := s.byID[id]
	return i, ok
}
