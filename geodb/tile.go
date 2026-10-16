package geodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LukasSelin/zarr"
)

// TileOptions sets up a tile.
type TileOptions struct {
	Grid      Grid
	Variables []Variable
	// MapChunk is the side of the chunks of a scene, which are one scene
	// deep: a map of one date reads the chunks it covers and nothing else.
	// 512 by default.
	MapChunk int
	// CompositePeriod is the length of a composite's period, ten days by
	// default, and CompositeOrigin where the first begins, 1 January 2015
	// UTC by default (Sentinel-2A went up that June).
	CompositePeriod time.Duration
	CompositeOrigin time.Time
	// CompositeTime and CompositeSide are the chunks of composites:
	// CompositeTime periods deep and CompositeSide pixels square, 64 and 64
	// by default, so that the history of a pixel is one chunk for every 64
	// periods. They are kept in shards CompositeTime deep and MapChunk
	// square, which CompositeSide must divide.
	CompositeTime int
	CompositeSide int
	// StatsParcels and StatsScenes are the chunks of the parcel statistics:
	// 128 parcels by 128 scenes by default. A parcel's history is a chunk
	// for every StatsScenes scenes; a scene appended rewrites a chunk for
	// every StatsParcels parcels.
	StatsParcels int
	StatsScenes  int
}

type tileMeta struct {
	Grid            Grid       `json:"grid"`
	Variables       []Variable `json:"variables"`
	MapChunk        int        `json:"map_chunk"`
	CompositeOrigin int64      `json:"composite_origin"` // Unix seconds
	CompositePeriod int64      `json:"composite_period"` // seconds
	CompositeTime   int        `json:"composite_time"`
	CompositeSide   int        `json:"composite_side"`
	StatsParcels    int        `json:"stats_parcels"`
	StatsScenes     int        `json:"stats_scenes"`
}

func (o TileOptions) meta() (tileMeta, error) {
	m := tileMeta{
		Grid:          o.Grid,
		Variables:     slices.Clone(o.Variables),
		MapChunk:      o.MapChunk,
		CompositeTime: o.CompositeTime,
		CompositeSide: o.CompositeSide,
		StatsParcels:  o.StatsParcels,
		StatsScenes:   o.StatsScenes,
	}
	def := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	def(&m.MapChunk, 512)
	def(&m.CompositeTime, 64)
	def(&m.CompositeSide, 64)
	def(&m.StatsParcels, 128)
	def(&m.StatsScenes, 128)
	period := o.CompositePeriod
	if period == 0 {
		period = 10 * 24 * time.Hour
	}
	origin := o.CompositeOrigin
	if origin.IsZero() {
		origin = time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	m.CompositePeriod, m.CompositeOrigin = int64(period/time.Second), origin.Unix()

	if err := m.Grid.validate(); err != nil {
		return m, err
	}
	if len(m.Variables) == 0 {
		return m, errors.New("geodb: a tile needs a variable")
	}
	seen := map[string]bool{}
	for i := range m.Variables {
		if err := m.Variables[i].validate(); err != nil {
			return m, err
		}
		if seen[m.Variables[i].Name] {
			return m, fmt.Errorf("geodb: variable %s is given twice", m.Variables[i].Name)
		}
		seen[m.Variables[i].Name] = true
	}
	switch {
	case m.MapChunk <= 0 || m.CompositeTime <= 0 || m.CompositeSide <= 0 || m.StatsParcels <= 0 || m.StatsScenes <= 0:
		return m, errors.New("geodb: chunk sizes must be positive")
	case m.MapChunk%m.CompositeSide != 0:
		return m, fmt.Errorf("geodb: CompositeSide %d does not divide MapChunk %d", m.CompositeSide, m.MapChunk)
	case m.CompositePeriod <= 0:
		return m, errors.New("geodb: the composite period must be at least a second")
	}
	return m, nil
}

func (m tileMeta) variable(name string) (Variable, bool) {
	for _, v := range m.Variables {
		if v.Name == name {
			return v, true
		}
	}
	return Variable{}, false
}

// period is the composite period t is in.
func (m tileMeta) period(t int64) int64 {
	d := t - m.CompositeOrigin
	p := d / m.CompositePeriod
	if d < 0 && d%m.CompositePeriod != 0 {
		p--
	}
	return p
}

// Tile is the scenes of one tile of a satellite product, on the tile's own
// grid, with their composites and the statistics of the parcels in it.
type Tile struct {
	db   *DB
	name string
	path string

	state    atomic.Pointer[tileState]
	writeMu  sync.Mutex // one writer in this process at a time
	reloadMu sync.Mutex // one reload at a time, so that none undoes a later one
}

// statsPath is where the statistics made for a generation of parcels are.
func (t *Tile) statsPath(gen int64) string { return joinPath(t.path, "stats", genName(gen)) }

// tileState is a snapshot of a tile. Nothing in it changes once it is made.
type tileState struct {
	meta    tileMeta
	proj    Projection
	times   []int64
	scenes  map[string]*zarr.Array
	comps   map[string]*zarr.Array
	stats   map[string]*zarr.Array
	statsN  map[string]int // scenes the statistics of each variable cover
	parcels *tileParcels
}

// CreateTile makes a tile. If there are parcels, the tile is indexed for
// them at once.
func (db *DB) CreateTile(ctx context.Context, name string, o TileOptions) (*Tile, error) {
	if err := validName("tile", name); err != nil {
		return nil, err
	}
	m, err := o.meta()
	if err != nil {
		return nil, err
	}
	db.adminMu.Lock()
	defer db.adminMu.Unlock()
	if db.Tile(name) != nil {
		return nil, fmt.Errorf("geodb: tile %s: %w", name, zarr.ErrExists)
	}
	t := &Tile{db: db, name: name, path: joinPath("tiles", name)}
	g, err := zarr.CreateGroup(ctx, db.store, t.path, nil)
	if err != nil {
		return nil, err
	}
	for _, sub := range []string{"scenes", "composites"} {
		if _, err := g.CreateGroup(ctx, sub, nil); err != nil {
			return nil, err
		}
	}
	H, W, M := m.Grid.Height, m.Grid.Width, m.MapChunk
	if _, err := zarr.CreateArray(ctx, db.store, joinPath(t.path, "scenes", "time"), zarr.ArrayOptions{
		Shape:          []int{0},
		ChunkShape:     []int{4096},
		DataType:       zarr.Int64,
		Codecs:         codecs(zarr.Int64),
		DimensionNames: []string{"time"},
		Attributes:     map[string]any{"units": "seconds since 1970-01-01T00:00:00Z", "calendar": "proleptic_gregorian"},
	}); err != nil {
		return nil, err
	}
	for _, v := range m.Variables {
		if _, err := zarr.CreateArray(ctx, db.store, joinPath(t.path, "scenes", v.Name), zarr.ArrayOptions{
			Shape:          []int{0, H, W},
			ChunkShape:     []int{1, min(M, H), min(M, W)},
			DataType:       zarr.Int16,
			FillValue:      fill16,
			Codecs:         codecs(zarr.Int16),
			DimensionNames: []string{"time", "y", "x"},
			Attributes:     v.cfAttributes(),
		}); err != nil {
			return nil, err
		}
		shard := []int{m.CompositeTime, M, M}
		if M == m.CompositeSide {
			shard = nil
		}
		ca := v.cfAttributes()
		ca["composite"] = v.Composite
		ca["period_seconds"] = m.CompositePeriod
		ca["origin"] = time.Unix(m.CompositeOrigin, 0).UTC().Format(time.RFC3339)
		if _, err := zarr.CreateArray(ctx, db.store, joinPath(t.path, "composites", v.Name), zarr.ArrayOptions{
			Shape:          []int{0, H, W},
			ChunkShape:     []int{m.CompositeTime, m.CompositeSide, m.CompositeSide},
			ShardShape:     shard,
			DataType:       zarr.Int16,
			FillValue:      fill16,
			Codecs:         codecs(zarr.Int16),
			DimensionNames: []string{"period", "y", "x"},
			Attributes:     ca,
		}); err != nil {
			return nil, err
		}
	}
	// The tile's description is the last of it written, and the registry
	// after that: a tile that is not in the registry is not opened.
	if err := g.SetAttributes(ctx, map[string]any{"geodb_tile": m}); err != nil {
		return nil, err
	}
	if err := db.register(ctx, name, ""); err != nil {
		return nil, err
	}
	if err := t.reload(ctx); err != nil {
		return nil, err
	}
	db.mu.Lock()
	db.tiles[name] = t
	db.mu.Unlock()
	if db.parcels.Load().len() > 0 {
		if err := t.indexParcels(ctx); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// reload makes a new snapshot of the tile from the store.
func (t *Tile) reload(ctx context.Context) error {
	t.reloadMu.Lock()
	defer t.reloadMu.Unlock()
	s := t.db.store
	g, err := zarr.OpenGroup(ctx, s, t.path)
	if err != nil {
		return err
	}
	st := &tileState{
		scenes: map[string]*zarr.Array{},
		comps:  map[string]*zarr.Array{},
		stats:  map[string]*zarr.Array{},
		statsN: map[string]int{},
	}
	ok, err := g.Attribute("geodb_tile", &st.meta)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("the tile was not finished being made")
	}
	if st.proj, err = projectionTo(st.meta.Grid.CRS); err != nil {
		return err
	}
	if st.times, err = readVector[int64](ctx, s, joinPath(t.path, "scenes", "time")); err != nil {
		return err
	}
	n := len(st.times)
	for _, v := range st.meta.Variables {
		if st.scenes[v.Name], err = zarr.OpenArray(ctx, s, joinPath(t.path, "scenes", v.Name)); err != nil {
			return err
		}
		if st.comps[v.Name], err = zarr.OpenArray(ctx, s, joinPath(t.path, "composites", v.Name)); err != nil {
			return err
		}
	}
	tp, err := t.loadParcelIndex(ctx, st.meta)
	if err != nil {
		return err
	}
	if tp.len() > 0 {
		st.parcels = tp
		for _, v := range st.meta.Variables {
			a, err := zarr.OpenArray(ctx, s, joinPath(t.statsPath(tp.generation), v.Name))
			if err != nil {
				return err
			}
			st.stats[v.Name] = a
			st.statsN[v.Name] = min(n, a.Shape()[1])
		}
	}
	t.state.Store(st)
	return nil
}

// loadParcelIndex reads which parcels the tile covers, or nil if it is not
// indexed for the parcels there are now. The index of the snapshot before is
// kept if it is of the same parcels, as it is from one append to the next.
func (t *Tile) loadParcelIndex(ctx context.Context, meta tileMeta) (*tileParcels, error) {
	s := t.db.store
	set := t.db.parcels.Load()
	if set.len() == 0 {
		return nil, nil
	}
	path := t.statsPath(set.generation)
	g, err := zarr.OpenGroup(ctx, s, path)
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tp := &tileParcels{}
	if ok, err := g.Attribute("generation", &tp.generation); err != nil || !ok || tp.generation != set.generation {
		return nil, err // made for other parcels, or not finished
	}
	if prev := t.state.Load(); prev != nil && prev.parcels != nil && prev.parcels.generation == tp.generation {
		return prev.parcels, nil
	}
	if tp.ordinal, err = readVector[int64](ctx, s, joinPath(path, "parcel")); err != nil {
		return nil, err
	}
	if tp.npix, err = readVector[int32](ctx, s, joinPath(path, "npix")); err != nil {
		return nil, err
	}
	if tp.spanOff, err = readVector[int64](ctx, s, joinPath(path, "span_offset")); err != nil {
		return nil, err
	}
	raw, err := readVector[int32](ctx, s, joinPath(path, "spans"))
	if err != nil {
		return nil, err
	}
	n := len(tp.ordinal)
	if len(tp.npix) != n || len(tp.spanOff) != n+1 || len(raw)%3 != 0 || !offsetsOK(tp.spanOff, int64(len(raw)/3)) {
		return nil, errors.New("geodb: the parcel index of the tile is broken")
	}
	tp.spans = make([]span, len(raw)/3)
	for i := range tp.spans {
		sp := span{raw[3*i], raw[3*i+1], raw[3*i+2]}
		if sp.row < 0 || int(sp.row) >= meta.Grid.Height || sp.c0 < 0 || sp.c1 <= sp.c0 || int(sp.c1) > meta.Grid.Width {
			return nil, errors.New("geodb: a span of the parcel index of the tile is off the grid")
		}
		tp.spans[i] = sp
	}
	for _, o := range tp.ordinal {
		if o < 0 || int(o) >= set.len() {
			return nil, errors.New("geodb: the parcel index of the tile names a parcel there is not")
		}
	}
	tp.finish()
	return tp, nil
}

// indexParcels works out which parcels the tile covers and their pixels,
// makes the statistics arrays for them, and works the statistics out for
// the scenes there are.
func (t *Tile) indexParcels(ctx context.Context) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.indexLocked(ctx)
}

// indexIfStale indexes the tile if there are parcels it is not indexed for:
// a PutParcels that stopped before it got to this tile, or one made by
// another process since this tile was last indexed here.
func (t *Tile) indexIfStale(ctx context.Context) error {
	set := t.db.parcels.Load()
	if set.len() == 0 {
		return nil
	}
	if tp := t.state.Load().parcels; tp != nil && tp.generation == set.generation {
		return nil
	}
	return t.indexLocked(ctx)
}

func (t *Tile) indexLocked(ctx context.Context) error {
	if err := t.reload(ctx); err != nil {
		return err
	}
	st := t.state.Load()
	set := t.db.parcels.Load()
	g := st.meta.Grid
	tb := g.Bounds()

	tp := &tileParcels{generation: set.generation, spanOff: []int64{0}}
	for i := range set.len() {
		if !projectedBBox(st.proj, set.boxes[i]).Intersects(tb) {
			continue
		}
		rings := set.rings(i)
		for _, r := range rings {
			for k, p := range r {
				r[k].X, r[k].Y = st.proj(p.X, p.Y)
			}
		}
		ss := rasterize(g, rings)
		if len(ss) == 0 {
			continue
		}
		var npix int32
		for _, s := range ss {
			npix += s.c1 - s.c0
		}
		tp.ordinal = append(tp.ordinal, int64(i))
		tp.npix = append(tp.npix, npix)
		tp.spans = append(tp.spans, ss...)
		tp.spanOff = append(tp.spanOff, int64(len(tp.spans)))
	}

	// The index goes under a path of the parcels' generation, written whole
	// before its generation attribute: a reader of another generation never
	// sees it, and an index that was not finished is begun again.
	s := t.db.store
	if _, err := zarr.OpenGroup(ctx, s, joinPath(t.path, "stats")); isNotFound(err) {
		if _, err := zarr.CreateGroup(ctx, s, joinPath(t.path, "stats"), nil); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	path := t.statsPath(set.generation)
	if err := zarr.Delete(ctx, s, path); err != nil {
		return err
	}
	sg, err := zarr.CreateGroup(ctx, s, path, nil)
	if err != nil {
		return err
	}
	pt := len(tp.ordinal)
	raw := make([]int32, 0, 3*len(tp.spans))
	for _, sp := range tp.spans {
		raw = append(raw, sp.row, sp.c0, sp.c1)
	}
	if err := writeVector(ctx, sg, "parcel", []int{pt}, []int{parcelChunk}, []string{"parcel"}, tp.ordinal); err != nil {
		return err
	}
	if err := writeVector(ctx, sg, "npix", []int{pt}, []int{parcelChunk}, []string{"parcel"}, tp.npix); err != nil {
		return err
	}
	if err := writeVector(ctx, sg, "span_offset", []int{pt + 1}, []int{parcelChunk}, []string{"parcel_edge"}, tp.spanOff); err != nil {
		return err
	}
	if err := writeVector(ctx, sg, "spans", []int{len(tp.spans), 3}, []int{4 * parcelChunk, 3}, []string{"span", "row_col0_col1"}, raw); err != nil {
		return err
	}
	for _, v := range st.meta.Variables {
		if _, err := sg.CreateArray(ctx, v.Name, zarr.ArrayOptions{
			Shape:          []int{pt, 0, numStats},
			ChunkShape:     []int{max(1, min(st.meta.StatsParcels, pt)), st.meta.StatsScenes, numStats},
			DataType:       zarr.Float32,
			FillValue:      float32NaN(),
			Codecs:         codecs(zarr.Float32),
			DimensionNames: []string{"parcel", "time", "stat"},
			Attributes:     map[string]any{"stats": StatNames},
		}); err != nil {
			return err
		}
	}
	if err := sg.SetAttributes(ctx, map[string]any{"generation": set.generation}); err != nil {
		return err
	}
	if err := dropGenerations(ctx, s, joinPath(t.path, "stats"), set); err != nil {
		return err
	}
	if err := t.reload(ctx); err != nil {
		return err
	}
	return t.backfillLocked(ctx)
}

// projectedBBox is a box around b projected: its corners and the middles of
// its sides, which is close enough for a box as small as a parcel's.
func projectedBBox(p Projection, b BBox) BBox {
	out := emptyBBox()
	for _, x := range []float64{b.MinX, (b.MinX + b.MaxX) / 2, b.MaxX} {
		for _, y := range []float64{b.MinY, (b.MinY + b.MaxY) / 2, b.MaxY} {
			px, py := p(x, y)
			out = out.extend(Point{px, py})
		}
	}
	return out
}

func float32NaN() float32 { return float32(math.NaN()) }

// BackfillStats works out the parcel statistics of the scenes that do not
// have them yet: those whose statistics an append did not get to write, or
// all of them if the tile is not indexed for the parcels there are, which
// it indexes first. It reads each of those scenes once.
func (t *Tile) BackfillStats(ctx context.Context) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if err := t.reload(ctx); err != nil {
		return err
	}
	if err := t.indexIfStale(ctx); err != nil {
		return err
	}
	return t.backfillLocked(ctx)
}

func (t *Tile) backfillLocked(ctx context.Context) error {
	st := t.state.Load()
	tp := st.parcels
	if tp.len() == 0 {
		return nil
	}
	g := st.meta.Grid
	n := len(st.times)
	for _, v := range st.meta.Variables {
		a, err := zarr.OpenArray(ctx, t.db.store, st.stats[v.Name].Path())
		if err != nil {
			return err
		}
		have := a.Shape()[1]
		if have > n {
			if err := a.Resize(ctx, []int{tp.len(), n, numStats}); err != nil {
				return err
			}
			have = n
		}
		scenes := st.scenes[v.Name]
		for i := have; i < n; i += st.meta.StatsScenes {
			k := min(st.meta.StatsScenes, n-i)
			block := make([]float32, tp.len()*k*numStats)
			for j := range k {
				scene := i + j
				src := func(ctx context.Context, r0, r1 int) ([]int16, error) {
					return zarr.Read[int16](ctx, scenes, []int{scene, r0, 0}, []int{1, r1 - r0, g.Width})
				}
				col, err := tp.compute(ctx, g.Width, g.Height, st.meta.MapChunk, v, src)
				if err != nil {
					return err
				}
				for p := range tp.len() {
					copy(block[(p*k+j)*numStats:], col[p*numStats:(p+1)*numStats])
				}
			}
			if _, err := zarr.Append(ctx, a, 1, block); err != nil {
				return err
			}
		}
	}
	return t.reload(ctx)
}

// Scene is one pass of the satellite over a tile: a raster of every variable
// of the tile, Width by Height in C order, NaN where there is no valid value.
type Scene struct {
	Time   time.Time
	Values map[string][]float32
}

// AppendScene adds a scene after the last, and returns its index. Its time
// must be after the last scene's. The parcel statistics of the scene are
// worked out from it while it is in memory, before it is committed.
func (t *Tile) AppendScene(ctx context.Context, sc Scene) (int, error) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if err := t.reload(ctx); err != nil {
		return 0, err
	}
	if err := t.indexIfStale(ctx); err != nil {
		return 0, err
	}
	st := t.state.Load()
	for _, v := range st.meta.Variables {
		if st.parcels.len() > 0 && st.statsN[v.Name] < len(st.times) {
			// Statistics that fell behind - an append made while another
			// generation of parcels was current here - are caught up first,
			// so that this scene's can follow them.
			if err := t.backfillLocked(ctx); err != nil {
				return 0, err
			}
			st = t.state.Load()
			break
		}
	}
	g := st.meta.Grid
	n := len(st.times)
	ts := sc.Time.Unix()
	if n > 0 && ts <= st.times[n-1] {
		return 0, fmt.Errorf("geodb: a scene of %s is not after the last, of %s",
			sc.Time.UTC().Format(time.RFC3339), time.Unix(st.times[n-1], 0).UTC().Format(time.RFC3339))
	}
	for name, vals := range sc.Values {
		if _, ok := st.meta.variable(name); !ok {
			return 0, fmt.Errorf("%w: variable %s of tile %s", ErrNotFound, name, t.name)
		}
		if len(vals) != g.Width*g.Height {
			return 0, fmt.Errorf("geodb: %d values of %s for a tile of %d by %d", len(vals), name, g.Width, g.Height)
		}
	}
	for _, v := range st.meta.Variables {
		if _, ok := sc.Values[v.Name]; !ok {
			return 0, fmt.Errorf("geodb: the scene has no %s", v.Name)
		}
	}

	s := t.db.store
	for _, v := range st.meta.Variables {
		raw := make([]int16, g.Width*g.Height)
		for i, f := range sc.Values[v.Name] {
			raw[i] = v.encode(f)
		}
		a, err := zarr.OpenArray(ctx, s, st.scenes[v.Name].Path())
		if err != nil {
			return 0, err
		}
		if err := appendAt(ctx, a, 0, n, raw); err != nil {
			return 0, err
		}
		if tp := st.parcels; tp.len() > 0 && st.statsN[v.Name] == n {
			src := func(_ context.Context, r0, r1 int) ([]int16, error) { return raw[r0*g.Width : r1*g.Width], nil }
			col, err := tp.compute(ctx, g.Width, g.Height, st.meta.MapChunk, v, src)
			if err != nil {
				return 0, err
			}
			sa, err := zarr.OpenArray(ctx, s, st.stats[v.Name].Path())
			if err != nil {
				return 0, err
			}
			if err := appendAt(ctx, sa, 1, n, col); err != nil {
				return 0, err
			}
		}
	}
	ta, err := zarr.OpenArray(ctx, s, joinPath(t.path, "scenes", "time"))
	if err != nil {
		return 0, err
	}
	if err := appendAt(ctx, ta, 0, n, []int64{ts}); err != nil {
		return 0, err
	}
	return n, t.reload(ctx)
}

// appendAt appends data to a along axis at index at, cutting back first
// what an append that did not finish left past it.
func appendAt[T zarr.Element](ctx context.Context, a *zarr.Array, axis, at int, data []T) error {
	shape := a.Shape()
	if shape[axis] != at {
		if shape[axis] < at {
			return fmt.Errorf("geodb: %s is %d long along axis %d, short of %d", a.Path(), shape[axis], axis, at)
		}
		shape[axis] = at
		if err := a.Resize(ctx, shape); err != nil {
			return err
		}
	}
	_, err := zarr.Append(ctx, a, axis, data)
	return err
}

// Name is the tile's name.
func (t *Tile) Name() string { return t.name }

// Grid is the tile's grid.
func (t *Tile) Grid() Grid { return t.state.Load().meta.Grid }

// Variables are the tile's variables.
func (t *Tile) Variables() []Variable { return slices.Clone(t.state.Load().meta.Variables) }

// Times are the times of the tile's scenes, in order.
func (t *Tile) Times() []time.Time {
	st := t.state.Load()
	out := make([]time.Time, len(st.times))
	for i, s := range st.times {
		out[i] = time.Unix(s, 0).UTC()
	}
	return out
}

// Pixel is the pixel of the tile a longitude and latitude are in.
func (t *Tile) Pixel(lon, lat float64) (col, row int, ok bool) {
	st := t.state.Load()
	x, y := st.proj(lon, lat)
	return st.meta.Grid.Pixel(Point{x, y})
}

// Sample is a value at a time.
type Sample struct {
	Time  time.Time `json:"time"`
	Value float32   `json:"value"`
}

func (t *Tile) variable(st *tileState, name string) (Variable, error) {
	v, ok := st.meta.variable(name)
	if !ok {
		return v, fmt.Errorf("%w: variable %s of tile %s", ErrNotFound, name, t.name)
	}
	return v, nil
}

// SceneWindow is a window of the scene at index i, in C order, NaN where
// there is no valid value. It reads the map chunks the window covers.
func (t *Tile) SceneWindow(ctx context.Context, variable string, i int, w Window) ([]float32, error) {
	st := t.state.Load()
	v, err := t.variable(st, variable)
	if err != nil {
		return nil, err
	}
	if i < 0 || i >= len(st.times) {
		return nil, fmt.Errorf("%w: scene %d of tile %s", ErrNotFound, i, t.name)
	}
	raw, err := zarr.Read[int16](ctx, st.scenes[variable], []int{i, w.Row, w.Col}, []int{1, w.Height, w.Width})
	if err != nil {
		return nil, err
	}
	out := make([]float32, len(raw))
	for k, x := range raw {
		out[k] = v.decode(x)
	}
	return out, nil
}

// PixelHistory is every scene's value of one pixel, NaN where it had none.
// It reads a chunk of every scene, so it is the slow way; CompositeHistory
// is the fast one.
func (t *Tile) PixelHistory(ctx context.Context, variable string, col, row int) ([]Sample, error) {
	st := t.state.Load()
	v, err := t.variable(st, variable)
	if err != nil {
		return nil, err
	}
	n := len(st.times)
	if n == 0 {
		return nil, nil
	}
	raw, err := zarr.Read[int16](ctx, st.scenes[variable], []int{0, row, col}, []int{n, 1, 1})
	if err != nil {
		return nil, err
	}
	out := make([]Sample, n)
	for k, x := range raw {
		out[k] = Sample{time.Unix(st.times[k], 0).UTC(), v.decode(x)}
	}
	return out, nil
}

// CompositeHistory is a pixel's composite in every period composites have
// been made for, at the start of each period, NaN where a period had no
// valid value. It reads a chunk for every CompositeTime periods.
func (t *Tile) CompositeHistory(ctx context.Context, variable string, col, row int) ([]Sample, error) {
	st := t.state.Load()
	v, err := t.variable(st, variable)
	if err != nil {
		return nil, err
	}
	a := st.comps[variable]
	k := a.Shape()[0]
	if k == 0 {
		return nil, nil
	}
	raw, err := zarr.Read[int16](ctx, a, []int{0, row, col}, []int{k, 1, 1})
	if err != nil {
		return nil, err
	}
	out := make([]Sample, k)
	for p, x := range raw {
		at := st.meta.CompositeOrigin + int64(p)*st.meta.CompositePeriod
		out[p] = Sample{time.Unix(at, 0).UTC(), v.decode(x)}
	}
	return out, nil
}
