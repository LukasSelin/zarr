package geodb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/LukasSelin/zarr"
)

// layoutVersion is the layout of the store this package writes and reads.
const layoutVersion = 1

// DB is a geodb store, opened. Its methods may be called from any number of
// goroutines at once.
type DB struct {
	store zarr.Store

	// adminMu holds apart what changes the shape of the whole store:
	// creating tiles and sources, and putting parcels.
	adminMu sync.Mutex

	mu      sync.RWMutex
	tiles   map[string]*Tile
	weather map[string]*Weather

	parcels atomic.Pointer[parcelSet]
}

// registry is the tiles and sources of a store, kept in the attributes of
// its root so that a store that cannot be listed - one over plain HTTP -
// can be opened.
type registry struct {
	Tiles   []string `json:"tiles"`
	Weather []string `json:"weather"`
}

// Create makes a new, empty geodb store in s, which must not hold one.
func Create(ctx context.Context, s zarr.Store) (*DB, error) {
	if _, err := zarr.CreateGroup(ctx, s, "", map[string]any{
		"geodb":    layoutVersion,
		"registry": registry{Tiles: []string{}, Weather: []string{}},
	}); err != nil {
		return nil, err
	}
	for _, g := range []string{"tiles", "weather", "parcel_weather"} {
		if _, err := zarr.CreateGroup(ctx, s, g, nil); err != nil {
			return nil, err
		}
	}
	return Open(ctx, s)
}

// Open opens the geodb store in s.
func Open(ctx context.Context, s zarr.Store) (*DB, error) {
	db := &DB{store: s, tiles: map[string]*Tile{}, weather: map[string]*Weather{}}
	if err := db.Refresh(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

// Store is the store the DB is in.
func (db *DB) Store() zarr.Store { return db.store }

func (db *DB) readRegistry(ctx context.Context) (*zarr.Group, registry, error) {
	var reg registry
	root, err := zarr.OpenGroup(ctx, db.store, "")
	if err != nil {
		return nil, reg, err
	}
	var v int
	ok, err := root.Attribute("geodb", &v)
	if err != nil {
		return nil, reg, err
	}
	if !ok {
		return nil, reg, errors.New("geodb: the store is not a geodb store: its root has no geodb attribute")
	}
	if v != layoutVersion {
		return nil, reg, fmt.Errorf("geodb: the store is of layout %d, and this package reads %d", v, layoutVersion)
	}
	if _, err := root.Attribute("registry", &reg); err != nil {
		return nil, reg, err
	}
	return root, reg, nil
}

// Refresh reads again what other processes may have written: the parcels,
// and the scenes, steps and statistics of every tile and source, including
// ones created since. Reads go on from the snapshot before until it is done.
// If the store is a CachedStore, what is cached of what can change - all but
// the chunks of scenes and parcels, which are written once - is dropped
// first.
//
// A tile or source that cannot be read keeps the snapshot it had, and the
// others are refreshed all the same; the error says which.
func (db *DB) Refresh(ctx context.Context) error {
	db.adminMu.Lock()
	defer db.adminMu.Unlock()
	if c, ok := db.store.(*CachedStore); ok {
		c.Purge(writtenOnce)
	}
	_, reg, err := db.readRegistry(ctx)
	if err != nil {
		return err
	}
	set, err := loadParcels(ctx, db.store)
	if err != nil {
		return err
	}
	db.parcels.Store(set)
	var errs []error
	for _, name := range reg.Tiles {
		t := db.Tile(name)
		if t == nil {
			t = &Tile{db: db, name: name, path: joinPath("tiles", name)}
		}
		if err := t.reload(ctx); err != nil {
			errs = append(errs, fmt.Errorf("geodb: tile %s: %w", name, err))
			continue
		}
		db.mu.Lock()
		db.tiles[name] = t
		db.mu.Unlock()
	}
	for _, name := range reg.Weather {
		w := db.WeatherSource(name)
		if w == nil {
			w = &Weather{db: db, name: name, path: joinPath("weather", name)}
		}
		if err := w.reload(ctx); err != nil {
			errs = append(errs, fmt.Errorf("geodb: weather %s: %w", name, err))
			continue
		}
		db.mu.Lock()
		db.weather[name] = w
		db.mu.Unlock()
	}
	return errors.Join(errs...)
}

// writtenOnce is whether a key is never written again once it is: a chunk
// of a scene (not of their times), or anything of a generation of parcels.
// Metadata is not, as arrays grow.
func writtenOnce(key string) bool {
	if strings.HasSuffix(key, "zarr.json") {
		return false
	}
	if rest, ok := strings.CutPrefix(key, "parcels/g"); ok {
		return strings.Contains(rest, "/")
	}
	parts := strings.SplitN(key, "/", 6) // tiles, t, scenes, var, c, ...
	return len(parts) == 6 && parts[0] == "tiles" && parts[2] == "scenes" && parts[3] != "time" && parts[4] == "c"
}

// register adds a tile or source to the registry in the root.
func (db *DB) register(ctx context.Context, tile, source string) error {
	root, reg, err := db.readRegistry(ctx)
	if err != nil {
		return err
	}
	if tile != "" && !slices.Contains(reg.Tiles, tile) {
		reg.Tiles = append(reg.Tiles, tile)
	}
	if source != "" && !slices.Contains(reg.Weather, source) {
		reg.Weather = append(reg.Weather, source)
	}
	return root.SetAttributes(ctx, map[string]any{"registry": reg})
}

// Tile is the tile of the given name, or nil.
func (db *DB) Tile(name string) *Tile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.tiles[name]
}

// Tiles are every tile, by name.
func (db *DB) Tiles() []*Tile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]*Tile, 0, len(db.tiles))
	for _, t := range db.tiles {
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b *Tile) int { return strings.Compare(a.name, b.name) })
	return out
}

// WeatherSource is the weather source of the given name, or nil.
func (db *DB) WeatherSource(name string) *Weather {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.weather[name]
}

// WeatherSources are every weather source, by name.
func (db *DB) WeatherSources() []*Weather {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]*Weather, 0, len(db.weather))
	for _, w := range db.weather {
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, b *Weather) int { return strings.Compare(a.name, b.name) })
	return out
}
