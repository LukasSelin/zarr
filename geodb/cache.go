package geodb

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/LukasSelin/zarr"
)

// CacheOptions sets up a CachedStore.
type CacheOptions struct {
	// MaxBytes is the most the cache holds, counting the values and not
	// what keeping them costs. Zero is 256 MiB.
	MaxBytes int64
	// Metadata is whether zarr.json is cached too. It is not by default, so
	// that a Refresh sees what another process wrote; with one process
	// writing and reading, caching it saves a request on every open.
	Metadata bool
}

// CachedStore keeps the values read from a store in memory, the least
// recently used going first when it is full. A key that is not there is
// remembered too, which is worth as much: the chunks of a scene under cloud
// are never written, and a read of one is otherwise a request that finds
// nothing.
//
// Many reads of one key at once make one read of the store. A Set or a
// Delete through the cache forgets what it held of the key; one made to the
// store by another process is not seen until the key is pushed out, so
// share a cache between processes only for data that does not change once
// written, which chunks of a scene are.
type CachedStore struct {
	inner zarr.Store
	max   int64
	meta  bool

	mu      sync.Mutex
	lru     *list.List // of *cacheEntry, most recent at the front
	entries map[cacheKey]*list.Element
	byKey   map[string]map[cacheKey]struct{}
	flights map[cacheKey]*flight
	bytes   int64

	hits, misses atomic.Int64
}

type cacheKey struct {
	key         string
	off, length int64
	whole       bool
}

type cacheEntry struct {
	k        cacheKey
	value    []byte
	notFound bool
}

type flight struct {
	done     chan struct{}
	value    []byte
	err      error
	notFound bool
	stale    bool // a Set or Delete of the key came while it was read
}

// NewCachedStore wraps s in a cache.
func NewCachedStore(s zarr.Store, o CacheOptions) *CachedStore {
	if o.MaxBytes <= 0 {
		o.MaxBytes = 256 << 20
	}
	return &CachedStore{
		inner:   s,
		max:     o.MaxBytes,
		meta:    o.Metadata,
		lru:     list.New(),
		entries: make(map[cacheKey]*list.Element),
		byKey:   make(map[string]map[cacheKey]struct{}),
		flights: make(map[cacheKey]*flight),
	}
}

// CacheStats is how a cache has done.
type CacheStats struct {
	Hits, Misses int64
	Bytes        int64 // held now
	Entries      int
}

// Stats is how the cache has done since it was made.
func (c *CachedStore) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{Hits: c.hits.Load(), Misses: c.misses.Load(), Bytes: c.bytes, Entries: len(c.entries)}
}

func (c *CachedStore) cacheable(key string) bool {
	return c.meta || !(key == "zarr.json" || strings.HasSuffix(key, "/zarr.json"))
}

func (c *CachedStore) Get(ctx context.Context, key string) ([]byte, error) {
	if !c.cacheable(key) {
		return c.inner.Get(ctx, key)
	}
	return c.read(ctx, cacheKey{key: key, whole: true}, func() ([]byte, error) {
		return c.inner.Get(ctx, key)
	})
}

// GetRange reads a range of a value. The store under the cache need not be
// a zarr.RangeGetter: one that is not is read whole, and the range cut out.
func (c *CachedStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	fetch := func() ([]byte, error) {
		if rg, ok := c.inner.(zarr.RangeGetter); ok {
			return rg.GetRange(ctx, key, offset, length)
		}
		b, err := c.inner.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return cut(key, b, offset, length)
	}
	if !c.cacheable(key) {
		return fetch()
	}
	return c.read(ctx, cacheKey{key: key, off: offset, length: length}, fetch)
}

// cut is the range of b a GetRange asks for.
func cut(key string, b []byte, offset, length int64) ([]byte, error) {
	size := int64(len(b))
	if offset < 0 {
		offset += size
	}
	if offset < 0 || length < 0 || offset > size || length > size-offset {
		return nil, fmt.Errorf("geodb: range %d+%d of %s, which is %d bytes", offset, length, key, size)
	}
	return append([]byte(nil), b[offset:offset+length]...), nil
}

func (c *CachedStore) read(ctx context.Context, k cacheKey, fetch func() ([]byte, error)) ([]byte, error) {
	c.mu.Lock()
	if e, ok := c.entries[k]; ok {
		c.lru.MoveToFront(e)
		ent := e.Value.(*cacheEntry)
		c.mu.Unlock()
		c.hits.Add(1)
		return answer(k.key, ent.value, ent.notFound)
	}
	if f, ok := c.flights[k]; ok {
		c.mu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if f.err != nil {
			if (errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded)) && ctx.Err() == nil {
				// The read was stopped by the context of whoever began it,
				// not this one's: read again.
				return c.read(ctx, k, fetch)
			}
			return nil, f.err
		}
		c.hits.Add(1)
		return answer(k.key, f.value, f.notFound)
	}
	f := &flight{done: make(chan struct{})}
	c.flights[k] = f
	c.mu.Unlock()
	c.misses.Add(1)

	v, err := fetch()
	if isNotFound(err) {
		f.notFound, err = true, nil
	}
	f.value, f.err = v, err

	c.mu.Lock()
	delete(c.flights, k)
	if err == nil && !f.stale {
		c.put(&cacheEntry{k: k, value: v, notFound: f.notFound})
	}
	c.mu.Unlock()
	close(f.done)
	if err != nil {
		return nil, err
	}
	return answer(k.key, v, f.notFound)
}

// answer is a copy of what the cache holds, which callers may change.
func answer(key string, v []byte, notFound bool) ([]byte, error) {
	if notFound {
		return nil, fmt.Errorf("%w: %s", zarr.ErrNotFound, key)
	}
	return append([]byte(nil), v...), nil
}

// put adds e, pushing out the least recently used until it fits. A value
// bigger than the whole cache is not kept.
func (c *CachedStore) put(e *cacheEntry) {
	size := int64(len(e.value)) + int64(len(e.k.key))
	if size > c.max {
		return
	}
	for c.bytes+size > c.max {
		c.remove(c.lru.Back())
	}
	c.entries[e.k] = c.lru.PushFront(e)
	keys := c.byKey[e.k.key]
	if keys == nil {
		keys = make(map[cacheKey]struct{})
		c.byKey[e.k.key] = keys
	}
	keys[e.k] = struct{}{}
	c.bytes += size
}

func (c *CachedStore) remove(el *list.Element) {
	e := c.lru.Remove(el).(*cacheEntry)
	delete(c.entries, e.k)
	if keys := c.byKey[e.k.key]; keys != nil {
		delete(keys, e.k)
		if len(keys) == 0 {
			delete(c.byKey, e.k.key)
		}
	}
	c.bytes -= int64(len(e.value)) + int64(len(e.k.key))
}

// forget drops what the cache holds of key, whole or in ranges, and marks
// reads of it under way as stale.
func (c *CachedStore) forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.byKey[key] {
		c.remove(c.entries[k])
	}
	for k, f := range c.flights {
		if k.key == key {
			f.stale = true
		}
	}
}

func (c *CachedStore) Set(ctx context.Context, key string, value []byte) error {
	c.forget(key)
	err := c.inner.Set(ctx, key, value)
	c.forget(key)
	return err
}

func (c *CachedStore) Delete(ctx context.Context, key string) error {
	c.forget(key)
	err := c.inner.Delete(ctx, key)
	c.forget(key)
	return err
}

func (c *CachedStore) List(ctx context.Context, prefix string, fn func(key string) error) error {
	return c.inner.List(ctx, prefix, fn)
}

// ListDir lists one level with the store's own ListDir where it has one.
func (c *CachedStore) ListDir(ctx context.Context, prefix string, fn func(name string) error) error {
	return zarr.ListDir(ctx, c.inner, prefix, fn)
}

var (
	_ zarr.Store       = (*CachedStore)(nil)
	_ zarr.RangeGetter = (*CachedStore)(nil)
	_ zarr.DirLister   = (*CachedStore)(nil)
)
