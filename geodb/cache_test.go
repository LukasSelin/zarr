package geodb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/storetest"
)

func TestACachedStoreKeepsTheContract(t *testing.T) {
	storetest.Run(t, func() zarr.Store { return NewCachedStore(zarr.NewMemoryStore(), CacheOptions{}) })
	storetest.Run(t, func() zarr.Store {
		return NewCachedStore(zarr.NewMemoryStore(), CacheOptions{Metadata: true, MaxBytes: 64})
	})
	storetest.Run(t, func() zarr.Store { return NewCachedStore(getOnly{zarr.NewMemoryStore()}, CacheOptions{}) })
}

// getOnly hides the ranges and listing of one level of a store.
type getOnly struct{ s *zarr.MemoryStore }

func (g getOnly) Get(ctx context.Context, k string) ([]byte, error) { return g.s.Get(ctx, k) }
func (g getOnly) Set(ctx context.Context, k string, v []byte) error { return g.s.Set(ctx, k, v) }
func (g getOnly) Delete(ctx context.Context, k string) error        { return g.s.Delete(ctx, k) }
func (g getOnly) List(ctx context.Context, p string, fn func(string) error) error {
	return g.s.List(ctx, p, fn)
}

// counting counts the reads that reach a store, and can hold them.
type counting struct {
	zarr.Store
	gets, ranges atomic.Int64
	hold         chan struct{} // if not nil, reads wait for it
}

func (c *counting) Get(ctx context.Context, k string) ([]byte, error) {
	c.gets.Add(1)
	if c.hold != nil {
		<-c.hold
	}
	return c.Store.Get(ctx, k)
}

func (c *counting) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	c.ranges.Add(1)
	return c.Store.(zarr.RangeGetter).GetRange(ctx, k, off, n)
}

func TestACachedValueIsReadOnce(t *testing.T) {
	ctx := context.Background()
	inner := &counting{Store: zarr.NewMemoryStore()}
	c := NewCachedStore(inner, CacheOptions{})
	_ = inner.Set(ctx, "a/c/0", []byte("hello"))
	for range 3 {
		b, err := c.Get(ctx, "a/c/0")
		if err != nil || string(b) != "hello" {
			t.Fatalf("Get = %q, %v", b, err)
		}
		b[0] = 'J' // the caller's to change
	}
	for range 3 {
		if _, err := c.Get(ctx, "a/c/1"); !errors.Is(err, zarr.ErrNotFound) {
			t.Fatalf("a key that is not there: %v", err)
		}
	}
	for range 3 {
		b, err := c.GetRange(ctx, "a/c/0", -3, 2)
		if err != nil || string(b) != "ll" {
			t.Fatalf("GetRange = %q, %v", b, err)
		}
	}
	if g, r := inner.gets.Load(), inner.ranges.Load(); g != 2 || r != 1 {
		t.Errorf("the store was read %d times whole and %d in ranges; want 2 and 1", g, r)
	}
	if s := c.Stats(); s.Hits != 6 || s.Misses != 3 || s.Entries != 3 {
		t.Errorf("stats = %+v", s)
	}
}

func TestMetadataIsNotCachedUnlessAskedFor(t *testing.T) {
	ctx := context.Background()
	inner := &counting{Store: zarr.NewMemoryStore()}
	_ = inner.Set(ctx, "a/zarr.json", []byte("{}"))
	_ = inner.Set(ctx, "zarr.json", []byte("{}"))
	c := NewCachedStore(inner, CacheOptions{})
	for range 2 {
		_, _ = c.Get(ctx, "a/zarr.json")
		_, _ = c.Get(ctx, "zarr.json")
	}
	if n := inner.gets.Load(); n != 4 {
		t.Errorf("metadata was read %d times, want 4", n)
	}
	c = NewCachedStore(inner, CacheOptions{Metadata: true})
	inner.gets.Store(0)
	for range 2 {
		_, _ = c.Get(ctx, "a/zarr.json")
	}
	if n := inner.gets.Load(); n != 1 {
		t.Errorf("metadata asked to be cached was read %d times, want 1", n)
	}
}

func TestASetOrDeleteThroughTheCacheIsSeen(t *testing.T) {
	ctx := context.Background()
	c := NewCachedStore(zarr.NewMemoryStore(), CacheOptions{})
	if _, err := c.Get(ctx, "k"); !errors.Is(err, zarr.ErrNotFound) {
		t.Fatal(err)
	}
	_ = c.Set(ctx, "k", []byte("one"))
	_, _ = c.GetRange(ctx, "k", 0, 2)
	if b, _ := c.Get(ctx, "k"); string(b) != "one" {
		t.Fatalf("after a Set, %q", b)
	}
	_ = c.Set(ctx, "k", []byte("two"))
	if b, _ := c.Get(ctx, "k"); string(b) != "two" {
		t.Fatalf("after a second Set, %q", b)
	}
	if b, _ := c.GetRange(ctx, "k", 0, 2); string(b) != "tw" {
		t.Fatalf("a range after a second Set, %q", b)
	}
	_ = c.Delete(ctx, "k")
	if _, err := c.Get(ctx, "k"); !errors.Is(err, zarr.ErrNotFound) {
		t.Fatalf("after a Delete: %v", err)
	}
}

func TestReadsOfOneKeyAtOnceAreOneRead(t *testing.T) {
	ctx := context.Background()
	inner := &counting{Store: zarr.NewMemoryStore(), hold: make(chan struct{})}
	_ = inner.Store.Set(ctx, "k", []byte("v"))
	c := NewCachedStore(inner, CacheOptions{})
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b, err := c.Get(ctx, "k"); err != nil || string(b) != "v" {
				t.Errorf("Get = %q, %v", b, err)
			}
		}()
	}
	for inner.gets.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond) // for the rest to line up behind it
	close(inner.hold)
	wg.Wait()
	if n := inner.gets.Load(); n != 1 {
		t.Errorf("20 reads at once made %d reads of the store", n)
	}
}

func TestAReadUnderWayWhenTheKeyIsSetIsNotKept(t *testing.T) {
	ctx := context.Background()
	inner := &counting{Store: zarr.NewMemoryStore(), hold: make(chan struct{})}
	_ = inner.Store.Set(ctx, "k", []byte("old"))
	c := NewCachedStore(inner, CacheOptions{})
	done := make(chan []byte)
	go func() {
		b, _ := c.Get(ctx, "k")
		done <- b
	}()
	for inner.gets.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	_ = c.Set(ctx, "k", []byte("new"))
	close(inner.hold)
	<-done
	inner.hold = nil
	if b, _ := c.Get(ctx, "k"); string(b) != "new" {
		t.Errorf("after the Set, %q", b)
	}
}

func TestAWaiterWhoseLeaderIsCancelledReadsAgain(t *testing.T) {
	inner := &slowFirst{Store: zarr.NewMemoryStore(), started: make(chan struct{})}
	_ = inner.Store.Set(context.Background(), "k", []byte("v"))
	c := NewCachedStore(inner, CacheOptions{})
	leader, cancel := context.WithCancel(context.Background())
	go func() { _, _ = c.Get(leader, "k") }()
	<-inner.started
	got := make(chan error)
	go func() {
		b, err := c.Get(context.Background(), "k")
		if err == nil && string(b) != "v" {
			err = errors.New("read " + string(b))
		}
		got <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-got; err != nil {
		t.Errorf("the waiter: %v", err)
	}
}

// slowFirst holds its first read until its context is done.
type slowFirst struct {
	zarr.Store
	once    sync.Once
	started chan struct{}
}

func (s *slowFirst) Get(ctx context.Context, k string) ([]byte, error) {
	first := false
	s.once.Do(func() { first = true; close(s.started) })
	if first {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.Store.Get(ctx, k)
}

func TestTheCacheHoldsNoMoreThanItsSize(t *testing.T) {
	ctx := context.Background()
	inner := zarr.NewMemoryStore()
	c := NewCachedStore(inner, CacheOptions{MaxBytes: 1000})
	big := []byte(strings.Repeat("x", 99))
	for i := range 100 {
		k := string(rune('a'+i%26)) + string(rune('a'+i/26))
		_ = inner.Set(ctx, k, big)
		_, _ = c.Get(ctx, k)
		if s := c.Stats(); s.Bytes > 1000 {
			t.Fatalf("the cache holds %d bytes", s.Bytes)
		}
	}
	if s := c.Stats(); s.Entries != 9 {
		t.Errorf("%d entries of 101 bytes in 1000", s.Entries)
	}
	_ = inner.Set(ctx, "huge", make([]byte, 2000))
	_, _ = c.Get(ctx, "huge")
	if s := c.Stats(); s.Entries != 9 {
		t.Errorf("a value bigger than the cache was kept: %+v", s)
	}
}
