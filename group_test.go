package zarr

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// family is a group at "" of n arrays, named a000 up, beside a group and a
// child whose metadata does not parse.
func family(t *testing.T, s Store, n int) []string {
	t.Helper()
	if _, err := CreateGroup(ctx, s, "", nil); err != nil {
		t.Fatal(err)
	}
	var names []string
	for i := range n {
		name := fmt.Sprintf("a%03d", i)
		mustArray(t, s, name, ArrayOptions{Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32})
		names = append(names, name)
	}
	if _, err := CreateGroup(ctx, s, "sub", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, "broken/zarr.json", []byte("not json at all")); err != nil {
		t.Fatal(err)
	}
	return names
}

func TestChildrenAreReadConcurrentlyAndInOrder(t *testing.T) {
	defer func(c int) { defaultConcurrency = c }(defaultConcurrency)
	for _, c := range []int{1, 4} {
		t.Run(fmt.Sprint(c), func(t *testing.T) {
			defaultConcurrency = c
			b := &busyStore{MemoryStore: NewMemoryStore()}
			names := family(t, b, 20)
			g, err := OpenGroup(ctx, b, "")
			if err != nil {
				t.Fatal(err)
			}
			b.reset()
			got, err := g.Children(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var want []Child
			for _, name := range names {
				want = append(want, Child{name, "array"})
			}
			want = append(want, Child{"broken", ""}, Child{"sub", "group"})
			slices.SortFunc(want, func(x, y Child) int { return strings.Compare(x.Name, y.Name) })
			if !slices.Equal(got, want) {
				t.Errorf("children = %v, want %v", got, want)
			}
			if b.most > c {
				t.Errorf("%d reads at once, more than the %d allowed", b.most, c)
			}
			if c > 1 && b.most < 2 {
				t.Errorf("%d reads at once of %d, none of them together", b.most, b.calls)
			}
		})
	}
}

func TestChildrenStopAtTheFirstFailedRead(t *testing.T) {
	defer func(c int) { defaultConcurrency = c }(defaultConcurrency)
	defaultConcurrency = 2
	m := NewMemoryStore()
	family(t, m, 100)
	broken := errors.New("broken read")
	s := &failingGet{Store: m, substring: "a003/", err: broken}
	g, err := OpenGroup(ctx, s, "")
	if err != nil {
		t.Fatal(err)
	}
	s.reads = 0
	if _, err := g.Children(ctx); !errors.Is(err, broken) {
		t.Fatalf("children: %v, want %v", err, broken)
	}
	if _, err := g.OpenArrays(ctx); !errors.Is(err, broken) {
		t.Fatalf("open arrays: %v, want %v", err, broken)
	}
	if s.reads > 50 {
		t.Errorf("%d reads, which went on past the failed one", s.reads)
	}
}

// cancellingStore cancels a ctx on the nth read of it.
type cancellingStore struct {
	Store
	n      int64
	reads  atomic.Int64
	cancel context.CancelFunc
}

func (s *cancellingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.reads.Add(1) == s.n {
		s.cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Store.Get(ctx, key)
}

func TestChildrenStopWhenTheContextIsCancelled(t *testing.T) {
	defer func(c int) { defaultConcurrency = c }(defaultConcurrency)
	for _, c := range []int{1, 4} {
		t.Run(fmt.Sprint(c), func(t *testing.T) {
			defaultConcurrency = c
			m := NewMemoryStore()
			family(t, m, 100)
			g, err := OpenGroup(ctx, m, "")
			if err != nil {
				t.Fatal(err)
			}
			cctx, cancel := context.WithCancel(ctx)
			defer cancel()
			s := &cancellingStore{Store: m, n: 5, cancel: cancel}
			g.store = s
			if _, err := g.Children(cctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("children: %v, want it cancelled", err)
			}
			if n := s.reads.Load(); n > 5+int64(c) {
				t.Errorf("%d reads, which went on after the cancellation", n)
			}
		})
	}
}

// A store may answer a read the ctx has cancelled with nothing wrong; the
// children are cancelled all the same, and do not look complete.
func TestChildrenCancelledBeforeTheyStartAreCancelled(t *testing.T) {
	m := NewMemoryStore()
	family(t, m, 20)
	g, err := OpenGroup(ctx, m, "")
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := g.Children(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("children: %v, want it cancelled", err)
	}
}

// countGets counts the reads of a store.
type countGets struct {
	Store
	mu   sync.Mutex
	gets []string
}

func (s *countGets) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets = append(s.gets, key)
	s.mu.Unlock()
	return s.Store.Get(ctx, key)
}

func TestOpenArraysReadsEachChildOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		names := family(t, s, 12)
		a, err := OpenArray(ctx, s, "a004")
		if err != nil {
			t.Fatal(err)
		}
		if err := Write(ctx, a, nil, nil, []int32{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
		c := &countGets{Store: s}
		g, err := OpenGroup(ctx, c, "")
		if err != nil {
			t.Fatal(err)
		}
		c.gets = nil
		arrays, err := g.OpenArrays(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// A read for each child, the group and the broken one among them.
		if len(c.gets) != len(names)+2 {
			t.Errorf("%d reads, want %d: %v", len(c.gets), len(names)+2, c.gets)
		}
		var paths []string
		for _, a := range arrays {
			paths = append(paths, a.Path())
		}
		if !slices.Equal(paths, names) {
			t.Errorf("arrays = %v, want %v", paths, names)
		}
		got, err := Read[int32](ctx, arrays[4], nil, nil)
		if err != nil || !slices.Equal(got, []int32{1, 2, 3, 4}) {
			t.Errorf("read of %s = %v, %v", arrays[4].Path(), got, err)
		}
		if !slices.Equal(arrays[4].Metadata().Shape, a.Metadata().Shape) {
			t.Errorf("metadata = %+v, want %+v", arrays[4].Metadata(), a.Metadata())
		}
	})
}

func TestOpenArraysOfASubgroupAndOfNone(t *testing.T) {
	s := NewMemoryStore()
	family(t, s, 0)
	mustArray(t, s, "sub/bui", ArrayOptions{Shape: []int{2}, ChunkShape: []int{1}, DataType: Int32})
	g, err := OpenGroup(ctx, s, "")
	if err != nil {
		t.Fatal(err)
	}
	if arrays, err := g.OpenArrays(ctx); err != nil || len(arrays) != 0 {
		t.Errorf("arrays of the root = %v, %v", arrays, err)
	}
	sub, err := g.OpenGroup(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	arrays, err := sub.OpenArrays(ctx)
	if err != nil || len(arrays) != 1 || arrays[0].Path() != "sub/bui" {
		t.Errorf("arrays of sub = %v, %v", arrays, err)
	}
}

// An array OpenArray cannot open, OpenArrays cannot either, and says so the
// same way.
func TestOpenArraysFailsOnAnArrayThatDoesNotOpen(t *testing.T) {
	for name, meta := range map[string]string{
		"unsupported data type": `{"zarr_format": 3, "node_type": "array", "shape": [2], "data_type": "float4",
			"chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [1]}},
			"chunk_key_encoding": {"name": "default"}, "fill_value": 0, "codecs": [{"name": "bytes"}]}`,
		"unknown field": `{"zarr_format": 3, "node_type": "array", "shape": [2], "data_type": "int32",
			"chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [1]}},
			"chunk_key_encoding": {"name": "default"}, "fill_value": 0, "codecs": [{"name": "bytes"}],
			"mystery": {}}`,
	} {
		t.Run(name, func(t *testing.T) {
			s := NewMemoryStore()
			family(t, s, 2)
			if err := s.Set(ctx, "odd/zarr.json", []byte(meta)); err != nil {
				t.Fatal(err)
			}
			_, want := OpenArray(ctx, s, "odd")
			if want == nil {
				t.Fatal("OpenArray opened it")
			}
			g, err := OpenGroup(ctx, s, "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.OpenArrays(ctx); err == nil || err.Error() != want.Error() {
				t.Errorf("open arrays: %v, want %v", err, want)
			}
		})
	}
}
