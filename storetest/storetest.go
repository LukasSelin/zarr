// Package storetest checks that a zarr.Store keeps the contract: the keys and
// prefixes it must refuse, what Get, Set, Delete and List do to each other,
// and the one level of a listing. It is as much for a store kept outside this
// repository as for the ones in it.
//
//	func TestMyStore(t *testing.T) {
//		storetest.Run(t, func() zarr.Store { return myStore() })
//	}
//
// RunReadOnly is the same for a store that cannot be written, given what it
// holds.
package storetest

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/LukasSelin/zarr"
)

// seed is the little hierarchy every listing check is made against: a root
// group over an array and another whose name begins the same way.
var seed = []string{
	"zarr.json",
	"a/zarr.json",
	"a/c/0/0",
	"a/c/0/1",
	"ab/zarr.json",
}

// Run checks the store fresh returns. fresh must return an empty store, and
// is called more than once.
func Run(t *testing.T, fresh func() zarr.Store) {
	ctx := context.Background()

	t.Run("a key comes back and then goes", func(t *testing.T) {
		s := fresh()
		if _, err := s.Get(ctx, "a/c/0/0"); !errors.Is(err, zarr.ErrNotFound) {
			t.Errorf("get of a key never written: %v, want ErrNotFound", err)
		}
		if err := s.Delete(ctx, "a/c/0/0"); err != nil {
			t.Errorf("delete of a key never written: %v", err)
		}
		if err := s.Set(ctx, "a/c/0/0", []byte("value")); err != nil {
			t.Fatal(err)
		}
		if got, err := s.Get(ctx, "a/c/0/0"); err != nil || string(got) != "value" {
			t.Errorf("get %q, %v", got, err)
		}
		if err := s.Set(ctx, "a/c/0/0", []byte("again")); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Get(ctx, "a/c/0/0"); string(got) != "again" {
			t.Errorf("set over a key: %q", got)
		}
		if err := s.Delete(ctx, "a/c/0/0"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "a/c/0/0"); !errors.Is(err, zarr.ErrNotFound) {
			t.Errorf("get of a deleted key: %v, want ErrNotFound", err)
		}
	})

	t.Run("keys it must refuse", func(t *testing.T) {
		s := fresh()
		for _, bad := range []string{"", "/a", "a/", "a//b", "a/./b", "a/../b", `a\b`, "a:b"} {
			if err := s.Set(ctx, bad, []byte("x")); err == nil {
				t.Errorf("set %q was allowed", bad)
			}
		}
	})

	t.Run("prefixes it must refuse", func(t *testing.T) {
		s := fresh()
		for _, bad := range []string{"/a", "a//b", "a/./b", "a/../b", "..", "../outside", `a\b`, "a:b"} {
			if err := s.List(ctx, bad, func(string) error { return nil }); err == nil {
				t.Errorf("list %q was allowed", bad)
			}
		}
		for _, ok := range []string{"", "a", "a/", "a/c/"} {
			if err := s.List(ctx, ok, func(string) error { return nil }); err != nil {
				t.Errorf("list %q: %v", ok, err)
			}
		}
	})

	t.Run("listing under a prefix", func(t *testing.T) {
		listing(ctx, t, seeded(ctx, t, fresh))
	})

	t.Run("listing stops at the error of the callback", func(t *testing.T) {
		listingStops(ctx, t, seeded(ctx, t, fresh))
	})

	t.Run("one level of a listing", func(t *testing.T) {
		oneLevel(ctx, t, seeded(ctx, t, fresh))
	})
}

// RunReadOnly checks a store that cannot be written, such as a
// zarr.HTTPStore. open must return a store holding the keys and values of
// seed, and is called more than once. It checks Get against them, and
// GetRange if the store is a zarr.RangeGetter; that Set and Delete return an
// error wrapping zarr.ErrReadOnly and change nothing; and the listings Run
// checks, unless List returns an error wrapping errors.ErrUnsupported, which
// says the store has no way to list. ListDir must then return one too.
func RunReadOnly(t *testing.T, open func(seed map[string][]byte) zarr.Store) {
	ctx := context.Background()
	values := map[string][]byte{}
	for _, k := range seed {
		values[k] = []byte(k)
	}

	t.Run("the keys it holds", func(t *testing.T) {
		s := open(values)
		for k, v := range values {
			if got, err := s.Get(ctx, k); err != nil || string(got) != string(v) {
				t.Errorf("get %q: %q, %v", k, got, err)
			}
		}
		for _, k := range []string{"a/c/9/9", "nothing/zarr.json", "b"} {
			if _, err := s.Get(ctx, k); !errors.Is(err, zarr.ErrNotFound) {
				t.Errorf("get of %q, which it does not hold: %v, want ErrNotFound", k, err)
			}
		}
	})

	t.Run("it refuses to be written", func(t *testing.T) {
		s := open(values)
		for _, k := range []string{"a/zarr.json", "a/c/9/9"} {
			if err := s.Set(ctx, k, []byte("x")); !errors.Is(err, zarr.ErrReadOnly) {
				t.Errorf("set %q: %v, want ErrReadOnly", k, err)
			}
			if err := s.Delete(ctx, k); !errors.Is(err, zarr.ErrReadOnly) {
				t.Errorf("delete %q: %v, want ErrReadOnly", k, err)
			}
		}
		if got, err := s.Get(ctx, "a/zarr.json"); err != nil || string(got) != "a/zarr.json" {
			t.Errorf("after a refused set and delete: %q, %v", got, err)
		}
	})

	if _, ok := open(values).(zarr.RangeGetter); ok {
		t.Run("ranges", func(t *testing.T) {
			ranges(ctx, t, open(values).(zarr.RangeGetter))
		})
	}

	t.Run("listing", func(t *testing.T) {
		s := open(values)
		err := s.List(ctx, "", func(string) error { return nil })
		if errors.Is(err, errors.ErrUnsupported) {
			if err := zarr.ListDir(ctx, s, "", func(string) error { return nil }); !errors.Is(err, errors.ErrUnsupported) {
				t.Errorf("list says it cannot list and list one level %v", err)
			}
			t.Skip("the store cannot list")
		}
		listing(ctx, t, s)
		listingStops(ctx, t, s)
		oneLevel(ctx, t, s)
	})
}

// ranges checks the ranges of "a/zarr.json", whose value is its own key.
func ranges(ctx context.Context, t *testing.T, s zarr.RangeGetter) {
	const key, v = "a/zarr.json", "a/zarr.json"
	n := int64(len(v))
	for _, c := range []struct {
		offset, length int64
		want           string
	}{
		{0, n, v},
		{2, 4, "zarr"},
		{n - 1, 1, "n"},
		{-4, 4, "json"},
		{-4, 2, "js"},
		{-n, n, v},
		{-1, 0, ""},
		{5, 0, ""},
		{n, 0, ""},
	} {
		got, err := s.GetRange(ctx, key, c.offset, c.length)
		if err != nil || string(got) != c.want {
			t.Errorf("range %d+%d: %q, %v; want %q", c.offset, c.length, got, err, c.want)
		}
	}
	for _, c := range [][2]int64{{n - 2, 3}, {n, 1}, {n + 1, 0}, {-n - 1, n + 1}, {-2, 3}, {0, -1}} {
		if got, err := s.GetRange(ctx, key, c[0], c[1]); err == nil {
			t.Errorf("range %d+%d is outside and read %q", c[0], c[1], got)
		}
	}
	for _, c := range [][2]int64{{-4, 4}, {0, 4}, {0, 0}} {
		if _, err := s.GetRange(ctx, "a/c/9/9", c[0], c[1]); !errors.Is(err, zarr.ErrNotFound) {
			t.Errorf("range %d+%d of a key it does not hold: %v, want ErrNotFound", c[0], c[1], err)
		}
	}
}

// listing checks List of a store holding the seed keys.
func listing(ctx context.Context, t *testing.T, s zarr.Store) {
	t.Helper()
	for _, c := range []struct {
		prefix string
		want   []string
	}{
		{"", seed},
		{"a/", []string{"a/c/0/0", "a/c/0/1", "a/zarr.json"}},
		{"a/c/", []string{"a/c/0/0", "a/c/0/1"}},
		// A prefix is a string and not a path, so "a" is "ab" too.
		{"a", []string{"a/c/0/0", "a/c/0/1", "a/zarr.json", "ab/zarr.json"}},
		{"nothing/", nil},
		{"zarr.json", []string{"zarr.json"}},
	} {
		if got := list(ctx, t, s, c.prefix); !same(got, c.want) {
			t.Errorf("list %q = %v, want %v", c.prefix, got, c.want)
		}
	}
}

// listingStops checks that List stops at the first error of its callback.
func listingStops(ctx context.Context, t *testing.T, s zarr.Store) {
	t.Helper()
	stop := errors.New("stop here")
	n := 0
	err := s.List(ctx, "", func(string) error {
		n++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Errorf("list returned %v, want the error of the callback", err)
	}
	if n != 1 {
		t.Errorf("the callback ran %d times after it returned an error", n)
	}
}

// oneLevel checks zarr.ListDir of a store holding the seed keys.
func oneLevel(ctx context.Context, t *testing.T, s zarr.Store) {
	t.Helper()
	for _, c := range []struct {
		prefix string
		want   []string
	}{
		{"", []string{"a/", "ab/", "zarr.json"}},
		{"a/", []string{"c/", "zarr.json"}},
		{"a/c/", []string{"0/"}},
	} {
		var got []string
		err := zarr.ListDir(ctx, s, c.prefix, func(name string) error {
			got = append(got, name)
			return nil
		})
		if err != nil {
			t.Fatalf("list one level of %q: %v", c.prefix, err)
		}
		if !same(got, c.want) {
			t.Errorf("one level of %q = %v, want %v", c.prefix, got, c.want)
		}
	}
}

// seeded is a store holding the seed keys.
func seeded(ctx context.Context, t *testing.T, fresh func() zarr.Store) zarr.Store {
	t.Helper()
	s := fresh()
	for _, k := range seed {
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// list is every key of s under prefix.
func list(ctx context.Context, t *testing.T, s zarr.Store, prefix string) []string {
	t.Helper()
	var got []string
	if err := s.List(ctx, prefix, func(key string) error {
		got = append(got, key)
		return nil
	}); err != nil {
		t.Fatalf("list %q: %v", prefix, err)
	}
	return got
}

// same says whether two listings hold the same names, in whatever order the
// store gave them.
func same(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	g, w := append([]string(nil), got...), append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	for i := range g {
		if g[i] != w[i] {
			return false
		}
	}
	return true
}
