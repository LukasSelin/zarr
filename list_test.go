package zarr

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// seeded fills a store with a little hierarchy: a root group over an array
// and another whose name begins the same way.
func seeded(t *testing.T, s Store) Store {
	t.Helper()
	for _, k := range []string{"zarr.json", "a/zarr.json", "a/c/0/0", "a/c/0/1", "ab/zarr.json"} {
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// listed is every key of s under prefix, sorted.
func listed(t *testing.T, s Store, prefix string) []string {
	t.Helper()
	var keys []string
	if err := s.List(ctx, prefix, func(key string) error {
		keys = append(keys, key)
		return nil
	}); err != nil {
		t.Fatalf("list %q: %v", prefix, err)
	}
	slices.Sort(keys)
	return keys
}

// eachStore runs f over a store of each kind.
func eachStore(t *testing.T, f func(t *testing.T, s Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { f(t, NewMemoryStore()) })
	t.Run("directory", func(t *testing.T) { f(t, NewDirStore(t.TempDir())) })
}

func TestListingStopsAtTheErrorOfTheCallback(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		seeded(t, s)
		// fs.SkipDir is an error filepath.WalkDir reads as one of its own,
		// which a DirStore must hand back rather than act on.
		for _, stop := range []error{errors.New("stop here"), fs.SkipDir, fs.SkipAll} {
			n := 0
			err := s.List(ctx, "", func(string) error {
				n++
				return stop
			})
			if !errors.Is(err, stop) {
				t.Errorf("list returned %v, want %v", err, stop)
			}
			if n != 1 {
				t.Errorf("the callback ran %d times after returning %v", n, stop)
			}
		}
	})
}

func TestOneLevelIsDerivedFromAStoreThatCannotListIt(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		seeded(t, s)
		for _, prefix := range []string{"", "a/", "a/c/"} {
			var own, derived []string
			if err := ListDir(ctx, s, prefix, func(name string) error {
				own = append(own, name)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// plainStore hides DirLister as it hides RangeGetter.
			if err := ListDir(ctx, plainStore{s}, prefix, func(name string) error {
				derived = append(derived, name)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			slices.Sort(own)
			slices.Sort(derived)
			if !slices.Equal(own, derived) {
				t.Errorf("one level of %q: the store says %v, derived from List %v", prefix, own, derived)
			}
		}
	})
}

func TestADirectoryStoreDoesNotListWhatASetIsWriting(t *testing.T) {
	dir := t.TempDir()
	s := NewDirStore(dir)
	seeded(t, s)
	// A Set writes to a file beside the key's, named with a dot in front,
	// and renames it into place; a listing that ran while it did must not
	// take the half-written one for a key.
	if err := os.WriteFile(filepath.Join(dir, "a", "c", "0", ".0.1234567"), []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".hidden", "c"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".hidden", "c", "0"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := []string{"a/c/0/0", "a/c/0/1", "a/zarr.json", "ab/zarr.json", "zarr.json"}
	if got := listed(t, s, ""); !slices.Equal(got, want) {
		t.Errorf("list = %v, want %v", got, want)
	}

	// And the same while Sets are really running.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 16 {
				key := "a/c/" + string(rune('0'+i)) + "/" + string(rune('0'+j%10))
				if err := s.Set(ctx, key, []byte("value")); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.List(ctx, "", func(key string) error {
				for _, seg := range strings.Split(key, "/") {
					if strings.HasPrefix(seg, ".") {
						t.Errorf("listed %q, which no Set wrote", key)
					}
				}
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestListingADirectoryStoreThatIsNotThereYet(t *testing.T) {
	s := NewDirStore(filepath.Join(t.TempDir(), "never", "written"))
	if got := listed(t, s, ""); len(got) != 0 {
		t.Errorf("list = %v, want nothing", got)
	}
	if err := ListDir(ctx, s, "", func(string) error {
		t.Error("a name in a store that is not there")
		return nil
	}); err != nil {
		t.Errorf("one level: %v", err)
	}
}

func TestAListingPrefixThatReachesOutside(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "outside"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := NewDirStore(filepath.Join(dir, "store"))
	for _, bad := range []string{"..", "../", "../outside", "/a", "a/../b", `a\b`, "a:b", "a//b"} {
		if err := s.List(ctx, bad, func(key string) error {
			t.Errorf("list %q reached %q", bad, key)
			return nil
		}); err == nil {
			t.Errorf("list %q was allowed", bad)
		}
		if err := s.ListDir(ctx, bad, func(name string) error {
			t.Errorf("one level of %q reached %q", bad, name)
			return nil
		}); err == nil {
			t.Errorf("one level of %q was allowed", bad)
		}
	}
}
