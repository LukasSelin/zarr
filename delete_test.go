package zarr

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// filled is an array at path holding data that is not its fill value, so
// that every one of its chunks is written.
func filledArray(t *testing.T, s Store, path string, o ArrayOptions) *Array {
	t.Helper()
	a := mustArray(t, s, path, o)
	n := product(o.Shape)
	data := make([]int32, n)
	for i := range data {
		data[i] = int32(i + 1)
	}
	if err := Write(ctx, a, make([]int, len(o.Shape)), o.Shape, data); err != nil {
		t.Fatal(err)
	}
	return a
}

// failingDelete fails to delete the keys ending in a suffix.
type failingDelete struct {
	*MemoryStore
	suffix string
}

func (s failingDelete) Delete(c context.Context, key string) error {
	if strings.HasSuffix(key, s.suffix) {
		return fmt.Errorf("zarr: cannot delete %s", key)
	}
	return s.MemoryStore.Delete(c, key)
}

func TestDeletingAnArrayTakesItsChunks(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		if _, err := CreateGroup(ctx, s, "", nil); err != nil {
			t.Fatal(err)
		}
		a := filledArray(t, s, "height", ArrayOptions{
			Shape: []int{4, 4}, ChunkShape: []int{2, 2}, DataType: Int32,
		})
		if len(listed(t, s, "height/")) < 5 {
			t.Fatalf("the array was not written: %v", listed(t, s, "height/"))
		}
		if err := a.Delete(ctx); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, "height/"); len(got) != 0 {
			t.Errorf("after deleting the array: %v", got)
		}
		if got := listed(t, s, ""); !slices.Equal(got, []string{"zarr.json"}) {
			t.Errorf("the group around it: %v", got)
		}
		if _, err := OpenArray(ctx, s, "height"); !errors.Is(err, ErrNotFound) {
			t.Errorf("opening a deleted array: %v", err)
		}
	})
}

func TestDeletingAGroupTakesTheNodesUnderIt(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		if _, err := CreateGroup(ctx, s, "", nil); err != nil {
			t.Fatal(err)
		}
		g, err := CreateGroup(ctx, s, "fwi", nil)
		if err != nil {
			t.Fatal(err)
		}
		filledArray(t, s, "fwi/isi", ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32,
		})
		if _, err := CreateGroup(ctx, s, "fwi/sub", nil); err != nil {
			t.Fatal(err)
		}
		filledArray(t, s, "fwi/sub/bui", ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32,
		})
		if err := g.Delete(ctx); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, ""); !slices.Equal(got, []string{"zarr.json"}) {
			t.Errorf("after deleting the group: %v", got)
		}
	})
}

func TestDeletingTheRootEmptiesTheStore(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		g, err := CreateGroup(ctx, s, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		filledArray(t, s, "height", ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32,
		})
		if err := g.Delete(ctx); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, ""); len(got) != 0 {
			t.Errorf("after deleting the root: %v", got)
		}
	})
}

func TestDeletingANodeLeavesItsNeighbour(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		filledArray(t, s, "height", ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32,
		})
		filledArray(t, s, "heightmap", ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32,
		})
		before := listed(t, s, "heightmap/")
		if err := Delete(ctx, s, "height"); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, "height/"); len(got) != 0 {
			t.Errorf("after deleting height: %v", got)
		}
		if got := listed(t, s, "heightmap/"); !slices.Equal(got, before) {
			t.Errorf("heightmap = %v, want %v", got, before)
		}
	})
}

func TestANodeWhoseMetadataIsBrokenIsStillDeleted(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		filledArray(t, s, "height", ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32,
		})
		if err := s.Set(ctx, "height/zarr.json", []byte("not json at all")); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenArray(ctx, s, "height"); err == nil {
			t.Fatal("the metadata was meant to be broken")
		}
		if err := Delete(ctx, s, "height"); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, ""); len(got) != 0 {
			t.Errorf("after deleting it: %v", got)
		}
	})
}

func TestADeleteThatFailsPartWayLeavesNothingThatOpens(t *testing.T) {
	m := NewMemoryStore()
	filledArray(t, m, "height", ArrayOptions{
		Shape: []int{4, 4}, ChunkShape: []int{2, 2}, DataType: Int32,
	})
	if err := Delete(ctx, failingDelete{m, "c/1/1"}, "height"); err == nil {
		t.Fatal("the delete was meant to fail")
	}
	// The metadata goes first, so what is left is keys nothing opens rather
	// than an array whose missing chunks read as the fill value.
	if _, err := OpenArray(ctx, m, "height"); !errors.Is(err, ErrNotFound) {
		t.Errorf("opening what was half deleted: %v", err)
	}
	if got := listed(t, m, ""); len(got) == 0 {
		t.Fatal("the delete was meant to fail part way")
	}
	// Doing it again clears what was left.
	if err := Delete(ctx, m, "height"); err != nil {
		t.Fatal(err)
	}
	if got := listed(t, m, ""); len(got) != 0 {
		t.Errorf("after deleting it again: %v", got)
	}
}

func TestDeletingWhatIsNotThereIsNotAnError(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		if err := Delete(ctx, s, "never/written"); err != nil {
			t.Errorf("deleting a path with no node: %v", err)
		}
		if err := Delete(ctx, s, ""); err != nil {
			t.Errorf("deleting the root of an empty store: %v", err)
		}
	})
}

func TestDeletingAnArrayWithV2Keys(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		a := mustArray(t, s, "height", ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: Int32,
		})
		// The v2 encoding puts the chunks directly under the array, beside
		// its metadata, rather than under a "c" of their own.
		m := a.meta
		m.ChunkKeyEncoding = keyEncoding{v2: true, sep: "."}.named()
		if err := writeMetadata(ctx, s, "height", m); err != nil {
			t.Fatal(err)
		}
		b, err := OpenArray(ctx, s, "height")
		if err != nil {
			t.Fatal(err)
		}
		if err := Write(ctx, b, []int{0}, []int{4}, []int32{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, "height/"); !slices.Equal(got, []string{"height/0", "height/1", "height/zarr.json"}) {
			t.Fatalf("the v2 layout is not what it was: %v", got)
		}
		if err := b.Delete(ctx); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, ""); len(got) != 0 {
			t.Errorf("after deleting it: %v", got)
		}
	})
}

func TestDeletingAnArrayOfMoreChunksThanOneBatch(t *testing.T) {
	s := NewMemoryStore()
	a := filledArray(t, s, "height", ArrayOptions{
		Shape: []int{1001}, ChunkShape: []int{1}, DataType: Int32,
	})
	if got := len(listed(t, s, "height/")); got != 1002 {
		t.Fatalf("%d keys, want a chunk each and the metadata", got)
	}
	if err := a.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if got := listed(t, s, ""); len(got) != 0 {
		t.Errorf("after deleting it: %v", got)
	}
}

func TestAGroupListsItsChildren(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		g, err := CreateGroup(ctx, s, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		filledArray(t, s, "isi", ArrayOptions{Shape: []int{2}, ChunkShape: []int{1}, DataType: Int32})
		filledArray(t, s, "fwi", ArrayOptions{Shape: []int{2}, ChunkShape: []int{1}, DataType: Int32})
		if _, err := CreateGroup(ctx, s, "sub", nil); err != nil {
			t.Fatal(err)
		}
		filledArray(t, s, "sub/bui", ArrayOptions{Shape: []int{2}, ChunkShape: []int{1}, DataType: Int32})
		want := []Child{{"fwi", "array"}, {"isi", "array"}, {"sub", "group"}}
		got, err := g.Children(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Errorf("children = %v, want %v", got, want)
		}
		sub, err := g.OpenGroup(ctx, "sub")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := sub.Children(ctx); err != nil || !slices.Equal(got, []Child{{"bui", "array"}}) {
			t.Errorf("children of sub = %v, %v", got, err)
		}
	})
}

func TestAChildWithNoMetadataIsNotAChild(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		g, err := CreateGroup(ctx, s, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		filledArray(t, s, "isi", ArrayOptions{Shape: []int{2}, ChunkShape: []int{1}, DataType: Int32})
		if err := Delete(ctx, s, "isi"); err != nil {
			t.Fatal(err)
		}
		// A DirStore keeps the directories the chunks were in; they are not
		// nodes, and the chunks of an array are not nodes either.
		got, err := g.Children(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Errorf("children = %v, want none", got)
		}
	})
}

func TestAChildWhoseMetadataIsBrokenIsStillAChild(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		g, err := CreateGroup(ctx, s, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Set(ctx, "broken/zarr.json", []byte("not json at all")); err != nil {
			t.Fatal(err)
		}
		got, err := g.Children(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Finding it with no type is how it is deleted.
		if !slices.Equal(got, []Child{{"broken", ""}}) {
			t.Fatalf("children = %v", got)
		}
		if err := Delete(ctx, s, got[0].Name); err != nil {
			t.Fatal(err)
		}
		if got := listed(t, s, ""); !slices.Equal(got, []string{"zarr.json"}) {
			t.Errorf("after deleting it: %v", got)
		}
	})
}

func TestChildrenOfAStoreThatCannotListOneLevel(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		if _, err := CreateGroup(ctx, s, "", nil); err != nil {
			t.Fatal(err)
		}
		filledArray(t, s, "isi", ArrayOptions{Shape: []int{2}, ChunkShape: []int{1}, DataType: Int32})
		if _, err := CreateGroup(ctx, s, "sub", nil); err != nil {
			t.Fatal(err)
		}
		g, err := OpenGroup(ctx, plainStore{s}, "")
		if err != nil {
			t.Fatal(err)
		}
		got, err := g.Children(ctx)
		if err != nil {
			t.Fatal(err)
		}
		want := []Child{{"isi", "array"}, {"sub", "group"}}
		if !slices.Equal(got, want) {
			t.Errorf("children = %v, want %v", got, want)
		}
	})
}
