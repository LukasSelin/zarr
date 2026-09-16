package zarr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

// grown is all, a C-order array of shape, grown along axis by the slices of
// more, each across that axis.
func grown(all []int32, shape []int, axis int, more []int32) ([]int32, []int) {
	across := 1
	for k, v := range shape {
		if k != axis {
			across *= v
		}
	}
	n := len(more) / across
	next := slices.Clone(shape)
	next[axis] += n
	out := make([]int32, product(next))
	at := make([]int, len(shape))
	copyBlock(out, next, at, all, shape, at, shape)
	at[axis] = shape[axis]
	region := slices.Clone(shape)
	region[axis] = n
	copyBlock(out, next, at, more, region, make([]int, len(shape)), region)
	return out, next
}

func TestAppendsReadBackAlongEachAxis(t *testing.T) {
	for _, shards := range []bool{false, true} {
		for axis := range 3 {
			s := NewMemoryStore()
			o := ArrayOptions{Shape: []int{2, 3, 5}, ChunkShape: []int{2, 2, 2}, DataType: Int32, FillValue: -1}
			o.Shape[axis] = 0
			if shards {
				o.ShardShape = []int{4, 4, 4}
			}
			a := mustArray(t, s, "t", o)
			var all []int32
			shape := a.Shape()
			next := int32(0)
			for _, n := range []int{1, 2, 3, 1, 4} {
				across := 1
				for k, v := range shape {
					if k != axis {
						across *= v
					}
				}
				more := make([]int32, n*across)
				for i := range more {
					more[i], next = next, next+1
				}
				at, err := Append(ctx, a, axis, more)
				if err != nil {
					t.Fatal(err)
				}
				if at != shape[axis] {
					t.Errorf("appended at %d, not %d", at, shape[axis])
				}
				all, shape = grown(all, shape, axis, more)
				if !slices.Equal(a.Shape(), shape) {
					t.Fatalf("shape %v, not %v", a.Shape(), shape)
				}
				got, err := Read[int32](ctx, a, nil, nil)
				if err != nil || !slices.Equal(got, all) {
					t.Fatalf("shards %v, axis %d: read %v, %v; want %v", shards, axis, got, err, all)
				}
			}
			b, err := OpenArray(ctx, s, "t")
			if err != nil {
				t.Fatal(err)
			}
			if got, err := Read[int32](ctx, b, nil, nil); err != nil || !slices.Equal(got, all) {
				t.Errorf("shards %v, axis %d: reopened, read %v, %v", shards, axis, got, err)
			}
		}
	}
}

func TestShrinkingThenGrowingReadsFill(t *testing.T) {
	for _, shards := range []bool{false, true} {
		s := NewMemoryStore()
		o := ArrayOptions{Shape: []int{9, 7}, ChunkShape: []int{2, 3}, DataType: Float32, FillValue: math.NaN()}
		if shards {
			o.ShardShape = []int{4, 6}
		}
		a := mustArray(t, s, "", o)
		all := make([]float32, 63)
		for i := range all {
			all[i] = float32(i)
		}
		if err := Write(ctx, a, nil, nil, all); err != nil {
			t.Fatal(err)
		}
		if err := a.Resize(ctx, []int{3, 4}); err != nil {
			t.Fatal(err)
		}
		for _, k := range s.Keys() {
			if k == "zarr.json" {
				continue
			}
			var i, j int
			if _, err := fmt.Sscanf(k, "c/%d/%d", &i, &j); err != nil {
				t.Fatal(err)
			}
			if i*a.grid[0] >= 3 || j*a.grid[1] >= 4 {
				t.Errorf("shards %v: %s is past the new end and was kept", shards, k)
			}
		}
		if err := a.Resize(ctx, []int{9, 7}); err != nil {
			t.Fatal(err)
		}
		got, err := Read[float32](ctx, a, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i, v := range got {
			y, x := i/7, i%7
			if y < 3 && x < 4 {
				if v != all[i] {
					t.Errorf("shards %v: [%d %d] is %v, not %v", shards, y, x, v, all[i])
				}
			} else if !math.IsNaN(float64(v)) {
				t.Errorf("shards %v: [%d %d] is %v after shrinking past it, not the fill", shards, y, x, v)
			}
		}
	}
}

func TestWriteChunkKeepsNothingPastTheEnd(t *testing.T) {
	for _, shards := range []bool{false, true} {
		o := ArrayOptions{Shape: []int{5}, ChunkShape: []int{4}, DataType: Int16, FillValue: -1}
		if shards {
			o.ShardShape = []int{8}
		}
		a := mustArray(t, NewMemoryStore(), "", o)
		chunk := []int16{9, 9, 9, 9}
		if err := WriteChunk(ctx, a, []int{1}, chunk); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(chunk, []int16{9, 9, 9, 9}) {
			t.Errorf("shards %v: WriteChunk changed what it was given: %v", shards, chunk)
		}
		if err := a.Resize(ctx, []int{8}); err != nil {
			t.Fatal(err)
		}
		got, err := Read[int16](ctx, a, nil, nil)
		if want := []int16{-1, -1, -1, -1, 9, -1, -1, -1}; err != nil || !slices.Equal(got, want) {
			t.Errorf("shards %v: read %v, %v; want %v", shards, got, err, want)
		}
	}
}

// failingSet is a MemoryStore whose Set of a key with the suffix fails.
type failingSet struct {
	*MemoryStore
	suffix string
}

func (s failingSet) Set(ctx context.Context, key string, value []byte) error {
	if strings.HasSuffix(key, s.suffix) {
		return errors.New("failing " + key)
	}
	return s.MemoryStore.Set(ctx, key, value)
}

func TestAnAppendThatFailsLeavesTheShape(t *testing.T) {
	m := NewMemoryStore()
	a := mustArray(t, m, "", ArrayOptions{Shape: []int{0, 2}, ChunkShape: []int{3, 2}, DataType: Int32})
	if _, err := Append(ctx, a, 0, []int32{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	b, err := OpenArray(ctx, failingSet{m, "zarr.json"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Append(ctx, b, 0, []int32{5, 6}); err == nil {
		t.Fatal("an append whose metadata failed to write did not fail")
	}
	if !slices.Equal(b.Shape(), []int{2, 2}) {
		t.Errorf("shape %v after a failed append", b.Shape())
	}
	if err := a.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := Read[int32](ctx, a, nil, nil); err != nil || !slices.Equal(got, []int32{1, 2, 3, 4}) {
		t.Errorf("read %v, %v after a failed append", got, err)
	}
	// Doing it again where it can be done writes over what it left.
	if _, err := Append(ctx, a, 0, []int32{7, 8}); err != nil {
		t.Fatal(err)
	}
	if got, err := Read[int32](ctx, a, nil, nil); err != nil || !slices.Equal(got, []int32{1, 2, 3, 4, 7, 8}) {
		t.Errorf("read %v, %v", got, err)
	}
}

func TestRefreshSeesAnotherHandlesAppend(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "x", ArrayOptions{Shape: []int{0}, ChunkShape: []int{4}, DataType: Uint8})
	b, err := OpenArray(ctx, s, "x")
	if err != nil {
		t.Fatal(err)
	}
	b.WriteEmptyChunks = true
	if _, err := Append(ctx, a, 0, []uint8{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := b.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(b.Shape(), []int{3}) || !b.WriteEmptyChunks {
		t.Errorf("refreshed shape %v, WriteEmptyChunks %v", b.Shape(), b.WriteEmptyChunks)
	}
}

func TestAppendsAndResizesThatCannotBe(t *testing.T) {
	a := mustArray(t, NewMemoryStore(), "", ArrayOptions{Shape: []int{2, 0}, ChunkShape: []int{2, 2}, DataType: Int32})
	for name, err := range map[string]error{
		"axis past the dimensions": second(Append(ctx, a, 2, []int32{1, 2})),
		"negative axis":            second(Append(ctx, a, -1, []int32{1, 2})),
		"nothing across the axis":  second(Append(ctx, a, 0, []int32{1, 2})),
		"not whole slices":         second(Append(ctx, a, 1, []int32{1, 2, 3})),
		"wrong element type":       second(Append(ctx, a, 1, []float64{1, 2})),
		"resize to other dims":     a.Resize(ctx, []int{2}),
		"resize to negative":       a.Resize(ctx, []int{2, -1}),
	} {
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if !slices.Equal(a.Shape(), []int{2, 0}) {
		t.Errorf("shape %v", a.Shape())
	}
	if at, err := Append(ctx, a, 1, []int32{}); err != nil || at != 0 {
		t.Errorf("appending nothing: %d, %v", at, err)
	}
}

func second[T any](_ T, err error) error { return err }
