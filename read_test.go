package zarr

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// offsetOf is the offset of p in a C-order array of shape.
func offsetOf(shape, p []int) int {
	i := 0
	for k := range p {
		i = i*shape[k] + p[k]
	}
	return i
}

// regionOf is the block of shape n at start in all, an array of shape, read
// one element at a time.
func regionOf[T any](all []T, shape, start, n []int) []T {
	out := make([]T, 0, product(n))
	if product(n) == 0 {
		return out
	}
	eachIndex(make([]int, len(n)), minus(n, slices.Repeat([]int{1}, len(n))), func(p []int) error {
		out = append(out, all[offsetOf(shape, plus(start, p))])
		return nil
	})
	return out
}

// randomBlock is a block of an array of shape: where it begins and its shape.
func randomBlock(r *rand.Rand, shape []int) (start, n []int) {
	start, n = make([]int, len(shape)), make([]int, len(shape))
	for k, s := range shape {
		start[k] = r.IntN(s + 1)
		n[k] = r.IntN(s - start[k] + 1)
		if r.IntN(3) == 0 {
			// Whole along this dimension, which makes runs span dimensions.
			start[k], n[k] = 0, s
		}
	}
	return start, n
}

func TestCopyBlockAndFillBlockCopyWhatOneElementAtATimeDoes(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		d := r.IntN(5)
		dstShape, srcShape := make([]int, d), make([]int, d)
		for k := range d {
			dstShape[k], srcShape[k] = 1+r.IntN(5), 1+r.IntN(5)
			if r.IntN(2) == 0 {
				srcShape[k] = dstShape[k]
			}
		}
		n := make([]int, d)
		dstAt, srcAt := make([]int, d), make([]int, d)
		for k := range d {
			n[k] = r.IntN(min(dstShape[k], srcShape[k]) + 1)
			dstAt[k] = r.IntN(dstShape[k] - n[k] + 1)
			srcAt[k] = r.IntN(srcShape[k] - n[k] + 1)
		}
		src := make([]int, product(srcShape))
		for i := range src {
			src[i] = i + 1
		}
		got, want := make([]int, product(dstShape)), make([]int, product(dstShape))
		copyBlock(got, dstShape, dstAt, src, srcShape, srcAt, n)
		filledGot, filledWant := make([]int, len(got)), make([]int, len(got))
		fillBlock(filledGot, dstShape, dstAt, n, -1)
		if product(n) > 0 {
			eachIndex(make([]int, d), minus(n, slices.Repeat([]int{1}, d)), func(p []int) error {
				want[offsetOf(dstShape, plus(dstAt, p))] = src[offsetOf(srcShape, plus(srcAt, p))]
				filledWant[offsetOf(dstShape, plus(dstAt, p))] = -1
				return nil
			})
		}
		if !slices.Equal(got, want) {
			t.Fatalf("copy of %v from %v in %v to %v in %v:\n got %v\nwant %v", n, srcAt, srcShape, dstAt, dstShape, got, want)
		}
		if !slices.Equal(filledGot, filledWant) {
			t.Fatalf("fill of %v at %v in %v:\n got %v\nwant %v", n, dstAt, dstShape, filledGot, filledWant)
		}
	}
}

// TestReadsOfEveryLayoutReadWhatWasWritten reads arrays whose shape is not a
// multiple of the chunk shape, some of whose chunks were never written,
// whole, chunk by chunk and in regions that are not aligned, unsharded and
// sharded, the shards read whole and by range.
func TestReadsOfEveryLayoutReadWhatWasWritten(t *testing.T) {
	type layout struct {
		shape, chunk, shard []int
	}
	layouts := []layout{
		{shape: []int{10, 7}, chunk: []int{3, 7}},                     // bands of whole rows
		{shape: []int{10, 7}, chunk: []int{4, 3}},                     // tiles
		{shape: []int{12, 8}, chunk: []int{4, 4}},                     // a multiple of the chunks
		{shape: []int{9, 6, 5}, chunk: []int{2, 6, 5}},                // whole planes
		{shape: []int{9, 6, 5}, chunk: []int{4, 3, 2}},                // blocks
		{shape: []int{13}, chunk: []int{4}},                           // one dimension
		{shape: []int{10, 7}, chunk: []int{2, 7}, shard: []int{6, 7}}, // shards of bands
		{shape: []int{10, 7}, chunk: []int{2, 3}, shard: []int{4, 6}},
		{shape: []int{9, 6, 5}, chunk: []int{2, 3, 2}, shard: []int{4, 6, 4}},
		{shape: []int{8, 6}, chunk: []int{2, 3}, shard: []int{2, 3}}, // a shard of one chunk
	}
	stores := []struct {
		name  string
		store func() Store
	}{
		{"by range", func() Store { return NewMemoryStore() }},
		{"whole", func() Store { return plainStore{NewMemoryStore()} }},
	}
	r := rand.New(rand.NewPCG(3, 4))
	for _, l := range layouts {
		for _, st := range stores {
			if l.shard == nil && st.name == "whole" {
				continue
			}
			for _, fill := range []int32{0, -1} {
				name := fmt.Sprintf("%v of %v in %v/%s/fill=%d", l.shape, l.chunk, l.shard, st.name, fill)
				t.Run(name, func(t *testing.T) {
					o := ArrayOptions{Shape: l.shape, ChunkShape: l.chunk, DataType: Int32, FillValue: fill}
					if l.shard != nil {
						o.ChunkShape = l.shard
						o.Codecs = []Codec{&ShardingCodec{ChunkShape: l.chunk}}
					}
					a := mustArray(t, st.store(), "a", o)
					// A few regions written, the rest of the array never.
					all := slices.Repeat([]int32{fill}, product(l.shape))
					for range 3 {
						start, n := randomBlock(r, l.shape)
						data := make([]int32, product(n))
						for i := range data {
							data[i] = r.Int32N(1000) + 1
						}
						if err := Write(ctx, a, start, n, data); err != nil {
							t.Fatal(err)
						}
						i := 0
						if product(n) > 0 {
							eachIndex(make([]int, len(n)), minus(n, slices.Repeat([]int{1}, len(n))), func(p []int) error {
								all[offsetOf(l.shape, plus(start, p))] = data[i]
								i++
								return nil
							})
						}
					}
					check := func(start, n []int) {
						t.Helper()
						got, err := Read[int32](ctx, a, start, n)
						if err != nil {
							t.Fatal(err)
						}
						if want := regionOf(all, l.shape, start, n); !slices.Equal(got, want) {
							t.Fatalf("region at %v of %v:\n got %v\nwant %v", start, n, got, want)
						}
					}
					check(make([]int, len(l.shape)), l.shape)
					// Each chunk, the ones at the edges cut short by the end.
					last := minus(a.NumChunks(), slices.Repeat([]int{1}, len(l.shape)))
					eachIndex(make([]int, len(l.shape)), last, func(idx []int) error {
						start := times(idx, l.chunk)
						n := make([]int, len(idx))
						for k := range n {
							n[k] = min(l.chunk[k], l.shape[k]-start[k])
						}
						check(start, n)
						return nil
					})
					for range 30 {
						check(randomBlock(r, l.shape))
					}
				})
			}
		}
	}
}

// TestAChunkReadOnItsOwnIsTheCallers checks that a region that is a single
// chunk, which is handed over as it was decoded, shares nothing with another
// read of it.
func TestAChunkReadOnItsOwnIsTheCallers(t *testing.T) {
	for _, c := range []struct {
		name  string
		store Store
		o     ArrayOptions
	}{
		{"unsharded", NewMemoryStore(), ArrayOptions{ChunkShape: []int{4, 4}}},
		{"sharded, by range", NewMemoryStore(), ArrayOptions{ChunkShape: []int{8, 8}, Codecs: []Codec{&ShardingCodec{ChunkShape: []int{4, 4}}}}},
		{"sharded, whole", plainStore{NewMemoryStore()}, ArrayOptions{ChunkShape: []int{8, 8}, Codecs: []Codec{&ShardingCodec{ChunkShape: []int{4, 4}}}}},
		{"a shard of one chunk", plainStore{NewMemoryStore()}, ArrayOptions{ChunkShape: []int{4, 4}, Codecs: []Codec{&ShardingCodec{ChunkShape: []int{4, 4}}}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.o.Shape, c.o.DataType, c.o.FillValue = []int{8, 8}, Uint8, 0
			a := mustArray(t, c.store, "a", c.o)
			all := make([]uint8, 64)
			for i := range all {
				all[i] = uint8(i + 1)
			}
			if err := Write(ctx, a, nil, nil, all); err != nil {
				t.Fatal(err)
			}
			start, n := []int{4, 0}, []int{4, 4}
			first := must(Read[uint8](ctx, a, start, n))
			want := regionOf(all, []int{8, 8}, start, n)
			if !slices.Equal(first, want) {
				t.Fatalf("got %v, want %v", first, want)
			}
			clear(first)
			if got := must(Read[uint8](ctx, a, start, n)); !slices.Equal(got, want) {
				t.Errorf("after the first read was cleared, got %v, want %v", got, want)
			}
			if got := must(Read[uint8](ctx, a, nil, nil)); !slices.Equal(got, all) {
				t.Errorf("after the first read was cleared, the whole array is %v", got)
			}
		})
	}
}
