package zarr

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

// storedChunks is the encoded chunks of the shard at key, nil where it holds
// none, and nil altogether if there is no shard.
func storedChunks(t *testing.T, s Store, a *Array, key string) [][]byte {
	t.Helper()
	b, err := s.Get(ctx, key)
	if err != nil {
		return nil
	}
	chunks, err := a.shard.split(a.perShard, b)
	if err != nil {
		t.Fatal(err)
	}
	return chunks
}

// reencoded is the shard at sidx as the array would encode it whole from what
// it reads as, or nil if it would not be stored.
func reencoded(t *testing.T, a *Array, sidx []int) []byte {
	t.Helper()
	buf, err := readStored[int32](ctx, a, sidx)
	if err != nil {
		t.Fatal(err)
	}
	buf = fillPastEnd(a, sidx, buf, true)
	if !a.WriteEmptyChunks && allFill(buf, a.fill.(int32)) {
		return nil
	}
	b, err := a.codecs.encode(buf, a.storedSpec())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAPartialShardWriteKeepsTheBytesOfTheChunksItDoesNotTouch(t *testing.T) {
	for _, loc := range []IndexLocation{IndexEnd, IndexStart} {
		t.Run(string(loc), func(t *testing.T) {
			s := NewMemoryStore()
			a := mustArray(t, s, "", ArrayOptions{Shape: []int{8, 8}, DataType: Int32, ChunkShape: []int{8, 8},
				Codecs: []Codec{&ShardingCodec{ChunkShape: []int{4, 4}, IndexLocation: loc,
					Codecs: []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 1}}}}})
			all := make([]int32, 64)
			for i := range all {
				all[i] = int32(i)
			}
			if err := Write(ctx, a, nil, nil, all); err != nil {
				t.Fatal(err)
			}
			// Chunk 1 is stored as gzip 9 would store it, which a chunk
			// decoded and encoded again through gzip 1 would not be.
			chunks := storedChunks(t, s, a, "c/0/0")
			raw := must(GzipCodec{}.DecodeBytes(chunks[1]))
			other := must(GzipCodec{Level: 9}.EncodeBytes(raw))
			if bytes.Equal(other, chunks[1]) {
				t.Fatal("gzip 9 stores the chunk as gzip 1 does")
			}
			chunks[1] = other
			if err := s.Set(ctx, "c/0/0", must(a.shard.assemble(a.perShard, chunks))); err != nil {
				t.Fatal(err)
			}

			// Two elements of chunk 2, and all of chunk 3 by WriteChunk.
			if err := Write(ctx, a, []int{5, 1}, []int{1, 2}, []int32{-1, -2}); err != nil {
				t.Fatal(err)
			}
			if err := WriteChunk(ctx, a, []int{1, 1}, slices.Repeat([]int32{-3}, 16)); err != nil {
				t.Fatal(err)
			}
			after := storedChunks(t, s, a, "c/0/0")
			for k, want := range [][]byte{chunks[0], other} {
				if !bytes.Equal(after[k], want) {
					t.Errorf("chunk %d untouched was stored again", k)
				}
			}
			all[5*8+1], all[5*8+2] = -1, -2
			for r := 4; r < 8; r++ {
				for c := 4; c < 8; c++ {
					all[r*8+c] = -3
				}
			}
			if got, err := Read[int32](ctx, a, nil, nil); err != nil || !slices.Equal(got, all) {
				t.Errorf("read %v: %v", got, err)
			}
		})
	}
}

func TestChunksOfFillComeAndGoFromAShardWrittenInPart(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{8}, ChunkShape: []int{2}, ShardShape: []int{8}, DataType: Int32, FillValue: 7})
	index := func() []uint64 {
		b, err := s.Get(ctx, "c/0")
		if err != nil {
			return nil
		}
		ix, err := a.shard.decodeIndex(a.perShard, b[len(b)-a.indexSize:])
		if err != nil {
			t.Fatal(err)
		}
		return ix
	}
	steps := []struct {
		start int
		data  []int32
		want  []uint64
	}{
		{2, []int32{1}, []uint64{noChunk, noChunk, 0, 8, noChunk, noChunk, noChunk, noChunk}},
		{5, []int32{1, 2}, []uint64{noChunk, noChunk, 0, 8, 8, 8, 16, 8}},
		{2, []int32{7, 7, 7, 7}, []uint64{noChunk, noChunk, noChunk, noChunk, noChunk, noChunk, 0, 8}},
		{6, []int32{7}, nil},
	}
	for i, st := range steps {
		if err := Write(ctx, a, []int{st.start}, []int{len(st.data)}, st.data); err != nil {
			t.Fatal(err)
		}
		if got := index(); !slices.Equal(got, st.want) {
			t.Errorf("after write %d, index %v, not %v", i, got, st.want)
		}
	}

	a.WriteEmptyChunks = true
	if err := WriteChunk(ctx, a, []int{1}, []int32{7, 7}); err != nil {
		t.Fatal(err)
	}
	if got, want := index(), []uint64{noChunk, noChunk, 0, 8, noChunk, noChunk, noChunk, noChunk}; !slices.Equal(got, want) {
		t.Errorf("a chunk of fill written with WriteEmptyChunks: index %v, not %v", got, want)
	}
}

func TestAShardAtTheEdgeKeepsNothingPastTheEnd(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{7, 5}, ChunkShape: []int{2, 2}, ShardShape: []int{4, 4}, DataType: Int32, FillValue: -1})
	if err := WriteChunk(ctx, a, []int{3, 2}, []int32{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := Write(ctx, a, []int{5, 3}, []int{2, 2}, []int32{5, 6, 8, 9}); err != nil {
		t.Fatal(err)
	}
	if got := reencoded(t, a, []int{1, 1}); !bytes.Equal(must(s.Get(ctx, "c/1/1")), got) {
		t.Error("the shard is not what writing it whole stores")
	}
	if err := a.Resize(ctx, []int{8, 8}); err != nil {
		t.Fatal(err)
	}
	got, err := Read[int32](ctx, a, []int{4, 4}, []int{4, 4})
	if err != nil {
		t.Fatal(err)
	}
	want := []int32{
		-1, -1, -1, -1,
		6, -1, -1, -1,
		9, -1, -1, -1,
		-1, -1, -1, -1,
	}
	if !slices.Equal(got, want) {
		t.Errorf("read %v, not %v", got, want)
	}
}

func TestAShardWrittenInPartIsWhatWritingItWholeStores(t *testing.T) {
	shape := []int{9, 10}
	for _, loc := range []IndexLocation{IndexEnd, IndexStart} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/WriteEmptyChunks=%v", loc, empty), func(t *testing.T) {
				s := NewMemoryStore()
				a := mustArray(t, s, "", ArrayOptions{Shape: shape, DataType: Int32, ChunkShape: []int{4, 6},
					Codecs: []Codec{&ShardingCodec{ChunkShape: []int{2, 3}, IndexLocation: loc,
						Codecs: []Codec{BytesCodec{Endian: Big}, GzipCodec{Level: 1}}}}})
				a.WriteEmptyChunks = empty
				r := rand.New(rand.NewPCG(1, uint64(len(loc))))
				for range 60 {
					start := []int{r.IntN(shape[0]), r.IntN(shape[1])}
					n := []int{1 + r.IntN(shape[0]-start[0]), 1 + r.IntN(shape[1]-start[1])}
					data := make([]int32, product(n))
					for i := range data {
						// Mostly fill, so that chunks come and go.
						if r.IntN(3) == 0 {
							data[i] = int32(r.IntN(5))
						}
					}
					if r.IntN(4) == 0 {
						idx := []int{r.IntN(5), r.IntN(4)}
						data = make([]int32, 6)
						data[r.IntN(6)] = int32(r.IntN(2))
						if err := WriteChunk(ctx, a, idx, data); err != nil {
							t.Fatal(err)
						}
					} else if err := Write(ctx, a, start, n, data); err != nil {
						t.Fatal(err)
					}
				}
				for _, key := range s.Keys() {
					if key == "zarr.json" {
						continue
					}
					var sidx [2]int
					if _, err := fmt.Sscanf(key, "c/%d/%d", &sidx[0], &sidx[1]); err != nil {
						t.Fatal(err)
					}
					want := reencoded(t, a, sidx[:])
					if empty {
						// Chunks never written are left out of a shard
						// written in part, which writing it whole keeps.
						continue
					}
					if !bytes.Equal(must(s.Get(ctx, key)), want) {
						t.Errorf("shard %s is not what writing it whole stores", key)
					}
				}
			})
		}
	}
}

func TestWritesToChunksOfOneShardFromManyGoroutinesAllLand(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{16, 16}, ChunkShape: []int{2, 2}, ShardShape: []int{16, 16}, DataType: Int32})
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := []int{i / 8, i % 8}
			if i%2 == 0 {
				errs <- WriteChunk(ctx, a, idx, slices.Repeat([]int32{int32(i + 1)}, 4))
				return
			}
			// One element of the chunk; the rest was never written.
			errs <- Write(ctx, a, times(idx, []int{2, 2}), []int{1, 1}, []int32{int32(i + 1)})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := Read[int32](ctx, a, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for r := range 16 {
		for c := range 16 {
			i := r/2*8 + c/2
			want := int32(i + 1)
			if i%2 == 1 && (r%2 != 0 || c%2 != 0) {
				want = 0
			}
			if got[r*16+c] != want {
				t.Fatalf("element %d, %d is %d, not %d", r, c, got[r*16+c], want)
			}
		}
	}
}

func TestAPartialWriteIntoABrokenShardIsAnError(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{4}, ChunkShape: []int{2}, ShardShape: []int{4}, DataType: Int32})
	if err := Write(ctx, a, nil, nil, []int32{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	shard := must(s.Get(ctx, "c/0"))
	shard[len(shard)-8] ^= 1
	if err := s.Set(ctx, "c/0", shard); err != nil {
		t.Fatal(err)
	}
	if err := WriteChunk(ctx, a, []int{0}, []int32{5, 6}); err == nil {
		t.Error("wrote a chunk into a shard whose index is broken")
	}
	if got := must(s.Get(ctx, "c/0")); !bytes.Equal(got, shard) {
		t.Error("the broken shard was written over")
	}
}
