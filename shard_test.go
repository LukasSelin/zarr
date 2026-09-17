package zarr

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"
	"sync"
	"testing"
)

// plainStore hides a store's range reads and its one-level listing, so that
// shards are read whole and a listing of one level is derived from List.
type plainStore struct{ Store }

// countingStore counts what is read from a store.
type countingStore struct {
	*MemoryStore
	mu           sync.Mutex
	gets, ranges int
	bytes        int64
}

func (s *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := s.MemoryStore.Get(ctx, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	s.bytes += int64(len(b))
	return b, err
}

func (s *countingStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b, err := s.MemoryStore.GetRange(ctx, key, offset, length)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ranges++
	s.bytes += int64(len(b))
	return b, err
}

func TestShardedRegionsReadBackWhatWasWritten(t *testing.T) {
	shape := []int{11, 13, 5}
	all := make([]int32, product(shape))
	for i := range all {
		all[i] = int32(i * 3)
	}
	regions := [][2][]int{
		{{0, 0, 0}, {1, 1, 1}},
		{{2, 3, 1}, {4, 5, 2}},
		{{10, 12, 4}, {1, 1, 1}},
		{{1, 0, 0}, {10, 13, 5}},
		{{3, 4, 0}, {8, 9, 5}},
	}
	for _, c := range []struct {
		name  string
		store func() Store
		shard Codec
		after []Codec
	}{
		{"index at the end", func() Store { return NewMemoryStore() }, &ShardingCodec{ChunkShape: []int{2, 3, 2}}, nil},
		{"index at the start, gzip within", func() Store { return NewMemoryStore() },
			&ShardingCodec{ChunkShape: []int{2, 3, 2}, Codecs: []Codec{BytesCodec{Endian: Big}, GzipCodec{Level: 1}}, IndexLocation: IndexStart}, nil},
		{"read whole", func() Store { return plainStore{NewMemoryStore()} }, &ShardingCodec{ChunkShape: []int{2, 3, 2}}, nil},
		{"gzip after the shard", func() Store { return NewMemoryStore() }, &ShardingCodec{ChunkShape: []int{2, 3, 2}}, []Codec{GzipCodec{Level: 5}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := c.store()
			a := mustArray(t, s, "a", ArrayOptions{Shape: shape, ChunkShape: []int{4, 6, 4}, DataType: Int32, FillValue: -7,
				Codecs: append([]Codec{c.shard}, c.after...)})
			if got := a.ChunkShape(); !slices.Equal(got, []int{2, 3, 2}) {
				t.Errorf("chunk shape %v", got)
			}
			if got := a.ShardShape(); !slices.Equal(got, []int{4, 6, 4}) {
				t.Errorf("shard shape %v", got)
			}
			if err := Write(ctx, a, nil, nil, all); err != nil {
				t.Fatal(err)
			}
			b, err := OpenArray(ctx, s, "a")
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range regions {
				got, err := Read[int32](ctx, b, r[0], r[1])
				if err != nil {
					t.Fatal(err)
				}
				if want := naive(all, shape, r[0], r[1]); !slices.Equal(got, want) {
					t.Errorf("region at %v of %v: got %v, want %v", r[0], r[1], got, want)
				}
			}

			// A patch over part of a shard keeps the rest of it.
			start, n := []int{1, 2, 1}, []int{5, 7, 3}
			if err := Write(ctx, b, start, n, make([]int32, product(n))); err != nil {
				t.Fatal(err)
			}
			want := slices.Clone(all)
			eachIndex([]int{0, 0, 0}, minusOne(n), func(p []int) error {
				want[((start[0]+p[0])*shape[1]+start[1]+p[1])*shape[2]+start[2]+p[2]] = 0
				return nil
			})
			if got, err := Read[int32](ctx, b, nil, nil); err != nil || !slices.Equal(got, want) {
				t.Errorf("patched array read back wrong: %v", err)
			}
		})
	}
}

// A shard as zarr-python 3.4.0 wrote it: int16 0 to 3 in chunks of two in a
// shard of four, the index first, uncompressed.
const pythonShard = "2400000000000000040000000000000028000000000000000400000000000000df3706b70000010002000300"

func TestAShardIsLaidOutAsZarrPythonLaysIt(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "b", ArrayOptions{Shape: []int{10}, ChunkShape: []int{4}, DataType: Int16,
		Codecs: []Codec{&ShardingCodec{ChunkShape: []int{2}, IndexLocation: IndexStart}}})
	data := make([]int16, 10)
	for i := range data {
		data[i] = int16(i)
	}
	if err := Write(ctx, a, nil, nil, data); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, "b/c/0"); hex.EncodeToString(got) != pythonShard {
		t.Errorf("shard is\n %x\nnot\n %s", got, pythonShard)
	}

	s.Set(ctx, "p/c/0", must(hex.DecodeString(pythonShard)))
	s.Set(ctx, "p/zarr.json", must(s.Get(ctx, "b/zarr.json")))
	p, err := OpenArray(ctx, s, "p")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Read[int16](ctx, p, nil, []int{4}); err != nil || !slices.Equal(got, []int16{0, 1, 2, 3}) {
		t.Errorf("read %v: %v", got, err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestReadingAChunkReadsOnlyItsPartOfTheShard(t *testing.T) {
	s := &countingStore{MemoryStore: NewMemoryStore()}
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{256, 256}, ChunkShape: []int{16, 16}, ShardShape: []int{128, 128}, DataType: Float64})
	data := make([]float64, 256*256)
	for i := range data {
		data[i] = float64(i) + 0.5
	}
	if err := Write(ctx, a, nil, nil, data); err != nil {
		t.Fatal(err)
	}
	shard, _ := s.MemoryStore.Get(ctx, "c/1/1")
	s.gets, s.ranges, s.bytes = 0, 0, 0

	got, err := Read[float64](ctx, a, []int{200, 150}, []int{3, 3})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != data[200*256+150] || got[8] != data[202*256+152] {
		t.Errorf("read %v", got)
	}
	if s.gets != 0 || s.ranges != 2 {
		t.Errorf("%d reads and %d range reads, not 0 and 2", s.gets, s.ranges)
	}
	if want := int64(16*16*8 + a.indexSize); s.bytes != want {
		t.Errorf("read %d bytes of a shard of %d, not %d", s.bytes, len(shard), want)
	}

	one, err := ReadChunk[float64](ctx, a, []int{12, 9})
	if err != nil || one[0] != data[12*16*256+9*16] || len(one) != 256 {
		t.Errorf("chunk read wrong: %v", err)
	}
}

func TestChunksOfNothingButFillAreLeftOutOfTheShard(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{8}, ChunkShape: []int{2}, ShardShape: []int{4}, DataType: Uint8})
	if err := Write(ctx, a, nil, nil, []uint8{0, 0, 3, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if keys := s.Keys(); !slices.Equal(keys, []string{"c/0", "zarr.json"}) {
		t.Errorf("keys %v", keys)
	}
	shard, _ := s.Get(ctx, "c/0")
	index, err := a.shard.decodeIndex(a.perShard, shard[len(shard)-a.indexSize:])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(index, []uint64{noChunk, noChunk, 0, 2}) {
		t.Errorf("index %v", index)
	}

	if err := WriteChunk(ctx, a, []int{3}, []uint8{9, 9}); err != nil {
		t.Fatal(err)
	}
	if got, err := Read[uint8](ctx, a, nil, nil); err != nil || !slices.Equal(got, []uint8{0, 0, 3, 0, 0, 0, 9, 9}) {
		t.Errorf("read %v: %v", got, err)
	}
	if err := Write(ctx, a, []int{0}, []int{4}, make([]uint8, 4)); err != nil {
		t.Fatal(err)
	}
	if keys := s.Keys(); !slices.Equal(keys, []string{"c/1", "zarr.json"}) {
		t.Errorf("a shard of nothing but fill was kept: %v", keys)
	}
}

func TestABrokenShardIsAnError(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "", ArrayOptions{Shape: []int{4}, ChunkShape: []int{2}, ShardShape: []int{4}, DataType: Int32})
	if err := Write(ctx, a, nil, nil, []int32{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	shard, _ := s.Get(ctx, "c/0")
	shard[len(shard)-8] ^= 1
	s.Set(ctx, "c/0", shard)
	if _, err := Read[int32](ctx, a, nil, nil); err == nil || !strings.Contains(err.Error(), "crc32c") {
		t.Errorf("a corrupted index read: %v", err)
	}
	if _, err := Read[int32](ctx, mustOpen(t, plainStore{s}, ""), nil, nil); err == nil {
		t.Error("a corrupted index read whole")
	}

	_, err := CreateArray(ctx, NewMemoryStore(), "", ArrayOptions{Shape: []int{4}, ChunkShape: []int{3}, ShardShape: []int{4}, DataType: Int32})
	if err == nil {
		t.Error("made shards that are not a whole number of chunks")
	}
	_, err = CreateArray(ctx, NewMemoryStore(), "", ArrayOptions{Shape: []int{4}, ChunkShape: []int{3}, DataType: Int32,
		Codecs: []Codec{&ShardingCodec{ChunkShape: []int{3}, IndexCodecs: []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 9}}}}})
	if err == nil {
		t.Error("made a shard index of no fixed length")
	}
}

func mustOpen(t *testing.T, s Store, path string) *Array {
	t.Helper()
	a, err := OpenArray(ctx, s, path)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
