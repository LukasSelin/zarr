package zarr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// busyStore counts how many calls of a store are in flight at once and the
// most there ever were, holding each up long enough that they meet.
type busyStore struct {
	*MemoryStore
	mu        sync.Mutex
	now, most int
	calls     int
}

func (s *busyStore) enter() {
	s.mu.Lock()
	s.now++
	s.calls++
	s.most = max(s.most, s.now)
	s.mu.Unlock()
	time.Sleep(time.Millisecond)
	s.mu.Lock()
	s.now--
	s.mu.Unlock()
}

func (s *busyStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.enter()
	return s.MemoryStore.Get(ctx, key)
}

func (s *busyStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.enter()
	return s.MemoryStore.GetRange(ctx, key, offset, length)
}

func (s *busyStore) Set(ctx context.Context, key string, value []byte) error {
	s.enter()
	return s.MemoryStore.Set(ctx, key, value)
}

func (s *busyStore) Delete(ctx context.Context, key string) error {
	s.enter()
	return s.MemoryStore.Delete(ctx, key)
}

// reset forgets what a busyStore counted, so that a read is counted on its
// own after the write that seeded it.
func (s *busyStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now, s.most, s.calls = 0, 0, 0
}

// failingGet is a store whose reads of a key holding the substring fail, and
// which counts the reads it was asked for.
type failingGet struct {
	Store
	substring string
	err       error
	mu        sync.Mutex
	reads     int
}

func (s *failingGet) read(key string) error {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	if strings.Contains(key, s.substring) {
		return s.err
	}
	return nil
}

func (s *failingGet) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.read(key); err != nil {
		return nil, err
	}
	return s.Store.Get(ctx, key)
}

func (s *failingGet) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := s.read(key); err != nil {
		return nil, err
	}
	return s.Store.(RangeGetter).GetRange(ctx, key, offset, length)
}

// The concurrency tests are on a 64 by 64 int32 array in chunks of 8, as a
// whole array and as shards of 2 by 2 chunks: enough stored objects that the
// goroutines have something to divide.
var concurrentShape = []int{64, 64}

func concurrentData() []int32 {
	data := make([]int32, product(concurrentShape))
	for i := range data {
		data[i] = int32(i*7 + 1)
	}
	return data
}

func concurrentOptions(sharded bool) ArrayOptions {
	o := ArrayOptions{Shape: concurrentShape, ChunkShape: []int{8, 8}, DataType: Int32}
	if sharded {
		o.ShardShape = []int{16, 16}
	}
	return o
}

// eachPath runs f over the three ways a read reaches a chunk: a chunk of its
// own, a shard read whole, and a shard read a range at a time. wrap hides a
// store's range reads where the shards are to be read whole.
func eachPath(t *testing.T, f func(t *testing.T, sharded bool, wrap func(Store) Store)) {
	t.Helper()
	whole := func(s Store) Store { return plainStore{s} }
	same := func(s Store) Store { return s }
	for _, c := range []struct {
		name    string
		sharded bool
		wrap    func(Store) Store
	}{
		{"chunks", false, same},
		{"shards read whole", true, whole},
		{"shards read by range", true, same},
	} {
		t.Run(c.name, func(t *testing.T) { f(t, c.sharded, c.wrap) })
	}
}

func TestAReadUsesNoMoreStoreCallsAtOnceThanTheArrayAllows(t *testing.T) {
	eachPath(t, func(t *testing.T, sharded bool, wrap func(Store) Store) {
		for _, c := range []int{1, 2, 8} {
			t.Run(fmt.Sprint(c), func(t *testing.T) {
				b := &busyStore{MemoryStore: NewMemoryStore()}
				a := mustArray(t, wrap(b), "height", concurrentOptions(sharded))
				a.Concurrency = c
				if err := Write(ctx, a, nil, nil, concurrentData()); err != nil {
					t.Fatal(err)
				}
				b.reset()
				got, err := Read[int32](ctx, a, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, concurrentData()) {
					t.Error("read back something else")
				}
				if b.most > c {
					t.Errorf("%d store calls at once, more than the %d allowed", b.most, c)
				}
				if c > 1 && b.most < 2 {
					t.Errorf("%d store calls at once of %d, none of them together", b.most, b.calls)
				}
			})
		}
	})
}

func TestAWriteAndAResizeUseNoMoreStoreCallsAtOnceThanTheArrayAllows(t *testing.T) {
	for _, c := range []int{1, 4} {
		t.Run(fmt.Sprint(c), func(t *testing.T) {
			b := &busyStore{MemoryStore: NewMemoryStore()}
			a := mustArray(t, b, "height", concurrentOptions(false))
			a.Concurrency, a.WriteEmptyChunks = c, true
			if err := Write(ctx, a, nil, nil, concurrentData()); err != nil {
				t.Fatal(err)
			}
			if b.most > c {
				t.Errorf("write made %d store calls at once, more than the %d allowed", b.most, c)
			}
			if c > 1 && b.most < 2 {
				t.Error("write made no store calls together")
			}
			b.reset()
			// Shrinking deletes the stored objects wholly past the new end
			// and refills the ones it cuts through.
			if err := a.Resize(ctx, []int{20, 20}); err != nil {
				t.Fatal(err)
			}
			if b.most > c {
				t.Errorf("resize made %d store calls at once, more than the %d allowed", b.most, c)
			}
			if c > 1 && b.most < 2 {
				t.Error("resize made no store calls together")
			}
		})
	}
}

func TestConcurrencyOfOneReadsAndWritesWhatManyDo(t *testing.T) {
	eachPath(t, func(t *testing.T, sharded bool, wrap func(Store) Store) {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("WriteEmptyChunks=%v", empty), func(t *testing.T) {
				// A corner of nothing but fill leaves a stored object that
				// is not kept, so writeStored's delete is taken concurrently
				// too.
				data := concurrentData()
				for y := range 16 {
					for x := range 16 {
						data[y*concurrentShape[1]+x] = 0
					}
				}
				store := func(c int) (*MemoryStore, []int32) {
					t.Helper()
					m := NewMemoryStore()
					a := mustArray(t, wrap(m), "height", concurrentOptions(sharded))
					a.Concurrency, a.WriteEmptyChunks = c, empty
					if err := Write(ctx, a, nil, nil, data); err != nil {
						t.Fatal(err)
					}
					got, err := Read[int32](ctx, a, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					return m, got
				}
				one, oneRead := store(1)
				many, manyRead := store(8)
				if !slices.Equal(oneRead, manyRead) {
					t.Error("one at a time read something else than many did")
				}
				if !slices.Equal(oneRead, data) {
					t.Error("read back something else than was written")
				}
				if !slices.Equal(one.Keys(), many.Keys()) {
					t.Errorf("keys %v, not %v", many.Keys(), one.Keys())
				}
				for _, key := range one.Keys() {
					if !bytes.Equal(must(one.Get(ctx, key)), must(many.Get(ctx, key))) {
						t.Errorf("%s holds something else", key)
					}
				}
			})
		}
	})
}

func TestAConcurrentReadMakesTheSameStoreCallsAsASerialOne(t *testing.T) {
	counts := func(c int) (int, int, int64) {
		t.Helper()
		s := &countingStore{MemoryStore: NewMemoryStore()}
		a := mustArray(t, s, "height", concurrentOptions(true))
		if err := Write(ctx, a, nil, nil, concurrentData()); err != nil {
			t.Fatal(err)
		}
		a.Concurrency = c
		s.gets, s.ranges, s.bytes = 0, 0, 0
		if _, err := Read[int32](ctx, a, []int{5, 5}, []int{40, 40}); err != nil {
			t.Fatal(err)
		}
		return s.gets, s.ranges, s.bytes
	}
	gets, ranges, size := counts(1)
	gets8, ranges8, size8 := counts(8)
	if gets != gets8 || ranges != ranges8 || size != size8 {
		t.Errorf("%d gets, %d ranges and %d bytes eight at a time; %d, %d and %d one at a time",
			gets8, ranges8, size8, gets, ranges, size)
	}
}

func TestTheFirstErrorOfAConcurrentReadIsWhatIsReturned(t *testing.T) {
	eachPath(t, func(t *testing.T, sharded bool, wrap func(Store) Store) {
		broken := errors.New("broken")
		m := NewMemoryStore()
		a := mustArray(t, wrap(m), "height", concurrentOptions(sharded))
		a.WriteEmptyChunks = true
		if err := Write(ctx, a, nil, nil, concurrentData()); err != nil {
			t.Fatal(err)
		}
		// The wrap goes outside, so that plainStore hides the range reads of
		// the store rather than of the failingGet over it.
		f := &failingGet{Store: m, substring: "c/1/1", err: broken}
		b := mustOpen(t, wrap(f), "height")
		_, err := Read[int32](ctx, b, nil, nil)
		if !errors.Is(err, broken) {
			t.Errorf("read failed with %v, not the error the store gave", err)
		}
		if errors.Is(err, context.Canceled) {
			t.Errorf("read failed with %v: the cancellation of the chunks beside it", err)
		}
	})
}

func TestAReadThatFailsStopsTheChunksBesideIt(t *testing.T) {
	broken := errors.New("broken")
	m := NewMemoryStore()
	a := mustArray(t, m, "height", concurrentOptions(false))
	a.WriteEmptyChunks = true
	if err := Write(ctx, a, nil, nil, concurrentData()); err != nil {
		t.Fatal(err)
	}
	chunks := product(a.NumChunks())
	// The first chunk in C order fails, so what the store is asked for after
	// it is what the cancellation did not stop.
	f := &failingGet{Store: m, substring: "c/0/0", err: broken}
	b := mustOpen(t, f, "height")
	b.Concurrency = 4
	if _, err := Read[int32](ctx, b, nil, nil); !errors.Is(err, broken) {
		t.Fatalf("read failed with %v", err)
	}
	if f.reads > chunks/2 {
		t.Errorf("read %d of %d chunks after the first of them failed", f.reads, chunks)
	}
}

// cancelling is a store that cancels the read it is part of as soon as a
// chunk is asked for, and counts the chunks that were.
type cancelling struct {
	Store
	cancel func()
	mu     sync.Mutex
	reads  int
}

func (s *cancelling) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, "/c/") {
		s.mu.Lock()
		s.reads++
		s.mu.Unlock()
		s.cancel()
	}
	return s.Store.Get(ctx, key)
}

func TestACancelledContextStopsAReadPartWay(t *testing.T) {
	m := NewMemoryStore()
	a := mustArray(t, m, "height", concurrentOptions(false))
	a.WriteEmptyChunks = true
	if err := Write(ctx, a, nil, nil, concurrentData()); err != nil {
		t.Fatal(err)
	}
	chunks := product(a.NumChunks())
	inner, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &cancelling{Store: plainStore{m}, cancel: cancel}
	b := mustOpen(t, c, "height")
	b.Concurrency = 4
	if _, err := Read[int32](inner, b, nil, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("read failed with %v, not a cancellation", err)
	}
	if c.reads > chunks/2 {
		t.Errorf("read %d of %d chunks after the ctx was cancelled", c.reads, chunks)
	}
}

func TestNothingRacesWhenAnArrayIsReadAndWrittenAtOnce(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		t.Run(fmt.Sprintf("sharded=%v", sharded), func(t *testing.T) {
			m := NewMemoryStore()
			a := mustArray(t, m, "height", concurrentOptions(sharded))
			data := concurrentData()
			if err := Write(ctx, a, nil, nil, data); err != nil {
				t.Fatal(err)
			}
			// Four readers over regions that overlap, and four writers each
			// of stored objects of its own: what doc.go promises of an Array,
			// made to happen.
			var wg sync.WaitGroup
			errs := make([]error, 8)
			for i := range 4 {
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, errs[i] = Read[int32](ctx, a, []int{i * 8, 0}, []int{24, 64})
				}()
				go func() {
					defer wg.Done()
					at, n := i*16, 16*concurrentShape[1]
					errs[4+i] = Write(ctx, a, []int{at, 0}, []int{16, 64}, data[at*concurrentShape[1]:][:n])
				}()
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			if got, err := Read[int32](ctx, a, nil, nil); err != nil || !slices.Equal(got, data) {
				t.Errorf("read back something else: %v", err)
			}
		})
	}
}

func TestADirectoryStoreTakesConcurrentWrites(t *testing.T) {
	s := NewDirStore(t.TempDir())
	a := mustArray(t, s, "height", concurrentOptions(false))
	data := concurrentData()
	if err := Write(ctx, a, nil, nil, data); err != nil {
		t.Fatal(err)
	}
	b := mustOpen(t, s, "height")
	if got, err := Read[int32](ctx, b, nil, nil); err != nil || !slices.Equal(got, data) {
		t.Errorf("read back something else: %v", err)
	}
}

func TestASpanCountsTheIndicesEachIndexWalks(t *testing.T) {
	for _, c := range []struct{ lo, hi []int }{
		{nil, nil},
		{[]int{0}, []int{4}},
		{[]int{2, 1}, []int{2, 3}},
		{[]int{0, 0, 0}, []int{1, 2, 3}},
	} {
		var want [][]int
		eachIndex(c.lo, c.hi, func(idx []int) error {
			want = append(want, slices.Clone(idx))
			return nil
		})
		if n := spanLen(c.lo, c.hi); n != len(want) {
			t.Errorf("%v to %v: %d indices, not %d", c.lo, c.hi, n, len(want))
		}
		idx := make([]int, len(c.lo))
		for n, w := range want {
			spanInto(idx, c.lo, c.hi, n)
			if !slices.Equal(idx, w) {
				t.Errorf("%v to %v: index %d is %v, not %v", c.lo, c.hi, n, idx, w)
			}
			if got := spanOf(c.lo, c.hi, w); got != n {
				t.Errorf("%v to %v: %v is at %d, not %d", c.lo, c.hi, w, got, n)
			}
		}
	}
}

// Tiles that do not line up with the chunks, written from a goroutine each:
// no two tiles overlap, but most chunks - and every shard - are shared by
// several of them, each of which reads the stored object, patches its part
// and writes it back. Every element must hold what its own tile wrote, with
// the tiles written through one handle on the array or through two.
func TestDisjointWritesFromManyGoroutinesSharingAChunkAllLand(t *testing.T) {
	shape, tile := []int{30, 42}, []int{4, 5}
	for _, sharded := range []bool{false, true} {
		for _, handles := range []int{1, 2} {
			t.Run(fmt.Sprintf("sharded=%v/handles=%d", sharded, handles), func(t *testing.T) {
				s := &busyStore{MemoryStore: NewMemoryStore()}
				o := ArrayOptions{Shape: shape, ChunkShape: []int{7, 9}, DataType: Int32, FillValue: int32(-1)}
				if sharded {
					o.ShardShape = []int{14, 18}
				}
				arrays := []*Array{mustArray(t, s, "a", o)}
				if handles == 2 {
					b, err := OpenArray(ctx, s, "a")
					if err != nil {
						t.Fatal(err)
					}
					arrays = append(arrays, b)
				}
				var wg sync.WaitGroup
				errs := make(chan error, product(shape))
				n := 0
				for y := 0; y < shape[0]; y += tile[0] {
					for x := 0; x < shape[1]; x += tile[1] {
						a := arrays[n%len(arrays)]
						n++
						wg.Add(1)
						go func() {
							defer wg.Done()
							region := []int{min(tile[0], shape[0]-y), min(tile[1], shape[1]-x)}
							data := make([]int32, product(region))
							for i := range data {
								data[i] = int32((y+i/region[1])*shape[1] + x + i%region[1])
							}
							errs <- Write(ctx, a, []int{y, x}, region, data)
						}()
					}
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatal(err)
					}
				}
				got, err := Read[int32](ctx, arrays[0], nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				lost := 0
				for i, v := range got {
					if v != int32(i) {
						lost++
					}
				}
				if lost > 0 {
					t.Errorf("%d of %d elements lost to a write beside them", lost, len(got))
				}
			})
		}
	}
}

// A Write waiting for a stored object another is patching gives up when its
// ctx does, and the lock is not left behind for the next one.
func TestAWriteWaitingForAStoredObjectStopsWithItsContext(t *testing.T) {
	s := NewMemoryStore()
	a := mustArray(t, s, "a", ArrayOptions{Shape: []int{4}, ChunkShape: []int{4}, DataType: Int32})
	unlock, err := storedLocks.lock(ctx, a.storedKey([]int{0}))
	if err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := Write(c, a, []int{1}, []int{2}, []int32{5, 6}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a write of a held chunk: %v", err)
	}
	unlock()
	if err := Write(ctx, a, []int{1}, []int{2}, []int32{5, 6}); err != nil {
		t.Fatal(err)
	}
	storedLocks.mu.Lock()
	left := len(storedLocks.held)
	storedLocks.mu.Unlock()
	if left != 0 {
		t.Errorf("%d locks left behind", left)
	}
}
