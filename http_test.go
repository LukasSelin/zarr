package zarr_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/storetest"
)

// host serves the keys of a store under /data/fwi.zarr/ as a static host
// does, ranges and all, and counts what it is asked for.
type host struct {
	store zarr.Store
	// ignoreRange answers a ranged GET with the whole value, as a server
	// that does not know ranges would.
	ignoreRange bool
	// missing is the status of a key the store does not hold: 404 if 0.
	missing int

	mu       sync.Mutex
	requests []string
	queries  []string
}

func (h *host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.requests = append(h.requests, r.Method+" "+r.URL.Path+" "+r.Header.Get("Range"))
	h.queries = append(h.queries, r.URL.RawQuery)
	h.mu.Unlock()
	key, ok := strings.CutPrefix(r.URL.Path, "/data/fwi.zarr/")
	v, err := h.store.Get(r.Context(), key)
	if !ok || err != nil {
		w.WriteHeader(statusOr(h.missing, http.StatusNotFound))
		return
	}
	if h.ignoreRange {
		r.Header.Del("Range")
	}
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(v))
}

func statusOr(v, or int) int {
	if v == 0 {
		return or
	}
	return v
}

func (h *host) ranged() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.requests {
		if !strings.HasSuffix(r, " ") {
			n++
		}
	}
	return n
}

// serve starts h and returns a store of what it serves.
func serve(t *testing.T, h http.Handler) *zarr.HTTPStore {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	s, err := zarr.NewHTTPStore(srv.URL+"/data/fwi.zarr/", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAStoreOverHTTPKeepsTheReadOnlyContract(t *testing.T) {
	for _, ignoreRange := range []bool{false, true} {
		t.Run(fmt.Sprintf("ignoring ranges %v", ignoreRange), func(t *testing.T) {
			storetest.RunReadOnly(t, func(seed map[string][]byte) zarr.Store {
				m := zarr.NewMemoryStore()
				for k, v := range seed {
					m.Set(context.Background(), k, v)
				}
				return serve(t, &host{store: m, ignoreRange: ignoreRange})
			})
		})
	}
}

// published writes a group with a plain and a sharded array, some of whose
// chunks are never written, as a hierarchy put on a web server would be.
func published(t *testing.T) (*zarr.MemoryStore, []float32) {
	t.Helper()
	ctx := context.Background()
	m := zarr.NewMemoryStore()
	root, err := zarr.CreateGroup(ctx, m, "", map[string]any{"title": "fwi"})
	if err != nil {
		t.Fatal(err)
	}
	all := make([]float32, 8*8)
	for i := range all {
		// The first rows are fill, so their chunks are never written.
		if i >= 2*8 {
			all[i] = float32(i)
		}
	}
	for name, shard := range map[string][]int{"plain": nil, "sharded": {4, 8}} {
		a, err := root.CreateArray(ctx, name, zarr.ArrayOptions{
			Shape:      []int{8, 8},
			ChunkShape: []int{2, 4},
			ShardShape: shard,
			DataType:   zarr.Float32,
			Codecs:     []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := zarr.Write(ctx, a, nil, nil, all); err != nil {
			t.Fatal(err)
		}
	}
	return m, all
}

func TestReadingAnArrayOverHTTP(t *testing.T) {
	ctx := context.Background()
	m, all := published(t)
	for _, ignoreRange := range []bool{false, true} {
		h := &host{store: m, ignoreRange: ignoreRange}
		s := serve(t, h)
		g, err := zarr.OpenGroup(ctx, s, "")
		if err != nil {
			t.Fatal(err)
		}
		var title string
		if ok, err := g.Attribute("title", &title); !ok || err != nil || title != "fwi" {
			t.Errorf("title %q, %v, %v", title, ok, err)
		}
		for _, name := range []string{"plain", "sharded"} {
			a, err := zarr.OpenArray(ctx, s, name)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := zarr.Read[float32](ctx, a, nil, nil); err != nil || !slices.Equal(got, all) {
				t.Errorf("%s, ignoring ranges %v: %v, %v", name, ignoreRange, got, err)
			}
			if got, err := zarr.Read[float32](ctx, a, []int{0, 0}, []int{2, 2}); err != nil || !slices.Equal(got, []float32{0, 0, 0, 0}) {
				t.Errorf("%s, a chunk never written: %v, %v", name, got, err)
			}
		}
	}
}

func TestAShardedArrayIsReadOverHTTPInRanges(t *testing.T) {
	ctx := context.Background()
	m, _ := published(t)
	h := &host{store: m}
	s := serve(t, h)
	a, err := zarr.OpenArray(ctx, s, "sharded")
	if err != nil {
		t.Fatal(err)
	}
	before := h.ranged()
	got, err := zarr.Read[float32](ctx, a, []int{5, 6}, []int{1, 1})
	if err != nil || !slices.Equal(got, []float32{5*8 + 6}) {
		t.Fatalf("one element: %v, %v", got, err)
	}
	// The shard's index and the one chunk, not the whole shard.
	if n := h.ranged() - before; n != 2 {
		t.Errorf("one element read in %d ranges: %v", n, h.requests)
	}
}

func TestKeysAreEscapedAndTheQueryIsKept(t *testing.T) {
	ctx := context.Background()
	m := zarr.NewMemoryStore()
	key := "a b/%41?#x/zarr.json"
	m.Set(ctx, key, []byte("{}"))
	h := &host{store: m}
	srv := httptest.NewServer(h)
	defer srv.Close()
	s, err := zarr.NewHTTPStore(srv.URL+"/data/fwi.zarr?sig=secret#frag", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, key); err != nil || string(got) != "{}" {
		t.Errorf("get %q: %q, %v", key, got, err)
	}
	if h.queries[0] != "sig=secret" {
		t.Errorf("query %q", h.queries[0])
	}
	// The error of a request never answered does not carry the query.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Get(cancelled, key); err == nil || strings.Contains(err.Error(), "secret") {
		t.Errorf("error %v", err)
	}
	if _, err := s.Get(ctx, "../outside"); err == nil {
		t.Error("a key outside the store was asked for")
	}
}

func TestNewHTTPStoreWantsAnHTTPURL(t *testing.T) {
	for _, u := range []string{"", "fwi.zarr", "ftp://example.com/fwi.zarr", "https://", "http://[::1"} {
		if _, err := zarr.NewHTTPStore(u, nil); err == nil {
			t.Errorf("%q was taken", u)
		}
	}
}

func TestWritingOverHTTPIsReadOnly(t *testing.T) {
	ctx := context.Background()
	m, _ := published(t)
	s := serve(t, &host{store: m})
	a, err := zarr.OpenArray(ctx, s, "plain")
	if err != nil {
		t.Fatal(err)
	}
	if err := zarr.Write(ctx, a, []int{4, 0}, []int{1, 1}, []float32{1}); !errors.Is(err, zarr.ErrReadOnly) {
		t.Errorf("write: %v", err)
	}
	// A chunk of nothing but fill is deleted rather than written.
	if err := zarr.Write(ctx, a, nil, nil, make([]float32, 64)); !errors.Is(err, zarr.ErrReadOnly) {
		t.Errorf("write of fill: %v", err)
	}
	if _, err := zarr.CreateArray(ctx, s, "new", zarr.ArrayOptions{Shape: []int{1}, ChunkShape: []int{1}, DataType: zarr.Int8}); !errors.Is(err, zarr.ErrReadOnly) {
		t.Errorf("create: %v", err)
	}
	if err := zarr.Delete(ctx, s, "plain"); !errors.Is(err, zarr.ErrReadOnly) {
		t.Errorf("delete: %v", err)
	}
	g, err := zarr.OpenGroup(ctx, s, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Children(ctx); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("children: %v", err)
	}
}

func TestForbiddenIsAnErrorUnlessSaidOtherwise(t *testing.T) {
	ctx := context.Background()
	m, all := published(t)
	s := serve(t, &host{store: m, missing: http.StatusForbidden})
	a, err := zarr.OpenArray(ctx, s, "plain")
	if err != nil {
		t.Fatal(err)
	}
	_, err = zarr.Read[float32](ctx, a, nil, nil)
	if err == nil || errors.Is(err, zarr.ErrNotFound) || !strings.Contains(err.Error(), "ForbiddenIsNotFound") {
		t.Errorf("403 read as %v", err)
	}
	s.ForbiddenIsNotFound = true
	if got, err := zarr.Read[float32](ctx, a, nil, nil); err != nil || !slices.Equal(got, all) {
		t.Errorf("with ForbiddenIsNotFound: %v, %v", got, err)
	}
}

func TestOtherStatusesAreErrors(t *testing.T) {
	ctx := context.Background()
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	if _, err := s.Get(ctx, "zarr.json"); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("get: %v", err)
	}
	if _, err := s.GetRange(ctx, "zarr.json", -4, 4); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("range: %v", err)
	}
}

// liar answers every ranged GET with a 206 of the Content-Range and body it
// is given, whatever was asked for.
func liar(contentRange, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentRange != "" {
			w.Header().Set("Content-Range", contentRange)
		}
		w.WriteHeader(http.StatusPartialContent)
		io.WriteString(w, body)
	})
}

func TestARangeMustBeWhatWasAskedFor(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name, contentRange, body string
	}{
		{"short", "bytes 2-5/11", "zar"},
		{"long", "bytes 2-5/11", "zarr.j"},
		{"another range", "bytes 3-6/11", "arr."},
		{"no Content-Range", "", "zarr"},
		{"a Content-Range of nonsense", "bytes 5-2/11", "zarr"},
		{"more than one range", "", "--sep\r\n"},
	} {
		s := serve(t, liar(c.contentRange, c.body))
		if got, err := s.GetRange(ctx, "a/zarr.json", 2, 4); err == nil {
			t.Errorf("%s: read %q", c.name, got)
		}
	}
	// A suffix must end at the end of the value.
	s := serve(t, liar("bytes 2-5/11", "zarr"))
	if got, err := s.GetRange(ctx, "a/zarr.json", -4, 4); err == nil {
		t.Errorf("a suffix that is not at the end: read %q", got)
	}
	s = serve(t, liar("bytes 7-10/*", "json"))
	if got, err := s.GetRange(ctx, "a/zarr.json", -4, 4); err == nil {
		t.Errorf("a suffix of a value of no length: read %q", got)
	}
}

func TestMaxObjectBytesOverHTTP(t *testing.T) {
	ctx := context.Background()
	m := zarr.NewMemoryStore()
	m.Set(ctx, "k", make([]byte, 100))
	for _, ignoreRange := range []bool{false, true} {
		s := serve(t, &host{store: m, ignoreRange: ignoreRange})
		s.MaxObjectBytes = 100
		if _, err := s.Get(ctx, "k"); err != nil {
			t.Error(err)
		}
		if _, err := s.GetRange(ctx, "k", -10, 10); err != nil {
			t.Error(err)
		}
		s.MaxObjectBytes = 99
		if _, err := s.Get(ctx, "k"); err == nil {
			t.Error("read a value past MaxObjectBytes")
		}
		if _, err := s.GetRange(ctx, "k", 0, 100); err == nil {
			t.Error("read a range past MaxObjectBytes")
		}
		if _, err := s.GetRange(ctx, "k", -10, 10); ignoreRange && err == nil {
			t.Error("read the whole of a value past MaxObjectBytes for its suffix")
		}
	}
	// A body with no length said is held to it as well.
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for range 10 {
			w.Write(make([]byte, 50))
			w.(http.Flusher).Flush()
		}
	}))
	s.MaxObjectBytes = 100
	if _, err := s.Get(ctx, "k"); err == nil {
		t.Error("read a streamed value past MaxObjectBytes")
	}
}

func TestACancelledReadOverHTTPReturns(t *testing.T) {
	stuck := make(chan struct{})
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stuck:
		}
	}))
	defer close(stuck)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Get(ctx, "zarr.json"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("get: %v", err)
	}
	if _, err := s.GetRange(ctx, "zarr.json", -4, 4); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("range: %v", err)
	}
}

// roundTrip is an http.RoundTripper of a function.
type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// FuzzHTTPRange has a server answer a range as it likes. An honest one says
// what it sends and sends the value's bytes, and a range read from it is
// either an error or the bytes asked for; a liar sends what it likes, and a
// range read from it is an error or as long as asked for. Neither may panic.
func FuzzHTTPRange(f *testing.F) {
	const v = "0123456789"
	f.Add(uint8(2), "", []byte{}, int64(2), int64(5), int64(2), int64(4))
	f.Add(uint8(0), "", []byte{}, int64(0), int64(9), int64(-4), int64(4))
	f.Add(uint8(3), "bytes 2-5/10", []byte("2345"), int64(0), int64(0), int64(2), int64(4))
	f.Add(uint8(1), "bytes 6-9/*", []byte("6789"), int64(0), int64(0), int64(-4), int64(4))
	f.Add(uint8(2), "", []byte{}, int64(0), int64(9), int64(10), int64(0))
	f.Fuzz(func(t *testing.T, mode uint8, contentRange string, body []byte, first, last, offset, length int64) {
		honest := mode&1 == 0
		status := []int{http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable, http.StatusNotFound}[mode/2%4]
		client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
			resp := &http.Response{StatusCode: status, Header: http.Header{}, Request: r, ContentLength: -1}
			b := body
			switch {
			case !honest:
				if contentRange != "" {
					resp.Header.Set("Content-Range", contentRange)
				}
			case r.Method == http.MethodHead || status == http.StatusOK:
				b, resp.ContentLength = []byte(v), int64(len(v))
			case status == http.StatusPartialContent && 0 <= first && first <= last && last < int64(len(v)):
				b = []byte(v[first : last+1])
				resp.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(v)))
			default:
				resp.StatusCode, b = http.StatusRequestedRangeNotSatisfiable, nil
			}
			if r.Method == http.MethodHead {
				b = nil
			}
			resp.Status = http.StatusText(resp.StatusCode)
			resp.Body = io.NopCloser(bytes.NewReader(b))
			return resp, nil
		})}
		s, err := zarr.NewHTTPStore("http://example.com/fwi.zarr", client)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.GetRange(context.Background(), "k", offset, length)
		if err != nil {
			return
		}
		if int64(len(got)) != length {
			t.Fatalf("range %d+%d read %d bytes", offset, length, len(got))
		}
		if !honest {
			return
		}
		at := offset
		if at < 0 {
			at += int64(len(v))
		}
		if at < 0 || length < 0 || at+length > int64(len(v)) {
			t.Fatalf("range %d+%d is outside and read %q", offset, length, got)
		}
		if string(got) != v[at:at+length] {
			t.Fatalf("range %d+%d read %q, not %q", offset, length, got, v[at:at+length])
		}
	})
}
