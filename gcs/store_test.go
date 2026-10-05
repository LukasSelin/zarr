package gcs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/storetest"
	"google.golang.org/api/option"
)

var ctx = context.Background()

// fake is a bucket in memory behind the HTTP API of GCS, as much of it as the
// client uses for this package: reads by the XML API, and attributes,
// listing, deletes and uploads in one request by the JSON API. It counts
// requests. The store is tested through the real client talking to it.
type fake struct {
	mu      sync.Mutex
	bucket  string
	objects map[string][]byte
	// forbid answers every request with 403, as GCS does a caller without
	// the permission.
	forbid bool
	// ignoreRange answers a ranged read with the whole object, as a server
	// that does not know ranges would.
	ignoreRange bool
	// pageSize, if more than 0, is how many names one listing answers with
	// before it gives a token for the next page.
	pageSize int
	requests []string
}

func newFake() *fake { return &fake{bucket: "b", objects: map[string][]byte{}} }

// store is a store of the objects of f under prefix.
func (f *fake) store(t *testing.T, prefix string) *Store {
	t.Helper()
	return New(f.client(t).Bucket("b"), prefix)
}

// client is a client of the GCS API at a server of f.
func (f *fake) client(t *testing.T) *storage.Client {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c, err := storage.NewClient(ctx,
		option.WithEndpoint(srv.URL+"/storage/v1/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	// A test of an error wants it at once, not after the client's backoff.
	c.SetRetry(storage.WithPolicy(storage.RetryNever))
	return c
}

func (f *fake) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if strings.HasPrefix(r, prefix) {
			n++
		}
	}
	return n
}

func apiError(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":{"code":%d,"message":%q}}`, code, http.StatusText(code))
}

func (f *fake) attrs(name string) map[string]any {
	return map[string]any{"kind": "storage#object", "bucket": f.bucket, "name": name, "size": strconv.Itoa(len(f.objects[name]))}
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forbid {
		apiError(w, http.StatusForbidden)
		return
	}
	if r.URL.Path == "/upload/storage/v1/b/"+f.bucket+"/o" && r.Method == http.MethodPost {
		f.upload(w, r)
		return
	}
	if rest, ok := strings.CutPrefix(r.URL.Path, "/storage/v1/b/"); ok {
		bucket, rest, _ := strings.Cut(rest, "/")
		if bucket != f.bucket {
			apiError(w, http.StatusNotFound)
			return
		}
		if rest == "o" && r.Method == http.MethodGet {
			f.list(w, r)
			return
		}
		name, ok := strings.CutPrefix(rest, "o/")
		_, there := f.objects[name]
		switch {
		case !ok:
			apiError(w, http.StatusBadRequest)
		case !there:
			apiError(w, http.StatusNotFound)
		case r.Method == http.MethodDelete:
			f.requests = append(f.requests, "DELETE "+name)
			delete(f.objects, name)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Query().Get("alt") == "media":
			f.read(w, r, name)
		case r.Method == http.MethodGet:
			f.requests = append(f.requests, "ATTRS "+name)
			json.NewEncoder(w).Encode(f.attrs(name))
		default:
			apiError(w, http.StatusBadRequest)
		}
		return
	}
	// A read by the XML API: /bucket/name.
	bucket, name, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if _, there := f.objects[name]; bucket != f.bucket || !there || r.Method != http.MethodGet {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	f.read(w, r, name)
}

func (f *fake) read(w http.ResponseWriter, r *http.Request, name string) {
	f.requests = append(f.requests, "GET "+name+" "+r.Header.Get("Range"))
	if f.ignoreRange {
		r.Header.Del("Range")
	}
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(f.objects[name]))
}

// upload takes an upload in one request: multipart/related, the object's
// attributes as JSON and then its bytes.
func (f *fake) upload(w http.ResponseWriter, r *http.Request) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || r.URL.Query().Get("uploadType") != "multipart" {
		apiError(w, http.StatusBadRequest)
		return
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	var meta struct{ Name string }
	part, err := mr.NextPart()
	if err == nil {
		err = json.NewDecoder(part).Decode(&meta)
	}
	if err == nil {
		part, err = mr.NextPart()
	}
	var b []byte
	if err == nil {
		b, err = io.ReadAll(part)
	}
	if err != nil || meta.Name == "" {
		apiError(w, http.StatusBadRequest)
		return
	}
	f.requests = append(f.requests, "PUT "+meta.Name)
	f.objects[meta.Name] = b
	json.NewEncoder(w).Encode(f.attrs(meta.Name))
}

// list answers as GCS does: the names under the prefix in order, with the
// ones that have the delimiter after the prefix rolled up into prefixes, a
// page at a time, the token being the last name of the page.
func (f *fake) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix, delim, after := q.Get("prefix"), q.Get("delimiter"), q.Get("pageToken")
	f.requests = append(f.requests, "LIST "+prefix+" "+delim)
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	type name struct {
		s      string
		common bool
	}
	var names []name
	seen := map[string]bool{}
	for _, k := range keys {
		s, common := k, false
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				s, common = k[:len(prefix)+i+len(delim)], true
			}
		}
		if !seen[s] {
			seen[s] = true
			names = append(names, name{s, common})
		}
	}
	out := map[string]any{"kind": "storage#objects"}
	items, prefixes := []any{}, []string{}
	n := 0
	for _, nm := range names {
		if after != "" && nm.s <= after {
			continue
		}
		if f.pageSize > 0 && n == f.pageSize {
			out["nextPageToken"] = after
			break
		}
		if nm.common {
			prefixes = append(prefixes, nm.s)
		} else {
			items = append(items, f.attrs(nm.s))
		}
		after = nm.s
		n++
	}
	out["items"], out["prefixes"] = items, prefixes
	json.NewEncoder(w).Encode(out)
}

func TestKeysAreObjectsUnderThePrefix(t *testing.T) {
	f := newFake()
	s := f.store(t, "/data/fwi.zarr/")
	if err := s.Set(ctx, "isi/c/0/1", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.objects["data/fwi.zarr/isi/c/0/1"]; !ok {
		t.Errorf("objects %v", f.objects)
	}
	if got, err := s.Get(ctx, "isi/c/0/1"); err != nil || string(got) != "abc" {
		t.Errorf("get %q, %v", got, err)
	}
	if err := s.Delete(ctx, "isi/c/0/1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "isi/c/0/1"); err != nil {
		t.Errorf("deleting what is not there: %v", err)
	}
	if _, err := s.Get(ctx, "isi/c/0/1"); !errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("get of a deleted key: %v", err)
	}
	whole := f.store(t, "")
	if err := whole.Set(ctx, "zarr.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.objects["zarr.json"]; !ok {
		t.Errorf("objects %v", f.objects)
	}
	if err := whole.Set(ctx, "empty", nil); err != nil {
		t.Fatal(err)
	}
	if got, err := whole.Get(ctx, "empty"); err != nil || len(got) != 0 {
		t.Errorf("an empty value: %q, %v", got, err)
	}
}

func TestRanges(t *testing.T) {
	f := newFake()
	f.objects["k"] = []byte("0123456789")
	s := f.store(t, "")
	for _, c := range []struct {
		offset, length int64
		want           string
	}{
		{0, 10, "0123456789"},
		{3, 4, "3456"},
		{9, 1, "9"},
		{-4, 4, "6789"},
		{-4, 2, "67"},
		{-10, 10, "0123456789"},
		{-1, 0, ""},
		{5, 0, ""},
		{10, 0, ""},
	} {
		got, err := s.GetRange(ctx, "k", c.offset, c.length)
		if err != nil || string(got) != c.want {
			t.Errorf("range %d+%d: %q, %v; want %q", c.offset, c.length, got, err, c.want)
		}
	}
	for _, c := range [][2]int64{{8, 3}, {10, 1}, {11, 0}, {-11, 11}, {-2, 3}, {0, -1}} {
		if got, err := s.GetRange(ctx, "k", c[0], c[1]); err == nil {
			t.Errorf("range %d+%d is outside and read %q", c[0], c[1], got)
		}
	}
	for _, c := range [][2]int64{{-4, 4}, {0, 4}, {0, 0}} {
		if _, err := s.GetRange(ctx, "missing", c[0], c[1]); !errors.Is(err, zarr.ErrNotFound) {
			t.Errorf("range %d+%d of a missing key: %v", c[0], c[1], err)
		}
	}

	f.ignoreRange = true
	for _, c := range [][2]int64{{0, 4}, {3, 4}, {-4, 4}} {
		if got, err := s.GetRange(ctx, "k", c[0], c[1]); err == nil {
			t.Errorf("range %d+%d from a server that ignores ranges read %q", c[0], c[1], got)
		}
	}
}

func TestErrorsThatAreNotNotFound(t *testing.T) {
	f := newFake()
	f.forbid = true
	s := f.store(t, "")
	if _, err := s.Get(ctx, "c/0"); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("403 read as %v", err)
	}
	if err := s.Delete(ctx, "c/0"); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("403 of a delete read as %v", err)
	}
	f.forbid = false
	missing := New(f.client(t).Bucket("other"), "")
	err := missing.List(ctx, "", func(string) error { return nil })
	if !errors.Is(err, storage.ErrBucketNotExist) || errors.Is(err, zarr.ErrNotFound) || !strings.Contains(err.Error(), `"other"`) {
		t.Errorf("listing a missing bucket: %v", err)
	}
}

func TestMaxObjectBytes(t *testing.T) {
	f := newFake()
	f.objects["k"] = make([]byte, 100)
	s := f.store(t, "")
	s.MaxObjectBytes = 100
	if _, err := s.Get(ctx, "k"); err != nil {
		t.Error(err)
	}
	s.MaxObjectBytes = 99
	if _, err := s.Get(ctx, "k"); err == nil {
		t.Error("read an object past MaxObjectBytes")
	}
}

func TestACancelledReadReturns(t *testing.T) {
	f := newFake()
	f.objects["k"] = []byte("v")
	s := f.store(t, "")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Get(cancelled, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("get: %v", err)
	}
	if _, err := s.GetRange(cancelled, "k", -1, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("range: %v", err)
	}
	if err := s.Set(cancelled, "k", []byte("w")); err == nil {
		t.Error("set with a cancelled context")
	}
	if string(f.objects["k"]) != "v" {
		t.Errorf("a cancelled set wrote %q", f.objects["k"])
	}
}

func TestAShardedArrayIsReadInRanges(t *testing.T) {
	f := newFake()
	s := f.store(t, "fwi.zarr")
	a, err := zarr.CreateArray(ctx, s, "", zarr.ArrayOptions{
		Shape:      []int{0, 8, 8},
		ChunkShape: []int{2, 4, 4},
		ShardShape: []int{4, 8, 8},
		DataType:   zarr.Float32,
		Codecs:     []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.GzipCodec{Level: 5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var all []float32
	for day := range 6 {
		grid := make([]float32, 64)
		for i := range grid {
			grid[i] = float32(day*100 + i)
		}
		at, err := zarr.Append(ctx, a, 0, grid)
		if err != nil || at != day {
			t.Fatalf("append day %d: at %d, %v", day, at, err)
		}
		all = append(all, grid...)
	}

	b, err := zarr.OpenArray(ctx, s, "")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(b.Shape(), []int{6, 8, 8}) {
		t.Fatalf("shape %v", b.Shape())
	}
	if got, err := zarr.Read[float32](ctx, b, nil, nil); err != nil || !slices.Equal(got, all) {
		t.Fatalf("read %v, %v", got, err)
	}

	before := f.count("GET")
	got, err := zarr.Read[float32](ctx, b, []int{5, 1, 6}, []int{1, 1, 1})
	if err != nil || len(got) != 1 || got[0] != 500+1*8+6 {
		t.Fatalf("one element: %v, %v", got, err)
	}
	gets := f.requests[len(f.requests)-(f.count("GET")-before):]
	if len(gets) != 2 {
		t.Errorf("one element read in %d requests, not an index and a chunk: %v", len(gets), gets)
	}
	for _, r := range gets {
		if strings.HasSuffix(r, " ") {
			t.Errorf("a whole object was read: %q", r)
		}
	}
}

// listed is every key of the store under prefix, sorted.
func listed(t *testing.T, s *Store, prefix string) []string {
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

func TestListingObjectsUnderThePrefix(t *testing.T) {
	f := newFake()
	s := f.store(t, "fwi.zarr")
	for _, k := range []string{"zarr.json", "isi/zarr.json", "isi/c/0/0", "isi/c/0/1"} {
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	// What is outside the store, and the folder placeholder the console
	// makes, which no Set could have written.
	f.objects["other.zarr/zarr.json"] = []byte("{}")
	f.objects["fwi.zarr/isi/"] = nil

	want := []string{"isi/c/0/0", "isi/c/0/1", "isi/zarr.json", "zarr.json"}
	if got := listed(t, s, ""); !slices.Equal(got, want) {
		t.Errorf("list = %v, want %v", got, want)
	}
	if got := listed(t, s, "isi/c/"); !slices.Equal(got, []string{"isi/c/0/0", "isi/c/0/1"}) {
		t.Errorf("list of a prefix = %v", got)
	}
	if got := listed(t, s, "nothing/"); len(got) != 0 {
		t.Errorf("list of a prefix with nothing under it = %v", got)
	}
	f.forbid = true
	if err := s.List(ctx, "", func(string) error { return nil }); err == nil {
		t.Error("listing without permission was allowed")
	}
}

func TestListingPagesThroughTheBucket(t *testing.T) {
	f := newFake()
	f.pageSize = 2
	s := f.store(t, "fwi.zarr")
	var want []string
	for i := range 5 {
		k := fmt.Sprintf("isi/c/0/%d", i)
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
		want = append(want, k)
	}
	before := f.count("LIST")
	if got := listed(t, s, ""); !slices.Equal(got, want) {
		t.Errorf("list = %v, want %v", got, want)
	}
	if n := f.count("LIST") - before; n != 3 {
		t.Errorf("%d requests for 5 keys at 2 a page, want 3", n)
	}
}

func TestListingOneLevelRollsUpWithTheDelimiter(t *testing.T) {
	f := newFake()
	s := f.store(t, "fwi.zarr")
	for _, k := range []string{"zarr.json", "isi/zarr.json", "isi/c/0/0", "bui/zarr.json"} {
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		prefix string
		want   []string
	}{
		{"", []string{"bui/", "isi/", "zarr.json"}},
		{"isi/", []string{"c/", "zarr.json"}},
	} {
		var got []string
		before := f.count("LIST")
		if err := zarr.ListDir(ctx, s, c.prefix, func(name string) error {
			got = append(got, name)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		if !slices.Equal(got, c.want) {
			t.Errorf("one level of %q = %v, want %v", c.prefix, got, c.want)
		}
		// One level is one request, not one for every key under it.
		if n := f.count("LIST") - before; n != 1 {
			t.Errorf("one level of %q took %d requests", c.prefix, n)
		}
	}
}

func TestDeletingAnArrayInABucket(t *testing.T) {
	f := newFake()
	s := f.store(t, "fwi.zarr")
	if _, err := zarr.CreateGroup(ctx, s, "", nil); err != nil {
		t.Fatal(err)
	}
	a, err := zarr.CreateArray(ctx, s, "isi", zarr.ArrayOptions{
		Shape: []int{4}, ChunkShape: []int{2}, DataType: zarr.Int32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := zarr.Write(ctx, a, []int{0}, []int{4}, []int32{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requests = nil
	f.mu.Unlock()
	if err := a.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	// The metadata goes first, so a delete that fails part way leaves keys
	// nothing opens rather than an array reading as its fill value.
	var deletes []string
	for _, r := range f.requests {
		if after, ok := strings.CutPrefix(r, "DELETE "); ok {
			deletes = append(deletes, after)
		}
	}
	if len(deletes) == 0 || deletes[0] != "fwi.zarr/isi/zarr.json" {
		t.Errorf("deletes %v, want the metadata first", deletes)
	}
	if got := listed(t, s, "isi/"); len(got) != 0 {
		t.Errorf("after deleting the array: %v", got)
	}
	if got := listed(t, s, ""); !slices.Equal(got, []string{"zarr.json"}) {
		t.Errorf("the group around it: %v", got)
	}
}

func TestAStoreInGCSKeepsTheStoreContract(t *testing.T) {
	storetest.Run(t, func() zarr.Store { return newFake().store(t, "fwi.zarr") })
	t.Run("whole bucket", func(t *testing.T) {
		storetest.Run(t, func() zarr.Store { return newFake().store(t, "") })
	})
}
