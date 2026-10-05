package azblob

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/storetest"
)

var ctx = context.Background()

// fake is a container in memory behind the REST API of Blob Storage, as much
// of it as the client uses for this package: Get Blob, Get Blob Properties,
// Put Blob, Delete Blob and List Blobs. It counts requests. The store is
// tested through the real client talking to it.
type fake struct {
	mu        sync.Mutex
	container string
	blobs     map[string][]byte
	// forbid answers every request with 403, as Blob Storage does a caller
	// without the permission.
	forbid bool
	// ignoreRange answers a ranged read with the whole blob, as a server
	// that does not know ranges would.
	ignoreRange bool
	// change, if not nil, is called between a read of a blob's properties
	// and the next request, as another writer would.
	change func()
	// pageSize, if more than 0, is how many names one listing answers with
	// before it gives a marker for the next page.
	pageSize int
	requests []string
}

func newFake() *fake { return &fake{container: "b", blobs: map[string][]byte{}} }

// store is a store of the blobs of f under prefix.
func (f *fake) store(t *testing.T, prefix string) *Store {
	t.Helper()
	return New(f.client(t, f.container), prefix)
}

// client is a client of the container name at a server of f.
func (f *fake) client(t *testing.T, name string) *container.Client {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c, err := container.NewClientWithNoCredential(srv.URL+"/account/"+name, &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: srv.Client(),
			// A test of an error wants it at once, not after backoff.
			Retry: policy.RetryOptions{MaxRetries: -1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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

func apiError(w http.ResponseWriter, r *http.Request, status int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
	}
}

func etag(b []byte) string {
	h := fnv.New64a()
	h.Write(b)
	return fmt.Sprintf(`"0x%X"`, h.Sum64())
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forbid {
		apiError(w, r, http.StatusForbidden, "AuthorizationPermissionMismatch")
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/account/")
	if !ok {
		apiError(w, r, http.StatusBadRequest, "InvalidUri")
		return
	}
	name, blob, isBlob := strings.Cut(rest, "/")
	if name != f.container {
		apiError(w, r, http.StatusNotFound, "ContainerNotFound")
		return
	}
	if !isBlob {
		if q := r.URL.Query(); r.Method == http.MethodGet && q.Get("restype") == "container" && q.Get("comp") == "list" {
			f.list(w, r)
			return
		}
		apiError(w, r, http.StatusBadRequest, "UnsupportedQueryParameter")
		return
	}
	if r.Method == http.MethodPut {
		if r.Header.Get("x-ms-blob-type") != "BlockBlob" {
			apiError(w, r, http.StatusBadRequest, "InvalidHeaderValue")
			return
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			apiError(w, r, http.StatusBadRequest, "InvalidInput")
			return
		}
		f.requests = append(f.requests, "PUT "+blob)
		f.blobs[blob] = b
		w.Header().Set("ETag", etag(b))
		w.WriteHeader(http.StatusCreated)
		return
	}
	if f.change != nil && r.Method != http.MethodHead {
		f.change()
		f.change = nil
	}
	v, there := f.blobs[blob]
	if !there {
		apiError(w, r, http.StatusNotFound, "BlobNotFound")
		return
	}
	switch r.Method {
	case http.MethodDelete:
		f.requests = append(f.requests, "DELETE "+blob)
		delete(f.blobs, blob)
		w.WriteHeader(http.StatusAccepted)
	case http.MethodHead:
		f.requests = append(f.requests, "HEAD "+blob)
		w.Header().Set("ETag", etag(v))
		w.Header().Set("Content-Length", strconv.Itoa(len(v)))
		w.Header().Set("x-ms-blob-type", "BlockBlob")
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		rng := r.Header.Get("x-ms-range")
		if rng == "" {
			rng = r.Header.Get("Range")
		}
		f.requests = append(f.requests, "GET "+blob+" "+rng)
		if m := r.Header.Get("If-Match"); m != "" && m != etag(v) {
			apiError(w, r, http.StatusPreconditionFailed, "ConditionNotMet")
			return
		}
		r.Header.Del("x-ms-range")
		r.Header.Del("Range")
		r.Header.Del("If-Match")
		if rng != "" && !f.ignoreRange {
			r.Header.Set("Range", rng)
		}
		w.Header().Set("ETag", etag(v))
		w.Header().Set("x-ms-blob-type", "BlockBlob")
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(v))
	default:
		apiError(w, r, http.StatusBadRequest, "UnsupportedHttpVerb")
	}
}

// listResult is the answer to List Blobs.
type listResult struct {
	XMLName       xml.Name `xml:"EnumerationResults"`
	ContainerName string   `xml:"ContainerName,attr"`
	Prefix        string   `xml:"Prefix"`
	Delimiter     string   `xml:"Delimiter,omitempty"`
	Blobs         struct {
		Blob       []listedBlob `xml:"Blob"`
		BlobPrefix []listedBlob `xml:"BlobPrefix"`
	} `xml:"Blobs"`
	NextMarker string `xml:"NextMarker"`
}

type listedBlob struct {
	Name string `xml:"Name"`
}

// list answers as Blob Storage does: the names under the prefix in order,
// with the ones that have the delimiter after the prefix rolled up into
// prefixes, a page at a time, the marker being the last name of the page.
func (f *fake) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix, delim, after := q.Get("prefix"), q.Get("delimiter"), q.Get("marker")
	f.requests = append(f.requests, "LIST "+prefix+" "+delim)
	var keys []string
	for k := range f.blobs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	out := listResult{ContainerName: f.container, Prefix: prefix, Delimiter: delim}
	seen := map[string]bool{}
	n := 0
	for _, k := range keys {
		s, common := k, false
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				s, common = k[:len(prefix)+i+len(delim)], true
			}
		}
		if seen[s] || after != "" && s <= after {
			continue
		}
		seen[s] = true
		if f.pageSize > 0 && n == f.pageSize {
			out.NextMarker = after
			break
		}
		if common {
			out.Blobs.BlobPrefix = append(out.Blobs.BlobPrefix, listedBlob{s})
		} else {
			out.Blobs.Blob = append(out.Blobs.Blob, listedBlob{s})
		}
		after = s
		n++
	}
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, xml.Header)
	xml.NewEncoder(w).Encode(out)
}

func TestKeysAreBlobsUnderThePrefix(t *testing.T) {
	f := newFake()
	s := f.store(t, "/data/fwi.zarr/")
	if err := s.Set(ctx, "isi/c/0/1", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.blobs["data/fwi.zarr/isi/c/0/1"]; !ok {
		t.Errorf("blobs %v", f.blobs)
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
	if _, ok := f.blobs["zarr.json"]; !ok {
		t.Errorf("blobs %v", f.blobs)
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
	f.blobs["k"] = []byte("0123456789")
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

func TestASuffixIsOfTheBlobItsLengthWasOf(t *testing.T) {
	f := newFake()
	f.blobs["k"] = []byte("0123456789")
	s := f.store(t, "")
	f.change = func() { f.blobs["k"] = []byte("abcdefghijklmnop") }
	if got, err := s.GetRange(ctx, "k", -4, 4); err == nil {
		t.Errorf("a blob replaced between its length and its range read %q", got)
	}
}

func TestErrorsThatAreNotNotFound(t *testing.T) {
	f := newFake()
	f.forbid = true
	s := f.store(t, "")
	if _, err := s.Get(ctx, "c/0"); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("403 read as %v", err)
	}
	if _, err := s.GetRange(ctx, "c/0", -4, 4); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("403 of a properties read as %v", err)
	}
	if err := s.Delete(ctx, "c/0"); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("403 of a delete read as %v", err)
	}
	f.forbid = false
	missing := New(f.client(t, "other"), "")
	if _, err := missing.Get(ctx, "c/0"); err == nil || errors.Is(err, zarr.ErrNotFound) || !strings.Contains(err.Error(), `"other"`) {
		t.Errorf("a read in a missing container: %v", err)
	}
	if _, err := missing.GetRange(ctx, "c/0", -4, 4); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("a range in a missing container: %v", err)
	}
	if err := missing.List(ctx, "", func(string) error { return nil }); err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("listing a missing container: %v", err)
	}
}

func TestMaxObjectBytes(t *testing.T) {
	f := newFake()
	f.blobs["k"] = make([]byte, 100)
	s := f.store(t, "")
	s.MaxObjectBytes = 100
	if _, err := s.Get(ctx, "k"); err != nil {
		t.Error(err)
	}
	s.MaxObjectBytes = 99
	if _, err := s.Get(ctx, "k"); err == nil {
		t.Error("read a blob past MaxObjectBytes")
	}
}

func TestACancelledReadReturns(t *testing.T) {
	f := newFake()
	f.blobs["k"] = []byte("v")
	s := f.store(t, "")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Get(cancelled, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("get: %v", err)
	}
	if _, err := s.GetRange(cancelled, "k", -1, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("range: %v", err)
	}
	if err := s.Set(cancelled, "k", []byte("w")); !errors.Is(err, context.Canceled) {
		t.Errorf("set: %v", err)
	}
	if string(f.blobs["k"]) != "v" {
		t.Errorf("a cancelled set wrote %q", f.blobs["k"])
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

	f.mu.Lock()
	f.requests = nil
	f.mu.Unlock()
	got, err := zarr.Read[float32](ctx, b, []int{5, 1, 6}, []int{1, 1, 1})
	if err != nil || len(got) != 1 || got[0] != 500+1*8+6 {
		t.Fatalf("one element: %v, %v", got, err)
	}
	// The index at the end of the shard is its length and then the range;
	// the chunk is a range.
	if heads, gets := f.count("HEAD"), f.count("GET"); heads != 1 || gets != 2 {
		t.Errorf("one element read in %d HEAD and %d GET: %v", heads, gets, f.requests)
	}
	for _, r := range f.requests {
		if strings.HasPrefix(r, "GET") && strings.HasSuffix(r, " ") {
			t.Errorf("a whole blob was read: %q", r)
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

func TestListingBlobsUnderThePrefix(t *testing.T) {
	f := newFake()
	s := f.store(t, "fwi.zarr")
	for _, k := range []string{"zarr.json", "isi/zarr.json", "isi/c/0/0", "isi/c/0/1"} {
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	// What is outside the store, and a directory marker, which no Set could
	// have written.
	f.blobs["other.zarr/zarr.json"] = []byte("{}")
	f.blobs["fwi.zarr/isi/"] = nil

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

func TestListingPagesThroughTheContainer(t *testing.T) {
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

func TestDeletingAnArrayInAContainer(t *testing.T) {
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

func TestAStoreInBlobStorageKeepsTheStoreContract(t *testing.T) {
	storetest.Run(t, func() zarr.Store { return newFake().store(t, "fwi.zarr") })
	t.Run("whole container", func(t *testing.T) {
		storetest.Run(t, func() zarr.Store { return newFake().store(t, "") })
	})
}
