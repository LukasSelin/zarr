package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/storetest"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var ctx = context.Background()

// fake is a bucket in memory that answers as S3 does, and counts requests.
type fake struct {
	mu      sync.Mutex
	bucket  string
	objects map[string][]byte
	// noList answers a key that is not there as S3 does to a role without
	// s3:ListBucket: 403.
	noList bool
	// ignoreRange answers a ranged GetObject with the whole object, as a
	// server that does not know ranges would.
	ignoreRange bool
	// pageSize, if more than 0, is how many names one ListObjectsV2 answers
	// with before it truncates.
	pageSize int
	requests []string
}

func newFake() *fake { return &fake{bucket: "b", objects: map[string][]byte{}} }

func responseError(code int, err error) error {
	return &smithy.OperationError{ServiceID: "S3", OperationName: "Test", Err: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}},
		Err:      err,
	}}
}

func (f *fake) lookup(bucket, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if bucket != f.bucket {
		return nil, responseError(404, &smithy.GenericAPIError{Code: "NoSuchBucket"})
	}
	b, ok := f.objects[key]
	switch {
	case ok:
		return b, nil
	case f.noList:
		return nil, responseError(403, &smithy.GenericAPIError{Code: "AccessDenied"})
	}
	return nil, responseError(404, &types.NoSuchKey{})
}

func (f *fake) note(r string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r)
}

func (f *fake) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	f.note("GET " + aws.ToString(in.Key) + " " + aws.ToString(in.Range))
	b, err := f.lookup(aws.ToString(in.Bucket), aws.ToString(in.Key))
	if err != nil {
		return nil, err
	}
	if in.Range != nil && !f.ignoreRange {
		size := int64(len(b))
		spec := strings.TrimPrefix(*in.Range, "bytes=")
		var lo, hi int64
		if strings.HasPrefix(spec, "-") {
			n, _ := strconv.ParseInt(spec[1:], 10, 64)
			lo, hi = max(size-n, 0), size-1
		} else {
			parts := strings.SplitN(spec, "-", 2)
			lo, _ = strconv.ParseInt(parts[0], 10, 64)
			hi, _ = strconv.ParseInt(parts[1], 10, 64)
			hi = min(hi, size-1)
		}
		if lo >= size {
			return nil, responseError(416, &smithy.GenericAPIError{Code: "InvalidRange"})
		}
		b = b[lo : hi+1]
	}
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b)), ContentLength: aws.Int64(int64(len(b)))}, nil
}

func (f *fake) HeadObject(_ context.Context, in *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	f.note("HEAD " + aws.ToString(in.Key))
	b, err := f.lookup(aws.ToString(in.Bucket), aws.ToString(in.Key))
	if err != nil {
		return nil, err
	}
	return &awss3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(b)))}, nil
}

func (f *fake) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	f.note("PUT " + aws.ToString(in.Key))
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[aws.ToString(in.Key)] = b
	return &awss3.PutObjectOutput{}, nil
}

func (f *fake) DeleteObject(_ context.Context, in *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	f.note("DELETE " + aws.ToString(in.Key))
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, aws.ToString(in.Key))
	return &awss3.DeleteObjectOutput{}, nil
}

// ListObjectsV2 answers as S3 does: the keys under the prefix in order, with
// the ones that have the delimiter after the prefix rolled up into common
// prefixes, a page at a time, the token being the last name of the page.
func (f *fake) ListObjectsV2(_ context.Context, in *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	f.note("LIST " + aws.ToString(in.Prefix) + " " + aws.ToString(in.Delimiter))
	f.mu.Lock()
	defer f.mu.Unlock()
	if aws.ToString(in.Bucket) != f.bucket {
		return nil, responseError(404, &smithy.GenericAPIError{Code: "NoSuchBucket"})
	}
	if f.noList {
		return nil, responseError(403, &smithy.GenericAPIError{Code: "AccessDenied"})
	}
	prefix, delim := aws.ToString(in.Prefix), aws.ToString(in.Delimiter)
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	// A name is a key, or the common prefix the delimiter rolls it up into.
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
		if seen[s] {
			continue
		}
		seen[s] = true
		names = append(names, name{s, common})
	}
	after := aws.ToString(in.ContinuationToken)
	out := &awss3.ListObjectsV2Output{}
	n := 0
	for _, nm := range names {
		if after != "" && nm.s <= after {
			continue
		}
		if f.pageSize > 0 && n == f.pageSize {
			out.IsTruncated = aws.Bool(true)
			break
		}
		if nm.common {
			out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(nm.s)})
		} else {
			out.Contents = append(out.Contents, types.Object{Key: aws.String(nm.s), Size: aws.Int64(int64(len(f.objects[nm.s])))})
		}
		out.NextContinuationToken = aws.String(nm.s)
		n++
	}
	if !aws.ToBool(out.IsTruncated) {
		out.NextContinuationToken = nil
	}
	return out, nil
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

func TestKeysAreObjectsUnderThePrefix(t *testing.T) {
	f := newFake()
	s := New(f, "b", "/data/fwi.zarr/")
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
	whole := New(f, "b", "")
	if err := whole.Set(ctx, "zarr.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.objects["zarr.json"]; !ok {
		t.Errorf("objects %v", f.objects)
	}
}

func TestRanges(t *testing.T) {
	f := newFake()
	f.objects["k"] = []byte("0123456789")
	s := New(f, "b", "")
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
	if _, err := s.GetRange(ctx, "missing", -4, 4); !errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("range of a missing key: %v", err)
	}
	if _, err := s.GetRange(ctx, "missing", 0, 0); !errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("empty range of a missing key: %v", err)
	}

	f.ignoreRange = true
	for _, c := range [][2]int64{{3, 4}, {-4, 4}} {
		if got, err := s.GetRange(ctx, "k", c[0], c[1]); err == nil {
			t.Errorf("range %d+%d from a server that ignores ranges read %q", c[0], c[1], got)
		}
	}
}

func TestErrorsThatAreNotNotFound(t *testing.T) {
	f := newFake()
	f.noList = true
	_, err := New(f, "b", "").Get(ctx, "c/0")
	if err == nil || errors.Is(err, zarr.ErrNotFound) || !strings.Contains(err.Error(), "s3:ListBucket") {
		t.Errorf("403 read as %v", err)
	}
	_, err = New(f, "other", "").Get(ctx, "c/0")
	if err == nil || errors.Is(err, zarr.ErrNotFound) {
		t.Errorf("a missing bucket read as %v", err)
	}
	if err := New(f, "b", "").Delete(ctx, "c/0"); err != nil {
		t.Errorf("delete: %v", err)
	}
}

func TestMaxObjectBytes(t *testing.T) {
	f := newFake()
	f.objects["k"] = make([]byte, 100)
	s := New(f, "b", "")
	s.MaxObjectBytes = 100
	if _, err := s.Get(ctx, "k"); err != nil {
		t.Error(err)
	}
	s.MaxObjectBytes = 99
	if _, err := s.Get(ctx, "k"); err == nil {
		t.Error("read an object past MaxObjectBytes")
	}
}

func TestAShardedArrayIsReadInRanges(t *testing.T) {
	f := newFake()
	s := New(f, "b", "fwi.zarr")
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
	s := New(f, "b", "fwi.zarr")
	for _, k := range []string{"zarr.json", "isi/zarr.json", "isi/c/0/0", "isi/c/0/1"} {
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	// What is outside the store, and the directory marker a console makes,
	// which no Set could have written.
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
	f.noList = true
	if err := s.List(ctx, "", func(string) error { return nil }); err == nil {
		t.Error("listing without s3:ListBucket was allowed")
	}
}

func TestListingPagesThroughTheBucket(t *testing.T) {
	f := newFake()
	f.pageSize = 2
	s := New(f, "b", "fwi.zarr")
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
	s := New(f, "b", "fwi.zarr")
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
	s := New(f, "b", "fwi.zarr")
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
	f.requests = nil
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

func TestAStoreInS3KeepsTheStoreContract(t *testing.T) {
	storetest.Run(t, func() zarr.Store { return New(newFake(), "b", "fwi.zarr") })
	t.Run("whole bucket", func(t *testing.T) {
		storetest.Run(t, func() zarr.Store { return New(newFake(), "b", "") })
	})
}
