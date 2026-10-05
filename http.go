package zarr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// HTTPStore is a Store read over plain HTTP: the key "isi/c/0/0" of a store
// at "https://example.com/fwi.zarr" is a GET of
// "https://example.com/fwi.zarr/isi/c/0/0". It is for a hierarchy published
// as static files - by a web server, a CDN, or the public endpoint of a
// bucket - and it is read only: Set and Delete return an error wrapping
// ErrReadOnly, so Write, CreateArray, CreateGroup and Delete do too.
//
// Plain HTTP has no way to list, so List returns an error wrapping
// errors.ErrUnsupported, and so do ListDir, Group.Children and Delete, which
// list. Open the arrays of a group by their names.
//
// It is a RangeGetter, so a sharded array is read a shard's index and the
// chunks it needs at a time, with Range requests. A server that ignores Range
// and answers with the whole value is not trusted to be right about it: the
// range is cut out of what it sends, so reading still works, but each range
// costs a read of the value up to its end, and a shard's index at the end
// costs all of it.
//
// A key that is not there is ErrNotFound if the server answers 404 or 410.
// Some answer 403 instead - the public endpoint of a bucket that may not be
// listed, for one - which is an error unless ForbiddenIsNotFound is set; with
// it set, a chunk the server forbids reads as fill rather than failing, so
// set it only for a server known to answer 403 for what is not there.
//
// It holds nothing of its own beyond what NewHTTPStore was given, and an
// http.Client is safe to use from several goroutines at once, so a region
// read has as many requests in flight as Array.Concurrency allows. Its
// fields are to be set before it is first used.
type HTTPStore struct {
	client *http.Client
	// base is the URL keys are under, with no "/" at its end and no query,
	// and query the query, which every request carries.
	base, query string
	// ForbiddenIsNotFound reads a 403 as a key that is not there.
	ForbiddenIsNotFound bool
	// MaxObjectBytes, if more than 0, is the most Get reads of one value; a
	// larger one is an error rather than all of it in memory. It is also the
	// most a range from a server that ignores Range is read through for.
	// NewHTTPStore sets it to 4 GiB, twice the most the elements of one chunk
	// or shard may take, so that a server cannot make a read allocate
	// without bound.
	MaxObjectBytes int64
}

// NewHTTPStore returns the store of the keys under baseURL, which must be
// http or https. A query in baseURL, such as the signature of a signed URL,
// is sent with every request. client may be nil for http.DefaultClient; give
// one whose Transport adds them for a server that wants headers, such as
// Authorization.
func NewHTTPStore(baseURL string, client *http.Client) (*HTTPStore, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("zarr: base URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("zarr: base URL %q is not an http or https URL", baseURL)
	}
	if client == nil {
		client = http.DefaultClient
	}
	query := u.RawQuery
	u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment = "", false, "", ""
	u.Path, u.RawPath = strings.TrimRight(u.Path, "/"), strings.TrimRight(u.RawPath, "/")
	return &HTTPStore{client: client, base: u.String(), query: query, MaxObjectBytes: 1 << 32}, nil
}

// url is the URL of key, each of its segments escaped.
func (s *HTTPStore) url(key string) string {
	segs := strings.Split(key, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	u := s.base + "/" + strings.Join(segs, "/")
	if s.query != "" {
		u += "?" + s.query
	}
	return u
}

// do sends a request for key, with the Range rng if it is not "". The error of
// a request that was never answered is said without the URL, whose query may
// be a signature.
func (s *HTTPStore) do(ctx context.Context, method, key, rng string) (*http.Response, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, s.url(key), nil)
	if err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("zarr: %s %s: %w", method, key, err)
	}
	return resp, nil
}

func (s *HTTPStore) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.do(ctx, http.MethodGet, key, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, s.fail(key, resp)
	}
	if s.MaxObjectBytes <= 0 {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("zarr: %s: %w", key, err)
		}
		return b, nil
	}
	if resp.ContentLength > s.MaxObjectBytes {
		return nil, s.tooLarge(key)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, s.MaxObjectBytes+1))
	if err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	if int64(len(b)) > s.MaxObjectBytes {
		return nil, s.tooLarge(key)
	}
	return b, nil
}

func (s *HTTPStore) tooLarge(key string) error {
	return fmt.Errorf("zarr: %s is more than %d bytes", key, s.MaxObjectBytes)
}

// GetRange reads length bytes of the value from offset in one request. A
// negative offset is a suffix range, the last -offset bytes, so the index at
// the end of a shard costs one request and no HEAD. A range of no bytes from
// a positive offset does cost a HEAD, as HTTP has no range for it.
//
// A 206 must say, in its Content-Range, that it is the range asked for, and
// be that many bytes. A 200 is the whole value, from a server that ignores
// Range, and the range is cut out of it.
func (s *HTTPStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	outside := func() error {
		return fmt.Errorf("zarr: range %d+%d is outside %s", offset, length, key)
	}
	if length < 0 || offset < 0 && length > -offset || offset > math.MaxInt64-length {
		return nil, outside()
	}
	if offset >= 0 && length == 0 {
		return s.emptyRange(ctx, key, offset, outside)
	}
	want, rng := length, fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	if offset < 0 {
		want, rng = -offset, fmt.Sprintf("bytes=%d", offset)
	}
	if s.MaxObjectBytes > 0 && want > s.MaxObjectBytes {
		return nil, s.tooLarge(key)
	}
	resp, err := s.do(ctx, http.MethodGet, key, rng)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		return s.cutRange(key, resp.Body, offset, length, outside)
	case http.StatusRequestedRangeNotSatisfiable:
		return nil, outside()
	default:
		return nil, s.fail(key, resp)
	}
	first, last, size, ok := parseContentRange(resp.Header.Get("Content-Range"))
	switch {
	case !ok:
		return nil, fmt.Errorf("zarr: %s: a 206 without a Content-Range of one range: %q", key, resp.Header.Get("Content-Range"))
	// A server answers a range that runs past the end with what there is
	// of it, and a suffix longer than the value with all of it: either is
	// fewer bytes than asked for.
	case last-first+1 != want:
		return nil, outside()
	case offset >= 0 && first != offset, offset < 0 && (size < 0 || last != size-1):
		return nil, fmt.Errorf("zarr: %s: asked for %s and the server answered with bytes %d-%d/%d", key, rng, first, last, size)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, want+1))
	if err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	if int64(len(b)) != want {
		return nil, fmt.Errorf("zarr: %s: a range of %d bytes answered with %d", key, want, len(b))
	}
	return b[:length], nil
}

// cutRange is the range of a whole value body, from a server that ignored
// Range. It reads no further into body than the range's end, and no further
// than MaxObjectBytes into it at all.
func (s *HTTPStore) cutRange(key string, body io.Reader, offset, length int64, outside func() error) ([]byte, error) {
	if offset >= 0 {
		if s.MaxObjectBytes > 0 && offset+length > s.MaxObjectBytes {
			return nil, s.tooLarge(key)
		}
		_, err := io.CopyN(io.Discard, body, offset)
		if errors.Is(err, io.EOF) {
			return nil, outside()
		}
		if err != nil {
			return nil, fmt.Errorf("zarr: %s: %w", key, err)
		}
		b, err := io.ReadAll(io.LimitReader(body, length))
		if err != nil {
			return nil, fmt.Errorf("zarr: %s: %w", key, err)
		}
		if int64(len(b)) != length {
			return nil, outside()
		}
		return b, nil
	}
	var all []byte
	var err error
	if s.MaxObjectBytes > 0 {
		all, err = io.ReadAll(io.LimitReader(body, s.MaxObjectBytes+1))
		if err == nil && int64(len(all)) > s.MaxObjectBytes {
			return nil, s.tooLarge(key)
		}
	} else {
		all, err = io.ReadAll(body)
	}
	if err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	if int64(len(all)) < -offset {
		return nil, outside()
	}
	at := int64(len(all)) + offset
	return all[at : at+length], nil
}

// emptyRange is a range of no bytes from offset: no bytes, if the value is
// at least offset long. It needs the value's length, which a HEAD says.
func (s *HTTPStore) emptyRange(ctx context.Context, key string, offset int64, outside func() error) ([]byte, error) {
	resp, err := s.do(ctx, http.MethodHead, key, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, s.fail(key, resp)
	}
	if resp.ContentLength < 0 {
		return nil, fmt.Errorf("zarr: %s: the server did not say how long it is", key)
	}
	if offset > resp.ContentLength {
		return nil, outside()
	}
	return []byte{}, nil
}

// parseContentRange reads a Content-Range of one range, "bytes 0-99/1000",
// with size -1 for a "*" in place of it.
func parseContentRange(h string) (first, last, size int64, ok bool) {
	rest, ok := strings.CutPrefix(h, "bytes ")
	if !ok {
		return 0, 0, 0, false
	}
	span, total, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, 0, 0, false
	}
	lo, hi, ok := strings.Cut(span, "-")
	if !ok {
		return 0, 0, 0, false
	}
	var err1, err2 error
	first, err1 = strconv.ParseInt(lo, 10, 64)
	last, err2 = strconv.ParseInt(hi, 10, 64)
	if err1 != nil || err2 != nil || first < 0 || last < first {
		return 0, 0, 0, false
	}
	size = -1
	if total != "*" {
		n, err := strconv.ParseInt(total, 10, 64)
		if err != nil || n <= last {
			return 0, 0, 0, false
		}
		size = n
	}
	return first, last, size, true
}

// fail is the store's error for an answer that is not the value: ErrNotFound
// for a key that is not there.
func (s *HTTPStore) fail(key string, resp *http.Response) error {
	switch {
	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusGone:
		return fmt.Errorf("%w: %s", ErrNotFound, key)
	case resp.StatusCode == http.StatusForbidden && s.ForbiddenIsNotFound:
		return fmt.Errorf("%w: %s (403)", ErrNotFound, key)
	case resp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("zarr: %s: 403 Forbidden, which some servers answer for a key that is not there: set ForbiddenIsNotFound if this one does", key)
	}
	return fmt.Errorf("zarr: %s: %s", key, resp.Status)
}

// Set returns an error wrapping ErrReadOnly.
func (s *HTTPStore) Set(ctx context.Context, key string, value []byte) error {
	return fmt.Errorf("%w: set %s over HTTP", ErrReadOnly, key)
}

// Delete returns an error wrapping ErrReadOnly.
func (s *HTTPStore) Delete(ctx context.Context, key string) error {
	return fmt.Errorf("%w: delete %s over HTTP", ErrReadOnly, key)
}

// List returns an error wrapping errors.ErrUnsupported: plain HTTP has no
// way to list.
func (s *HTTPStore) List(ctx context.Context, prefix string, fn func(key string) error) error {
	if err := checkPrefix(prefix); err != nil {
		return err
	}
	return fmt.Errorf("zarr: list %q over HTTP, which has no way to list: %w", prefix, errors.ErrUnsupported)
}

var (
	_ Store       = (*HTTPStore)(nil)
	_ RangeGetter = (*HTTPStore)(nil)
)
