// Package azblob is a zarr.Store kept in an Azure Blob Storage container.
//
//	cred, _ := azidentity.NewDefaultAzureCredential(nil)
//	c, _ := container.NewClient("https://account.blob.core.windows.net/fwi", cred, nil)
//	s := azblob.New(c, "fwi.zarr")
//	a, _ := zarr.OpenArray(ctx, s, "isi")
//
// It is a zarr.RangeGetter, so a sharded array is read a shard's index and
// the chunks it needs at a time rather than a shard at a time. Blob Storage
// has no range for the last bytes of a blob, so the index at the end of a
// shard costs a read of the blob's properties and then of the range; the
// range is asked for only of the blob the properties were of.
//
// It is a zarr.DirLister as well, listing one level of a hierarchy with the
// delimiter "/", which rolls a level up into one prefix.
//
// A Store holds nothing of its own beyond what New was given, and a
// *container.Client is safe to use from several goroutines at once, so a
// region read has as many requests in flight as zarr.Array.Concurrency
// allows.
//
// A blob that is not there is zarr.ErrNotFound, which an array reads as a
// chunk of nothing but its fill value; a container that is not there is an
// error, never ErrNotFound.
package azblob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/LukasSelin/zarr"
)

// Store is a zarr.Store and zarr.RangeGetter of the blobs in a container
// under a prefix: the key "isi/c/0/0" of a store with the prefix "fwi.zarr"
// is the blob "fwi.zarr/isi/c/0/0".
type Store struct {
	client *container.Client
	prefix string
	// MaxObjectBytes, if more than 0, is the most Get reads of one blob; a
	// larger one is an error rather than all of it in memory.
	MaxObjectBytes int64
}

// New returns the store of the blobs in the container under prefix, which
// may be "" for the whole container.
func New(c *container.Client, prefix string) *Store {
	return &Store{client: c, prefix: strings.Trim(prefix, "/")}
}

func (s *Store) name(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *Store) blob(key string) *blob.Client {
	return s.client.NewBlobClient(s.name(key))
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.blob(key).DownloadStream(ctx, nil)
	if err != nil {
		return nil, s.fail(key, err)
	}
	defer resp.Body.Close()
	if s.MaxObjectBytes <= 0 {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("zarr/azblob: %s: %w", key, err)
		}
		return b, nil
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, s.MaxObjectBytes+1))
	if err != nil {
		return nil, fmt.Errorf("zarr/azblob: %s: %w", key, err)
	}
	if int64(len(b)) > s.MaxObjectBytes {
		return nil, fmt.Errorf("zarr/azblob: %s is more than %d bytes", key, s.MaxObjectBytes)
	}
	return b, nil
}

// GetRange reads length bytes of the blob from offset. A range from a
// positive offset is one request. A negative offset counts back from the
// end, which Blob Storage has no range for, so it reads the blob's
// properties for its length first, and then the range on condition that
// the blob is the one the properties were of. So does a range of no bytes
// from a positive offset, and no more.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	outside := func() error {
		return fmt.Errorf("zarr/azblob: range %d+%d is outside %s", offset, length, key)
	}
	if length < 0 || offset < 0 && length > -offset {
		return nil, outside()
	}
	b := s.blob(key)
	opts := &blob.DownloadStreamOptions{}
	if offset < 0 || length == 0 {
		props, err := b.GetProperties(ctx, nil)
		if err != nil {
			return nil, s.fail(key, err)
		}
		size := deref(props.ContentLength)
		if offset < 0 {
			offset += size
		}
		if offset < 0 || offset > size {
			return nil, outside()
		}
		if length == 0 {
			return []byte{}, nil
		}
		opts.AccessConditions = &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: props.ETag}}
	}
	opts.Range = blob.HTTPRange{Offset: offset, Count: length}
	resp, err := b.DownloadStream(ctx, opts)
	if err != nil {
		if bloberror.HasCode(err, bloberror.InvalidRange) || status(err) == http.StatusRequestedRangeNotSatisfiable {
			return nil, outside()
		}
		return nil, s.fail(key, err)
	}
	defer resp.Body.Close()
	// Blob Storage answers a range that runs past the end with what there
	// is of it, and a server that ignores ranges with all of it. Either is
	// more or fewer bytes than asked for.
	got, err := io.ReadAll(io.LimitReader(resp.Body, length+1))
	if err != nil {
		return nil, fmt.Errorf("zarr/azblob: %s: %w", key, err)
	}
	if int64(len(got)) != length {
		return nil, outside()
	}
	return got, nil
}

// Set puts value under key, as a block blob in one request. A Put Blob is
// whole or not at all, so a reader never sees half a value, and the client
// retries it from the value on a failure on the way.
func (s *Store) Set(ctx context.Context, key string, value []byte) error {
	if err := zarr.ValidKey(key); err != nil {
		return err
	}
	_, err := s.client.NewBlockBlobClient(s.name(key)).Upload(ctx, streaming.NopCloser(bytes.NewReader(value)), nil)
	if err != nil {
		return s.fail(key, err)
	}
	return nil
}

// Delete removes the blob under key and its snapshots, which Blob Storage
// will not delete a blob without.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.blob(key).Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude)})
	if err != nil {
		if err := s.fail(key, err); !errors.Is(err, zarr.ErrNotFound) {
			return err
		}
	}
	return nil
}

// key is the store's key for a blob, and whether the blob is under the
// store's prefix at all.
func (s *Store) key(name string) (string, bool) {
	if s.prefix == "" {
		return name, true
	}
	return strings.CutPrefix(name, s.prefix+"/")
}

// List walks the blobs under the prefix, a page of up to five thousand at a
// time.
func (s *Store) List(ctx context.Context, prefix string, fn func(key string) error) error {
	if err := zarr.ValidPrefix(prefix); err != nil {
		return err
	}
	pager := s.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: to.Ptr(s.name(prefix))})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return s.fail(prefix, err)
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			key, ok := s.key(deref(item.Name))
			if !ok || zarr.ValidKey(key) != nil {
				// A blob no Set could have written, such as a directory
				// marker, whose name ends in "/".
				continue
			}
			if err := fn(key); err != nil {
				return err
			}
		}
	}
	return nil
}

// ListDir walks one level, with the delimiter "/", so a group is listed
// without reading the keys of the arrays under it.
func (s *Store) ListDir(ctx context.Context, prefix string, fn func(name string) error) error {
	if err := zarr.ValidPrefix(prefix); err != nil {
		return err
	}
	pager := s.client.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{Prefix: to.Ptr(s.name(prefix))})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return s.fail(prefix, err)
		}
		if page.Segment == nil {
			continue
		}
		for _, p := range page.Segment.BlobPrefixes {
			key, ok := s.key(deref(p.Name))
			if !ok {
				continue
			}
			// A prefix carries the delimiter already.
			if err := fn(strings.TrimPrefix(key, prefix)); err != nil {
				return err
			}
		}
		for _, item := range page.Segment.BlobItems {
			key, ok := s.key(deref(item.Name))
			if !ok || zarr.ValidKey(key) != nil {
				continue
			}
			if err := fn(strings.TrimPrefix(key, prefix)); err != nil {
				return err
			}
		}
	}
	return nil
}

// fail is err from Blob Storage about key as the store's error:
// zarr.ErrNotFound for a blob that is not there, and never for a container
// that is not.
func (s *Store) fail(key string, err error) error {
	switch {
	case bloberror.HasCode(err, bloberror.ContainerNotFound):
		return fmt.Errorf("zarr/azblob: container %q: %w", s.container(), err)
	case bloberror.HasCode(err, bloberror.BlobNotFound),
		// A HEAD, as GetProperties is, has no body to say which.
		status(err) == http.StatusNotFound && code(err) == "":
		return fmt.Errorf("%w: %s", zarr.ErrNotFound, key)
	}
	return fmt.Errorf("zarr/azblob: %s: %w", key, err)
}

// container is the name of the store's container: the last segment of the
// client's URL, without its query, which may be a signature.
func (s *Store) container() string {
	u, err := url.Parse(s.client.URL())
	if err != nil {
		return ""
	}
	return path.Base(u.Path)
}

// status is the HTTP status of the response err is about, or 0.
func status(err error) int {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return re.StatusCode
	}
	return 0
}

// code is the error code of the response err is about, or "".
func code(err error) string {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return re.ErrorCode
	}
	return ""
}

// deref is what p points to, or the zero value if it is nil.
func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

var (
	_ zarr.Store       = (*Store)(nil)
	_ zarr.RangeGetter = (*Store)(nil)
	_ zarr.DirLister   = (*Store)(nil)
)
