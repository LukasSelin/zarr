// Package gcs is a zarr.Store kept in a Google Cloud Storage bucket.
//
//	c, _ := storage.NewClient(ctx)
//	s := gcs.New(c.Bucket("my-bucket"), "fwi.zarr")
//	a, _ := zarr.OpenArray(ctx, s, "isi")
//
// It is a zarr.RangeGetter, so a sharded array is read a shard's index and
// the chunks it needs at a time rather than a shard at a time.
//
// It is a zarr.DirLister as well, listing one level of a hierarchy with the
// delimiter "/", which rolls a level up into one prefix.
//
// A Store holds nothing of its own beyond what New was given, and a
// *storage.BucketHandle is safe to use from several goroutines at once, so a
// region read has as many requests in flight as zarr.Array.Concurrency
// allows.
//
// A key that is not in the bucket is zarr.ErrNotFound, which an array reads
// as a chunk of nothing but its fill value. A read in a bucket that is not
// there is answered the same way, so opening a node in one is ErrNotFound
// too; a listing of one says the bucket is missing.
//
// The client retries an upload only if it is told the upload is idempotent.
// A Set is - the same bytes under the same key - so give the handle a policy
// of storage.RetryAlways to have a Set that failed on the way retried:
//
//	b := c.Bucket("my-bucket").Retryer(storage.WithPolicy(storage.RetryAlways))
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/LukasSelin/zarr"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// Store is a zarr.Store and zarr.RangeGetter of the objects in a bucket
// under a prefix: the key "isi/c/0/0" of a store with the prefix "fwi.zarr"
// is the object "fwi.zarr/isi/c/0/0".
type Store struct {
	bucket *storage.BucketHandle
	prefix string
	// MaxObjectBytes, if more than 0, is the most Get reads of one object;
	// a larger one is an error rather than all of it in memory.
	MaxObjectBytes int64
}

// New returns the store of the objects in bucket under prefix, which may be
// "" for the whole bucket.
func New(bucket *storage.BucketHandle, prefix string) *Store {
	return &Store{bucket: bucket, prefix: strings.Trim(prefix, "/")}
}

func (s *Store) name(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *Store) object(key string) *storage.ObjectHandle {
	return s.bucket.Object(s.name(key))
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := s.object(key).NewReader(ctx)
	if err != nil {
		return nil, s.fail(key, err)
	}
	defer r.Close()
	if s.MaxObjectBytes <= 0 {
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("zarr/gcs: %s: %w", key, err)
		}
		return b, nil
	}
	b, err := io.ReadAll(io.LimitReader(r, s.MaxObjectBytes+1))
	if err != nil {
		return nil, fmt.Errorf("zarr/gcs: %s: %w", key, err)
	}
	if int64(len(b)) > s.MaxObjectBytes {
		return nil, fmt.Errorf("zarr/gcs: %s is more than %d bytes", key, s.MaxObjectBytes)
	}
	return b, nil
}

// GetRange reads length bytes of the object from offset in one request. A
// negative offset is a suffix range, the last -offset bytes, so the index at
// the end of a shard costs one request and no read of the object's
// attributes. A range of no bytes from a positive offset does cost one, as
// there is no range for it.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	outside := func() error {
		return fmt.Errorf("zarr/gcs: range %d+%d is outside %s", offset, length, key)
	}
	if length < 0 || offset < 0 && length > -offset {
		return nil, outside()
	}
	obj := s.object(key)
	var r *storage.Reader
	var err error
	want := length
	switch {
	case offset < 0:
		// The client reads a suffix with a length of -1: to the end.
		r, err = obj.NewRangeReader(ctx, offset, -1)
		want = -offset
	case length > 0:
		r, err = obj.NewRangeReader(ctx, offset, length)
	default:
		attrs, err := obj.Attrs(ctx)
		if err != nil {
			return nil, s.fail(key, err)
		}
		if offset > attrs.Size {
			return nil, outside()
		}
		return []byte{}, nil
	}
	if err != nil {
		if status(err) == http.StatusRequestedRangeNotSatisfiable {
			return nil, outside()
		}
		return nil, s.fail(key, err)
	}
	defer r.Close()
	// GCS answers a range that runs past the end with what there is of it,
	// a suffix longer than the object with all of it, and any range of an
	// object it decompresses as it serves - one stored with Content-Encoding
	// gzip - with all of it. Any of those is more or fewer bytes than asked
	// for.
	b, err := io.ReadAll(io.LimitReader(r, want+1))
	if err != nil {
		return nil, fmt.Errorf("zarr/gcs: %s: %w", key, err)
	}
	if int64(len(b)) != want {
		return nil, outside()
	}
	return b[:length], nil
}

// Set puts value under key. An upload to GCS is whole or not at all, so a
// reader never sees half a value. It goes in one request, with a buffer the
// size of the value to retry it from rather than the client's 16 MiB, so
// that Array.Concurrency writes at once do not hold that much each.
func (s *Store) Set(ctx context.Context, key string, value []byte) error {
	if err := zarr.ValidKey(key); err != nil {
		return err
	}
	// Cancelling the context is what abandons an upload rather than
	// finishing it with what was written so far.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := s.object(key).NewWriter(ctx)
	if len(value) < googleapi.DefaultUploadChunkSize {
		w.ChunkSize = len(value) + 1
	}
	if _, err := w.Write(value); err != nil {
		return s.fail(key, err)
	}
	if err := w.Close(); err != nil {
		return s.fail(key, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.object(key).Delete(ctx); err != nil {
		if err := s.fail(key, err); !errors.Is(err, zarr.ErrNotFound) {
			return err
		}
	}
	return nil
}

// key is the store's key for an object, and whether the object is under the
// store's prefix at all.
func (s *Store) key(name string) (string, bool) {
	if s.prefix == "" {
		return name, true
	}
	return strings.CutPrefix(name, s.prefix+"/")
}

// objects walks the objects under prefix, with delim between the levels of
// a key if it is not "". An object rolled up by the delimiter comes as
// attributes with nothing but Prefix set.
func (s *Store) objects(ctx context.Context, prefix, delim string, fn func(*storage.ObjectAttrs) error) error {
	if err := zarr.ValidPrefix(prefix); err != nil {
		return err
	}
	q := &storage.Query{Prefix: s.name(prefix), Delimiter: delim}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return err
	}
	it := s.bucket.Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return s.fail(prefix, err)
		}
		if err := fn(attrs); err != nil {
			return err
		}
	}
}

// List walks the objects under the prefix, a page at a time.
func (s *Store) List(ctx context.Context, prefix string, fn func(key string) error) error {
	return s.objects(ctx, prefix, "", func(attrs *storage.ObjectAttrs) error {
		// An object no Set could have written, such as the folder
		// placeholder the console makes, whose name ends in "/", is passed
		// over.
		if key, ok := s.key(attrs.Name); ok && zarr.ValidKey(key) == nil {
			return fn(key)
		}
		return nil
	})
}

// ListDir walks one level, with the delimiter "/", so a group is listed
// without reading the keys of the arrays under it.
func (s *Store) ListDir(ctx context.Context, prefix string, fn func(name string) error) error {
	return s.objects(ctx, prefix, "/", func(attrs *storage.ObjectAttrs) error {
		if attrs.Prefix != "" {
			key, ok := s.key(attrs.Prefix)
			if !ok {
				return nil
			}
			// A prefix carries the delimiter already.
			return fn(strings.TrimPrefix(key, prefix))
		}
		if key, ok := s.key(attrs.Name); ok && zarr.ValidKey(key) == nil {
			return fn(strings.TrimPrefix(key, prefix))
		}
		return nil
	})
}

// fail is err from GCS about key as the store's error: zarr.ErrNotFound for
// a key that is not there, and never for a bucket that is not.
func (s *Store) fail(key string, err error) error {
	switch {
	case errors.Is(err, storage.ErrBucketNotExist):
		return fmt.Errorf("zarr/gcs: bucket %q: %w", s.bucket.BucketName(), err)
	case errors.Is(err, storage.ErrObjectNotExist):
		return fmt.Errorf("%w: %s", zarr.ErrNotFound, key)
	}
	return fmt.Errorf("zarr/gcs: %s: %w", key, err)
}

// status is the HTTP status of the response err is about, or 0.
func status(err error) int {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code
	}
	return 0
}

var (
	_ zarr.Store       = (*Store)(nil)
	_ zarr.RangeGetter = (*Store)(nil)
	_ zarr.DirLister   = (*Store)(nil)
)
