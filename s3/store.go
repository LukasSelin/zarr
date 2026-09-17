// Package s3 is a zarr.Store kept in an Amazon S3 bucket, or in anything
// that speaks its API.
//
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	s := s3.New(awss3.NewFromConfig(cfg), "my-bucket", "fwi.zarr")
//	a, _ := zarr.OpenArray(ctx, s, "isi")
//
// It is a zarr.RangeGetter, so a sharded array is read a shard's index and
// the chunks it needs at a time rather than a shard at a time.
//
// A Store holds nothing of its own beyond what New was given, and the client
// is safe to use from several goroutines at once, so a region read has as
// many requests in flight as zarr.Array.Concurrency allows.
//
// A key that is not in the bucket is zarr.ErrNotFound, which an array reads
// as a chunk of nothing but its fill value. S3 answers a GetObject of a key
// that is not there with 403 Access Denied rather than 404, though, unless
// whoever asks may s3:ListBucket on the bucket. Without that permission every
// chunk never written is an error, not fill: grant s3:ListBucket along with
// s3:GetObject.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/LukasSelin/zarr"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Client is what of *s3.Client the store uses.
type Client interface {
	GetObject(ctx context.Context, in *awss3.GetObjectInput, opts ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, in *awss3.DeleteObjectInput, opts ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
}

// Store is a zarr.Store and zarr.RangeGetter of the objects in a bucket
// under a prefix: the key "isi/c/0/0" of a store with the prefix "fwi.zarr"
// is the object "fwi.zarr/isi/c/0/0".
type Store struct {
	client Client
	bucket string
	prefix string
	// MaxObjectBytes, if more than 0, is the most Get reads of one object;
	// a larger one is an error rather than all of it in memory.
	MaxObjectBytes int64
}

// New returns the store of the objects in bucket under prefix, which may be
// "" for the whole bucket.
func New(c Client, bucket, prefix string) *Store {
	return &Store{client: c, bucket: bucket, prefix: strings.Trim(prefix, "/")}
}

func (s *Store) object(key string) *string {
	if s.prefix == "" {
		return aws.String(key)
	}
	return aws.String(s.prefix + "/" + key)
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: s.object(key)})
	if err != nil {
		return nil, s.fail(key, err)
	}
	defer out.Body.Close()
	if s.MaxObjectBytes <= 0 {
		b, err := io.ReadAll(out.Body)
		if err != nil {
			return nil, fmt.Errorf("zarr/s3: %s: %w", key, err)
		}
		return b, nil
	}
	b, err := io.ReadAll(io.LimitReader(out.Body, s.MaxObjectBytes+1))
	if err != nil {
		return nil, fmt.Errorf("zarr/s3: %s: %w", key, err)
	}
	if int64(len(b)) > s.MaxObjectBytes {
		return nil, fmt.Errorf("zarr/s3: %s is more than %d bytes", key, s.MaxObjectBytes)
	}
	return b, nil
}

// GetRange reads length bytes of the object from offset in one request. A
// negative offset is a suffix range, the last -offset bytes, so the index at
// the end of a shard costs one request and no HeadObject. A range of no bytes
// from a positive offset does cost a HeadObject, as S3 has no range for it.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	outside := func() error {
		return fmt.Errorf("zarr/s3: range %d+%d is outside %s", offset, length, key)
	}
	if length < 0 || offset < 0 && length > -offset {
		return nil, outside()
	}
	var rng string
	want := length
	switch {
	case offset < 0:
		rng, want = fmt.Sprintf("bytes=%d", offset), -offset
	case length > 0:
		rng = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	default:
		head, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &s.bucket, Key: s.object(key)})
		if err != nil {
			return nil, s.fail(key, err)
		}
		if offset > aws.ToInt64(head.ContentLength) {
			return nil, outside()
		}
		return []byte{}, nil
	}
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: s.object(key), Range: &rng})
	if err != nil {
		if status(err) == http.StatusRequestedRangeNotSatisfiable {
			return nil, outside()
		}
		return nil, s.fail(key, err)
	}
	defer out.Body.Close()
	// S3 answers a range that runs past the end with what there is of it,
	// and a suffix longer than the object with all of it; a server that
	// ignores Range answers with all of it. Any of those is more or fewer
	// bytes than asked for.
	b, err := io.ReadAll(io.LimitReader(out.Body, want+1))
	if err != nil {
		return nil, fmt.Errorf("zarr/s3: %s: %w", key, err)
	}
	if int64(len(b)) != want {
		return nil, outside()
	}
	return b[:length], nil
}

// Set puts value under key. A put to S3 is whole or not at all, so a reader
// never sees half a value.
func (s *Store) Set(ctx context.Context, key string, value []byte) error {
	if err := zarr.ValidKey(key); err != nil {
		return err
	}
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           s.object(key),
		Body:          bytes.NewReader(value),
		ContentLength: aws.Int64(int64(len(value))),
	})
	if err != nil {
		return s.fail(key, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &s.bucket, Key: s.object(key)})
	if err != nil {
		if err := s.fail(key, err); !errors.Is(err, zarr.ErrNotFound) {
			return err
		}
	}
	return nil
}

// fail is err from S3 about key as the store's error: zarr.ErrNotFound for a
// key that is not there, and never for a bucket that is not.
func (s *Store) fail(key string, err error) error {
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "NoSuchBucket" {
		return fmt.Errorf("zarr/s3: bucket %q: %w", s.bucket, err)
	}
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) || status(err) == http.StatusNotFound {
		return fmt.Errorf("%w: %s", zarr.ErrNotFound, key)
	}
	if status(err) == http.StatusForbidden {
		return fmt.Errorf("zarr/s3: %s: access denied, which is also what S3 answers for a key that is not there unless s3:ListBucket is allowed: %w", key, err)
	}
	return fmt.Errorf("zarr/s3: %s: %w", key, err)
}

// status is the HTTP status of the response err is about, or 0.
func status(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

var (
	_ zarr.Store       = (*Store)(nil)
	_ zarr.RangeGetter = (*Store)(nil)
	_ Client           = (*awss3.Client)(nil)
)
