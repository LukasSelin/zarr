package zarr

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Store is where a hierarchy keeps its keys. Keys are slash-separated,
// such as "height/c/0/1"; the metadata of a node is under "zarr.json" at its
// path.
//
// A Store's methods may be called from several goroutines at once: a region
// read or written fetches as many keys together as Array.Concurrency allows.
type Store interface {
	// Get returns the value under key, or an error wrapping ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, error)
	// Set puts value under key, replacing what was there.
	Set(ctx context.Context, key string, value []byte) error
	// Delete removes key. A key that is not there is not an error.
	Delete(ctx context.Context, key string) error
}

// RangeGetter is a Store that can read part of a value. A sharded array
// read from one fetches the index of a shard and the chunks it needs rather
// than the whole shard.
type RangeGetter interface {
	// GetRange returns length bytes of the value under key from offset,
	// where a negative offset counts back from the end. It returns an error
	// wrapping ErrNotFound for a key it does not hold, and an error for a
	// range that runs past either end.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
}

// span is where offset and length fall in a value of size bytes.
func span(key string, size, offset, length int64) (int64, error) {
	if offset < 0 {
		offset += size
	}
	if offset < 0 || length < 0 || offset+length > size {
		return 0, fmt.Errorf("zarr: range %d+%d is outside %s, of %d bytes", offset, length, key, size)
	}
	return offset, nil
}

var (
	// ErrNotFound is what a Store returns for a key it does not hold, and
	// what opening a node that is not there wraps.
	ErrNotFound = errors.New("zarr: not found")
	// ErrExists is what creating a node over one already there wraps.
	ErrExists = errors.New("zarr: already exists")
	// ErrUnsupported is what opening metadata this package cannot honour
	// wraps: an unknown codec, data type or extension it must understand.
	ErrUnsupported = errors.New("zarr: unsupported")
)

// ValidKey refuses keys that could reach outside a store: empty ones, ones
// beginning or ending in a slash, and ones with an empty, "." or ".."
// segment, a backslash or a colon. A Store kept elsewhere should refuse to
// Set what this refuses.
func ValidKey(key string) error { return checkKey(key) }

// checkKey refuses keys that could reach outside a store.
func checkKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") || strings.ContainsAny(key, "\\:") {
		return fmt.Errorf("zarr: bad key %q", key)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("zarr: bad key %q", key)
		}
	}
	return nil
}

// MemoryStore is a Store held in memory.
type MemoryStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{m: map[string][]byte{}}
}

func (s *MemoryStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return append([]byte(nil), v...), nil
}

func (s *MemoryStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	at, err := span(key, int64(len(v)), offset, length)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), v[at:at+length]...), nil
}

func (s *MemoryStore) Set(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = append([]byte(nil), value...)
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

// Keys is every key held, sorted.
func (s *MemoryStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// DirStore is a Store kept as files under a directory, one file a key.
type DirStore struct {
	root string
}

// NewDirStore returns a DirStore rooted at dir, which need not exist yet.
func NewDirStore(dir string) *DirStore {
	return &DirStore{root: dir}
}

func (s *DirStore) path(key string) (string, error) {
	if err := checkKey(key); err != nil {
		return "", err
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

func (s *DirStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return b, err
}

func (s *DirStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	at, err := span(key, info.Size(), offset, length)
	if err != nil {
		return nil, err
	}
	b := make([]byte, length)
	if _, err := f.ReadAt(b, at); err != nil {
		return nil, err
	}
	return b, nil
}

// Set writes to a file beside the key's and renames it into place, so that
// a reader never sees half a value.
func (s *DirStore) Set(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.path(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(p)+".*")
	if err != nil {
		return err
	}
	_, werr := f.Write(value)
	cerr := f.Close()
	if werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(f.Name(), p)
	}
	if werr != nil {
		os.Remove(f.Name())
	}
	return werr
}

func (s *DirStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
