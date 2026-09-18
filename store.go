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
	// List calls fn with every key that begins with prefix, in no
	// particular order, and stops at the first error fn returns, which it
	// returns as its own. A prefix is a string and not a path: "height" is
	// also the keys of "heightmap", and "" is every key the store holds. A
	// store need not answer a List whose keys fn is writing or deleting
	// under it.
	List(ctx context.Context, prefix string, fn func(key string) error) error
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

// DirLister is a Store that can list one level of its keys, the way S3 lists
// with a delimiter. Walking a hierarchy through one costs a request for each
// level rather than a request for every key underneath it.
type DirLister interface {
	// ListDir calls fn with what follows prefix in each key that begins
	// with it, cut at the first "/" and keeping it: "zarr.json" for the key
	// "zarr.json", and "height/" once for however many keys begin
	// "height/". It stops at the first error fn returns, which it returns.
	ListDir(ctx context.Context, prefix string, fn func(name string) error) error
}

// ListDir walks the one level under prefix of s: the names in it, a name
// that has keys under it with a "/" after it. It uses the store's own
// ListDir if it has one and derives it from List if not, which reads every
// key under prefix rather than the one level - for a group over a large
// array, every chunk of it.
func ListDir(ctx context.Context, s Store, prefix string, fn func(name string) error) error {
	if d, ok := s.(DirLister); ok {
		return d.ListDir(ctx, prefix, fn)
	}
	seen := map[string]bool{}
	return s.List(ctx, prefix, func(key string) error {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			return nil
		}
		name := dirName(rest)
		if seen[name] {
			return nil
		}
		seen[name] = true
		return fn(name)
	})
}

// dirName is rest cut at its first "/", which it keeps.
func dirName(rest string) string {
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i+1]
	}
	return rest
}

// listStop carries the error a List callback returned out of a walk that
// reads an error of its own, such as the fs.SkipDir of filepath.WalkDir.
type listStop struct{ err error }

func (e listStop) Error() string { return e.err.Error() }
func (e listStop) Unwrap() error { return e.err }

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

// ValidPrefix refuses prefixes that could reach outside a store. It is
// ValidKey but for "", which is every key the store holds, and for a prefix
// ending in "/", which is everything under a path; the last segment of a
// prefix is part of a name rather than a whole one. A Store kept elsewhere
// should refuse to List what this refuses.
func ValidPrefix(prefix string) error { return checkPrefix(prefix) }

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

// checkPrefix refuses prefixes that could reach outside a store.
func checkPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	bad := func() error { return fmt.Errorf("zarr: bad prefix %q", prefix) }
	if strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "\\:") {
		return bad()
	}
	segs := strings.Split(prefix, "/")
	for i, seg := range segs {
		// The last segment may be "" - a prefix ending in "/" - or part of
		// a name; no other may be empty, and none may be "." or "..".
		if seg == "." || seg == ".." || (seg == "" && i != len(segs)-1) {
			return bad()
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

// List calls fn with every key that begins with prefix, sorted. The keys are
// taken before the first call, so fn may write to the store or delete from
// it; what it writes is not listed.
func (s *MemoryStore) List(ctx context.Context, prefix string, fn func(key string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkPrefix(prefix); err != nil {
		return err
	}
	s.mu.RLock()
	var keys []string
	for k := range s.m {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

// ListDir leans on the order of List: the keys under one name are together
// in it, so a name is new when it is not the one before.
func (s *MemoryStore) ListDir(ctx context.Context, prefix string, fn func(name string) error) error {
	last, first := "", true
	return s.List(ctx, prefix, func(key string) error {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			return nil
		}
		name := dirName(rest)
		if !first && name == last {
			return nil
		}
		first, last = false, name
		return fn(name)
	})
}

// Keys is every key held, sorted. List is the same keys through the Store
// interface.
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
//
// A Set writes to a file beside the key's and renames it into place, so List
// and ListDir pass over every name beginning with a dot: a key whose last
// segment begins with one is written and read but never listed. Nothing this
// package writes is named that way. Deleting a key removes its file and not
// the directories above it, so the directories of a node that was deleted
// stay behind, empty; List does not yield one, a directory not being a key.
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

// walk is the directory a prefix names, that directory as a key path, and
// what of the first level of names in it the rest of the prefix keeps.
func (s *DirStore) walk(prefix string) (root, dir, base string, err error) {
	if err := checkPrefix(prefix); err != nil {
		return "", "", "", err
	}
	dir, base = "", prefix
	if i := strings.LastIndexByte(prefix, '/'); i >= 0 {
		dir, base = prefix[:i], prefix[i+1:]
	}
	return filepath.Join(s.root, filepath.FromSlash(dir)), dir, base, nil
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

// List walks the directory the prefix names, passing over the names the
// DirStore itself says it does. A prefix with nothing under it lists nothing
// and is not an error.
func (s *DirStore) List(ctx context.Context, prefix string, fn func(key string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, _, base, err := s.walk(prefix)
	if err != nil {
		return err
	}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// The prefix names nothing, or what was being walked went
			// while it was: either way there are no keys there.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if p == root {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") || filepath.Dir(p) == root && !strings.HasPrefix(d.Name(), base) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if checkKey(key) != nil {
			// A file no Set could have written: it is not a key, so it is
			// not listed. The error says why it is skipped, not that the
			// walk failed.
			return nil //nolint:nilerr
		}
		if err := fn(key); err != nil {
			return listStop{err}
		}
		return nil
	})
	var stop listStop
	if errors.As(err, &stop) {
		return stop.err
	}
	return err
}

// ListDir reads the prefix's directory rather than walking it. A directory
// a delete left empty is a name here, as the file system keeps it, where a
// store in S3 would have none to give.
func (s *DirStore) ListDir(ctx context.Context, prefix string, fn func(name string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, dir, base, err := s.walk(prefix)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") || !strings.HasPrefix(name, base) {
			continue
		}
		if checkKey(join(dir, name)) != nil {
			continue
		}
		rest := name[len(base):]
		if e.IsDir() {
			rest += "/"
		}
		if err := fn(rest); err != nil {
			return err
		}
	}
	return nil
}

var (
	_ Store       = (*MemoryStore)(nil)
	_ RangeGetter = (*MemoryStore)(nil)
	_ DirLister   = (*MemoryStore)(nil)
	_ Store       = (*DirStore)(nil)
	_ RangeGetter = (*DirStore)(nil)
	_ DirLister   = (*DirStore)(nil)
)
