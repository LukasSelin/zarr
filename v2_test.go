package zarr

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// testdata/v2/store.zarr is what zarr-python 3.4.0 writes with
// zarr_format=2: a group with attributes, an array in it, and a group in it
// holding another array.
func TestAZarrV2NodeIsRefusedAsVersion2(t *testing.T) {
	s := NewDirStore(filepath.Join("testdata", "v2", "store.zarr"))
	for _, c := range []struct {
		path, key string
		open      func(path string) error
	}{
		{"", ".zgroup", func(p string) error { _, err := OpenGroup(ctx, s, p); return err }},
		{"temperature", "temperature/.zarray", func(p string) error { _, err := OpenArray(ctx, s, p); return err }},
		{"forecast", "forecast/.zgroup", func(p string) error { _, err := OpenGroup(ctx, s, p); return err }},
		{"forecast/rain", "forecast/rain/.zarray", func(p string) error { _, err := OpenArray(ctx, s, p); return err }},
		// The wrong kind of node is still a version 2 one.
		{"temperature", "temperature/.zarray", func(p string) error { _, err := OpenGroup(ctx, s, p); return err }},
	} {
		err := c.open(c.path)
		if !errors.Is(err, ErrZarrV2) || !errors.Is(err, ErrUnsupported) {
			t.Errorf("%q: %v, not ErrZarrV2", c.path, err)
			continue
		}
		if errors.Is(err, ErrNotFound) {
			t.Errorf("%q: %v is ErrNotFound, but there is a node there", c.path, err)
		}
		if msg := err.Error(); !strings.Contains(msg, "version 2") || !strings.Contains(msg, "only version 3") || !strings.Contains(msg, c.key) {
			t.Errorf("%q: %q does not say it is version 2, or where", c.path, msg)
		}
	}
}

func TestZarrV2IsFoundByAnyOfItsMetadata(t *testing.T) {
	for _, key := range []string{".zarray", ".zgroup", ".zattrs"} {
		s := NewMemoryStore()
		if err := s.Set(ctx, "a/"+key, []byte("{}")); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenArray(ctx, s, "a"); !errors.Is(err, ErrZarrV2) {
			t.Errorf("%s: %v", key, err)
		}
	}
}

func TestANodeThatIsNotThereIsStillNotFound(t *testing.T) {
	s := NewMemoryStore()
	// Version 2 metadata beside the path, or under it, is not the node's.
	for _, key := range []string{"ab/.zarray", "a/b/.zgroup", ".zgroup"} {
		if err := s.Set(ctx, key, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	_, err := OpenArray(ctx, s, "a")
	if !errors.Is(err, ErrNotFound) || errors.Is(err, ErrZarrV2) {
		t.Errorf("%v, not ErrNotFound alone", err)
	}
}
