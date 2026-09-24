package zarr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Named is an extension point in metadata - a codec, a chunk grid, a chunk
// key encoding - as a name and its configuration. It is read from either an
// object or a bare name.
type Named struct {
	Name           string
	Configuration  json.RawMessage
	MustUnderstand *bool
}

type namedJSON struct {
	Name           string          `json:"name"`
	Configuration  json.RawMessage `json:"configuration,omitempty"`
	MustUnderstand *bool           `json:"must_understand,omitempty"`
}

func (n Named) MarshalJSON() ([]byte, error) {
	return json.Marshal(namedJSON(n))
}

func (n *Named) UnmarshalJSON(b []byte) error {
	if b = bytes.TrimSpace(b); len(b) > 0 && b[0] == '"' {
		*n = Named{}
		return json.Unmarshal(b, &n.Name)
	}
	var j namedJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*n = Named(j)
	return nil
}

// ArrayMetadata is the zarr.json of an array.
type ArrayMetadata struct {
	ZarrFormat          int                        `json:"zarr_format"`
	NodeType            string                     `json:"node_type"`
	Shape               []int                      `json:"shape"`
	DataType            DataType                   `json:"data_type"`
	ChunkGrid           Named                      `json:"chunk_grid"`
	ChunkKeyEncoding    Named                      `json:"chunk_key_encoding"`
	FillValue           json.RawMessage            `json:"fill_value"`
	Codecs              []Named                    `json:"codecs"`
	Attributes          map[string]json.RawMessage `json:"attributes,omitempty"`
	StorageTransformers []Named                    `json:"storage_transformers,omitempty"`
	DimensionNames      []*string                  `json:"dimension_names,omitempty"`
}

// GroupMetadata is the zarr.json of a group.
type GroupMetadata struct {
	ZarrFormat int                        `json:"zarr_format"`
	NodeType   string                     `json:"node_type"`
	Attributes map[string]json.RawMessage `json:"attributes,omitempty"`
}

var arrayKeys = map[string]bool{
	"zarr_format": true, "node_type": true, "shape": true, "data_type": true,
	"chunk_grid": true, "chunk_key_encoding": true, "fill_value": true, "codecs": true,
	"attributes": true, "storage_transformers": true, "dimension_names": true,
}

var groupKeys = map[string]bool{"zarr_format": true, "node_type": true, "attributes": true}

// checkPath refuses node paths the specification does not allow. The root
// is "".
func checkPath(path string) error {
	if path == "" {
		return nil
	}
	if strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") {
		return fmt.Errorf("zarr: bad path %q", path)
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.HasPrefix(seg, "__") || strings.ContainsAny(seg, "\\:") {
			return fmt.Errorf("zarr: bad path %q", path)
		}
	}
	return nil
}

func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "/" + name
}

func metadataKey(path string) string { return join(path, "zarr.json") }

// readMetadata reads the zarr.json at path into v, refusing a version other
// than 3, a node of another type, and any field not in known unless it says
// it need not be understood.
func readMetadata(ctx context.Context, s Store, path, node string, known map[string]bool, v any) error {
	if err := checkPath(path); err != nil {
		return err
	}
	b, err := s.Get(ctx, metadataKey(path))
	if errors.Is(err, ErrNotFound) {
		if key := v2Metadata(ctx, s, path); key != "" {
			return fmt.Errorf("%w: %q is a Zarr version 2 node, with %s and no zarr.json; only version 3 is supported", ErrZarrV2, path, key)
		}
	}
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return fmt.Errorf("zarr: metadata of %q: %w", path, err)
	}
	var head struct {
		ZarrFormat int    `json:"zarr_format"`
		NodeType   string `json:"node_type"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return fmt.Errorf("zarr: metadata of %q: %w", path, err)
	}
	if head.ZarrFormat != 3 {
		return fmt.Errorf("%w: %q is zarr_format %d", ErrUnsupported, path, head.ZarrFormat)
	}
	if head.NodeType != node {
		return fmt.Errorf("zarr: %q is a %q, not an %s", path, head.NodeType, node)
	}
	for k, raw := range fields {
		if known[k] {
			continue
		}
		var ext struct {
			MustUnderstand *bool `json:"must_understand"`
		}
		if json.Unmarshal(raw, &ext) != nil || ext.MustUnderstand == nil || *ext.MustUnderstand {
			return fmt.Errorf("%w: %q has field %q", ErrUnsupported, path, k)
		}
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("zarr: metadata of %q: %w", path, err)
	}
	return nil
}

// v2Metadata is the key of the Zarr version 2 metadata at path, or "" if
// there is none, or it could not be read to tell.
func v2Metadata(ctx context.Context, s Store, path string) string {
	for _, name := range []string{".zarray", ".zgroup", ".zattrs"} {
		if _, err := s.Get(ctx, join(path, name)); err == nil {
			return join(path, name)
		}
	}
	return ""
}

func writeMetadata(ctx context.Context, s Store, path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return s.Set(ctx, metadataKey(path), b)
}

// mustBeNew fails if a node is already at path.
func mustBeNew(ctx context.Context, s Store, path string) error {
	if err := checkPath(path); err != nil {
		return err
	}
	_, err := s.Get(ctx, metadataKey(path))
	switch {
	case err == nil:
		return fmt.Errorf("%w: %q", ErrExists, path)
	case errors.Is(err, ErrNotFound):
		return nil
	}
	return err
}

// attribute decodes the attribute name into v, and says whether there was one.
func attribute(attrs map[string]json.RawMessage, name string, v any) (bool, error) {
	raw, ok := attrs[name]
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(raw, v)
}

// withAttributes is attrs with set laid over it.
func withAttributes(attrs map[string]json.RawMessage, set map[string]any) (map[string]json.RawMessage, error) {
	out := make(map[string]json.RawMessage, len(attrs)+len(set))
	for k, v := range attrs {
		out[k] = v
	}
	for k, v := range set {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("zarr: attribute %q: %w", k, err)
		}
		out[k] = b
	}
	return out, nil
}
