package zarr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
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
	if n.decode(b) {
		return nil
	}
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

// decode decodes b into n in one pass, as UnmarshalJSON does, and says
// whether it could. Like decodeMetadata, it leaves anything but plain JSON
// to encoding/json.
func (n *Named) decode(b []byte) bool {
	i := skipSpace(b, 0)
	if i < len(b) && b[i] == '"' {
		end := skipString(b, i)
		if end < 0 || skipSpace(b, end) != len(b) || !plainKey(b[i+1:end-1]) {
			return false
		}
		*n = Named{Name: string(b[i+1 : end-1])}
		return true
	}
	var j Named
	var name, cfg, must bool
	end := members(b, i, 1, func(key []byte, i int) int {
		end := skipValue(b, i, 2)
		if end < 0 || !plainKey(key) {
			return -1
		}
		raw := b[i:end]
		switch string(key) {
		case "name":
			if name || raw[0] != '"' || !plainKey(raw[1:len(raw)-1]) {
				return -1
			}
			name, j.Name = true, string(raw[1:len(raw)-1])
		case "configuration":
			if cfg {
				return -1
			}
			cfg, j.Configuration = true, bytes.Clone(raw)
		case "must_understand":
			if must || string(raw) != "true" && string(raw) != "false" {
				return -1
			}
			u := string(raw) == "true"
			must, j.MustUnderstand = true, &u
		default:
			for _, k := range [...]string{"name", "configuration", "must_understand"} {
				if strings.EqualFold(k, string(key)) {
					return -1
				}
			}
		}
		return end
	})
	if end < 0 || skipSpace(b, end) != len(b) {
		return false
	}
	*n = j
	return true
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
	return parseMetadata(b, path, node, known, v)
}

// parseMetadata is readMetadata of the metadata b, already read from path.
func parseMetadata(b []byte, path, node string, known map[string]bool, v any) error {
	if decodeMetadata(b, node, known, v) {
		return nil
	}
	return decodeMetadataSlowly(b, path, node, known, v)
}

// decodeMetadata decodes the zarr.json b into v in one pass, and says
// whether it could. It decodes only what is plain: an object of version 3
// and of node type node, its keys unescaped ASCII, each field known once or
// not needing to be understood, and no key that encoding/json would match to
// a field of v only by case. Anything else it leaves, to be refused or
// decoded by decodeMetadataSlowly, which is what defines the result: where
// this decodes, that would decode the same.
func decodeMetadata(b []byte, node string, known map[string]bool, v any) bool {
	// The attributes are kept as they are written, in one copy of b rather
	// than one each.
	b = bytes.Clone(b)
	var seen []string
	var format, typed bool
	i := skipSpace(b, 0)
	end := members(b, i, 0, func(key []byte, i int) int {
		if !plainKey(key) {
			return -1
		}
		k := string(key)
		if !known[k] {
			for f := range known {
				if strings.EqualFold(f, k) {
					return -1
				}
			}
			end := skipValue(b, i, 1)
			var ext struct {
				MustUnderstand *bool `json:"must_understand"`
			}
			if end < 0 || json.Unmarshal(b[i:end], &ext) != nil || ext.MustUnderstand == nil || *ext.MustUnderstand {
				return -1
			}
			return end
		}
		if slices.Contains(seen, k) {
			return -1
		}
		seen = append(seen, k)
		p := fieldOf(v, k)
		if attrs, ok := p.(*map[string]json.RawMessage); ok && i < len(b) && b[i] == '{' {
			return decodeAttributes(attrs, b, i)
		}
		end := skipValue(b, i, 1)
		if end < 0 {
			return -1
		}
		raw := b[i:end]
		switch k {
		case "zarr_format":
			format = string(raw) == "3"
			if !format {
				return -1
			}
		case "node_type":
			typed = len(raw) == len(node)+2 && raw[0] == '"' && string(raw[1:len(raw)-1]) == node
			if !typed {
				return -1
			}
		default:
			if !decodeField(p, raw) {
				return -1
			}
		}
		return end
	})
	if end < 0 || skipSpace(b, end) != len(b) || !format || !typed {
		return false
	}
	switch v := v.(type) {
	case *ArrayMetadata:
		v.ZarrFormat, v.NodeType = 3, node
	case *GroupMetadata:
		v.ZarrFormat, v.NodeType = 3, node
	default:
		return false
	}
	return true
}

// fieldOf is the field of v that the known key key decodes into.
func fieldOf(v any, key string) any {
	switch m := v.(type) {
	case *ArrayMetadata:
		switch key {
		case "shape":
			return &m.Shape
		case "data_type":
			return &m.DataType
		case "chunk_grid":
			return &m.ChunkGrid
		case "chunk_key_encoding":
			return &m.ChunkKeyEncoding
		case "fill_value":
			return &m.FillValue
		case "codecs":
			return &m.Codecs
		case "attributes":
			return &m.Attributes
		case "storage_transformers":
			return &m.StorageTransformers
		case "dimension_names":
			return &m.DimensionNames
		}
	case *GroupMetadata:
		if key == "attributes" {
			return &m.Attributes
		}
	}
	return nil
}

// decodeObject decodes the JSON object b into the fields field gives for
// keys, as encoding/json would into a struct of those fields, and says
// whether it could. Like decodeMetadata, it leaves anything but plain JSON to
// encoding/json, to decode into fields that start again from nothing.
func decodeObject(b []byte, keys []string, field func(key string) any) bool {
	var seen uint64
	end := members(b, skipSpace(b, 0), 1, func(key []byte, i int) int {
		end := skipValue(b, i, 2)
		if end < 0 || !plainKey(key) {
			return -1
		}
		for f, k := range keys {
			if k == string(key) {
				if seen&(1<<f) != 0 || !decodeField(field(k), b[i:end]) {
					return -1
				}
				seen |= 1 << f
				return end
			}
			if strings.EqualFold(k, string(key)) {
				return -1
			}
		}
		return end
	})
	return end >= 0 && skipSpace(b, end) == len(b)
}

// decodeField decodes the valid JSON raw into the field p as encoding/json
// would, and says whether it could.
func decodeField(p any, raw []byte) bool {
	switch p := p.(type) {
	case nil:
		return false
	case *DataType:
		if s, ok := plainString(raw); ok {
			*p = DataType(s)
			return true
		}
	case *Endian:
		if s, ok := plainString(raw); ok {
			*p = Endian(s)
			return true
		}
	case *IndexLocation:
		if s, ok := plainString(raw); ok {
			*p = IndexLocation(s)
			return true
		}
	case *json.RawMessage:
		*p = bytes.Clone(raw)
		return true
	case *Named:
		return p.decode(raw)
	case *[]Named:
		ns := []Named{}
		if items(raw, 0, 1, func(i int) int {
			end := skipValue(raw, i, 2)
			var n Named
			if end < 0 || !n.decode(raw[i:end]) {
				return -1
			}
			ns = append(ns, n)
			return end
		}) < 0 {
			break
		}
		*p = ns
		return true
	case *[]int:
		ns := []int{}
		if items(raw, 0, 1, func(i int) int {
			end := skipValue(raw, i, 2)
			digits := raw[i:max(end, i)]
			if len(digits) > 0 && digits[0] == '-' {
				digits = digits[1:]
			}
			// Eighteen digits are within an int64, and anything but digits
			// is left to encoding/json to refuse or round.
			if len(digits) == 0 || len(digits) > 18 || strconv.IntSize < 64 || bytes.ContainsFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) {
				return -1
			}
			n, err := strconv.Atoi(string(raw[i:end]))
			if err != nil {
				return -1
			}
			ns = append(ns, n)
			return end
		}) < 0 {
			break
		}
		*p = ns
		return true
	case *[]*string:
		ss := []*string{}
		if items(raw, 0, 1, func(i int) int {
			end := skipValue(raw, i, 2)
			if end < 0 {
				return -1
			}
			if string(raw[i:end]) == "null" {
				ss = append(ss, nil)
				return end
			}
			s, ok := plainString(raw[i:end])
			if !ok {
				return -1
			}
			ss = append(ss, &s)
			return end
		}) < 0 {
			break
		}
		*p = ss
		return true
	}
	return json.Unmarshal(raw, p) == nil
}

// decodeAttributes decodes the object at b[i] into attrs as encoding/json
// would, but without decoding the values, and is the end of the object, or
// -1 if it is not valid or has a key that is not plain. The values are
// slices of b.
func decodeAttributes(attrs *map[string]json.RawMessage, b []byte, i int) int {
	m := map[string]json.RawMessage{}
	end := members(b, i, 1, func(key []byte, i int) int {
		if !plainKey(key) {
			return -1
		}
		end := skipValue(b, i, 2)
		if end >= 0 {
			m[string(key)] = b[i:end:end]
		}
		return end
	})
	*attrs = m
	return end
}

// decodeMetadataSlowly is readMetadata's refusal of what decodeMetadata
// would not decode, and its decoding of what is valid but unusual.
func decodeMetadataSlowly(b []byte, path, node string, known map[string]bool, v any) error {
	switch v := v.(type) {
	case *ArrayMetadata:
		*v = ArrayMetadata{}
	case *GroupMetadata:
		*v = GroupMetadata{}
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
