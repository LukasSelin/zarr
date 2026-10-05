package zarr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Zarr version 2 keeps a node's metadata in a .zarray or a .zgroup, and its
// attributes in a .zattrs beside it. This package reads both and writes
// neither: a version 2 array opens as the ArrayMetadata version 3 would give
// it, with ZarrFormat 2, and every write to one is refused with ErrZarrV2.

// zarray is the .zarray of a version 2 array.
type zarray struct {
	ZarrFormat         int               `json:"zarr_format"`
	Shape              []int             `json:"shape"`
	Chunks             []int             `json:"chunks"`
	DType              json.RawMessage   `json:"dtype"`
	FillValue          json.RawMessage   `json:"fill_value"`
	Order              string            `json:"order"`
	Compressor         json.RawMessage   `json:"compressor"`
	Filters            []json.RawMessage `json:"filters"`
	DimensionSeparator string            `json:"dimension_separator"`
}

// dimensionsAttribute is where xarray keeps the names of a version 2 array's
// dimensions, which version 2 has no place for.
const dimensionsAttribute = "_ARRAY_DIMENSIONS"

// readV2 reads the version 2 metadata of a node of type node at path, which
// has no zarr.json - the .zarray of an array or the .zgroup of a group - and
// its attributes. notFound is the error of the zarr.json not being there,
// which is the error if there is no node in either version.
func readV2(ctx context.Context, s Store, path, node string, notFound error) ([]byte, map[string]json.RawMessage, error) {
	key, other, is, isNot := ".zarray", ".zgroup", "a group", "an array"
	if node == "group" {
		key, other, is, isNot = other, key, isNot, is
	}
	b, err := s.Get(ctx, join(path, key))
	if errors.Is(err, ErrNotFound) {
		// What the node is, if it is one, to say so.
		_, err := s.Get(ctx, join(path, other))
		switch {
		case err == nil:
			return nil, nil, fmt.Errorf("zarr: %q is %s of Zarr version 2, not %s", path, is, isNot)
		case errors.Is(err, ErrNotFound):
			return nil, nil, notFound
		}
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, err
	}
	attrs, err := readAttributesV2(ctx, s, path)
	return b, attrs, err
}

// readAttributesV2 reads the .zattrs at path, which is no attributes if it
// is not there.
func readAttributesV2(ctx context.Context, s Store, path string) (map[string]json.RawMessage, error) {
	key := join(path, ".zattrs")
	b, err := s.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var attrs map[string]json.RawMessage
	if err := json.Unmarshal(b, &attrs); err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", key, err)
	}
	return attrs, nil
}

func openArrayV2(ctx context.Context, s Store, path string, notFound error) (*Array, error) {
	b, attrs, err := readV2(ctx, s, path, "array", notFound)
	if err != nil {
		return nil, err
	}
	return newArrayV2(s, path, b, attrs)
}

func openGroupV2(ctx context.Context, s Store, path string, notFound error) (*Group, error) {
	b, attrs, err := readV2(ctx, s, path, "group", notFound)
	if err != nil {
		return nil, err
	}
	var head struct {
		ZarrFormat int `json:"zarr_format"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return nil, fmt.Errorf("zarr: %s: %w", join(path, ".zgroup"), err)
	}
	if head.ZarrFormat != 2 {
		return nil, fmt.Errorf("%w: %q has a .zgroup of zarr_format %d", ErrUnsupported, path, head.ZarrFormat)
	}
	return &Group{store: s, path: path, meta: GroupMetadata{ZarrFormat: 2, NodeType: "group", Attributes: attrs}}, nil
}

// newArrayV2 is the array at path whose .zarray is b and attributes attrs.
func newArrayV2(s Store, path string, b []byte, attrs map[string]json.RawMessage) (*Array, error) {
	m, p, err := parseZarray(b, path)
	if err != nil {
		return nil, err
	}
	m.Attributes = attrs
	var names []string
	if ok, err := attribute(attrs, dimensionsAttribute, &names); ok && err == nil && len(names) == len(m.Shape) {
		for _, n := range names {
			m.DimensionNames = append(m.DimensionNames, &n)
		}
	}
	return makeArray(s, path, m, &p)
}

// parseZarray is the .zarray b as the metadata version 3 would have, with
// ZarrFormat 2, and the pipeline its chunks are decoded through. It leaves
// to makeArray what the two versions share: a shape and chunks that fit.
func parseZarray(b []byte, path string) (ArrayMetadata, pipeline, error) {
	var z zarray
	bad := func(format string, args ...any) error {
		return fmt.Errorf("zarr: array %q: %s", path, fmt.Sprintf(format, args...))
	}
	if err := json.Unmarshal(b, &z); err != nil {
		return ArrayMetadata{}, pipeline{}, bad(".zarray: %v", err)
	}
	if z.ZarrFormat != 2 {
		return ArrayMetadata{}, pipeline{}, fmt.Errorf("%w: array %q has a .zarray of zarr_format %d", ErrUnsupported, path, z.ZarrFormat)
	}
	if z.Shape == nil || z.Chunks == nil {
		return ArrayMetadata{}, pipeline{}, bad(".zarray has no shape or no chunks")
	}
	d, endian, err := parseTypestr(z.DType)
	if err != nil {
		return ArrayMetadata{}, pipeline{}, fmt.Errorf("%w: array %q: %w", ErrUnsupported, path, err)
	}
	fill, err := parseFillV2(d, z.FillValue)
	if err != nil {
		return ArrayMetadata{}, pipeline{}, bad("%v", err)
	}
	sep := z.DimensionSeparator
	switch sep {
	case "":
		sep = "."
	case ".", "/":
	default:
		return ArrayMetadata{}, pipeline{}, bad("dimension separator %q", sep)
	}
	var array ArrayBytesCodec = BytesCodec{Endian: endian}
	var transpose []Named
	switch z.Order {
	case "", "C":
	case "F":
		array = fortranBytes{BytesCodec{Endian: endian}}
		order := make([]int, len(z.Shape))
		for k := range order {
			order[k] = len(order) - 1 - k
		}
		cfg, _ := json.Marshal(map[string][]int{"order": order})
		transpose = []Named{{Name: "transpose", Configuration: cfg}}
	default:
		return ArrayMetadata{}, pipeline{}, bad("order %q, not C or F", z.Order)
	}
	cs := []Codec{array}
	for _, f := range z.Filters {
		c, err := parseCodecV2(f, d, path, "filter")
		if err != nil {
			return ArrayMetadata{}, pipeline{}, err
		}
		cs = append(cs, c)
	}
	if c := bytes.TrimSpace(z.Compressor); len(c) > 0 && string(c) != "null" {
		c, err := parseCodecV2(c, d, path, "compressor")
		if err != nil {
			return ArrayMetadata{}, pipeline{}, err
		}
		cs = append(cs, c)
	}
	p, err := pipelineOf(cs)
	if err != nil {
		return ArrayMetadata{}, pipeline{}, bad("%v", err)
	}
	named, err := namedCodecs(cs)
	if err != nil {
		return ArrayMetadata{}, pipeline{}, err
	}
	grid, _ := json.Marshal(map[string][]int{"chunk_shape": z.Chunks})
	return ArrayMetadata{
		ZarrFormat:       2,
		NodeType:         "array",
		Shape:            z.Shape,
		DataType:         d,
		ChunkGrid:        Named{Name: "regular", Configuration: grid},
		ChunkKeyEncoding: keyEncoding{v2: true, sep: sep}.named(),
		FillValue:        formatFill(fill),
		Codecs:           append(transpose, named...),
	}, p, nil
}

// parseTypestr is the data type and byte order of a numpy type string, such
// as "<f4": a byte order, a kind and a size in bytes. A type a byte wide has
// no byte order, and is written with "|".
func parseTypestr(raw json.RawMessage) (DataType, Endian, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", "", fmt.Errorf("dtype %s", raw)
	}
	types := map[string]DataType{
		"b1": Bool, "i1": Int8, "i2": Int16, "i4": Int32, "i8": Int64,
		"u1": Uint8, "u2": Uint16, "u4": Uint32, "u8": Uint64, "f4": Float32, "f8": Float64,
	}
	if len(s) < 2 {
		return "", "", fmt.Errorf("dtype %q", s)
	}
	d, ok := types[s[1:]]
	if !ok {
		return "", "", fmt.Errorf("dtype %q", s)
	}
	switch {
	case d.Size() == 1 && (s[0] == '|' || s[0] == '<' || s[0] == '>'):
		return d, "", nil
	case s[0] == '<':
		return d, Little, nil
	case s[0] == '>':
		return d, Big, nil
	}
	return "", "", fmt.Errorf("dtype %q", s)
}

// parseFillV2 is a version 2 fill_value: as version 3 has it, but null is
// zero, and an integer may be written as a float that is one.
func parseFillV2(d DataType, raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return fillFrom(d, nil)
	}
	v, err := parseFill(d, raw)
	if err == nil || d == Bool || d.float() || raw[0] == '"' {
		return v, err
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		if v, err := fillFrom(d, f); err == nil {
			return v, nil
		}
	}
	return nil, err
}

// parseCodecV2 is a version 2 compressor or filter, the object raw: the
// codec registered as "numcodecs." and its id, given the rest of the object
// as its configuration. zstd is the codec registered as "zstd" where there is
// none of that name, as importing github.com/LukasSelin/zarr/zstd makes it.
func parseCodecV2(raw json.RawMessage, d DataType, path, what string) (Codec, error) {
	var cfg map[string]json.RawMessage
	var id string
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg == nil || json.Unmarshal(cfg["id"], &id) != nil || id == "" {
		return nil, fmt.Errorf("zarr: array %q: %s %s is not an object with an id", path, what, raw)
	}
	delete(cfg, "id")
	name := "numcodecs." + id
	codecsMu.RLock()
	parse, ok := codecs[name]
	if !ok && id == "zstd" {
		parse, ok = codecs["zstd"]
		if _, set := cfg["checksum"]; !set {
			cfg["checksum"] = json.RawMessage("false")
		}
	}
	codecsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: array %q: %s %q, which no codec registered as %q reads", ErrUnsupported, path, what, id, name)
	}
	rest, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	c, err := parse(rest, d)
	if err != nil {
		return nil, fmt.Errorf("zarr: array %q: %s %q: %w", path, what, id, err)
	}
	if _, ok := c.(BytesBytesCodec); !ok {
		return nil, fmt.Errorf("%w: array %q: %s %q is not bytes to bytes", ErrUnsupported, path, what, id)
	}
	return c, nil
}

// v2Child is the type of the version 2 node at path - "array", "group", or
// "" if there is none - and its .zarray or .zgroup.
func v2Child(ctx context.Context, s Store, path string) (string, []byte, error) {
	for _, c := range [...]struct{ key, node string }{{".zarray", "array"}, {".zgroup", "group"}} {
		b, err := s.Get(ctx, join(path, c.key))
		if err == nil {
			if b == nil {
				b = []byte{}
			}
			return c.node, b, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return "", nil, err
		}
	}
	return "", nil, nil
}

// readOnly refuses a write to a node of Zarr version 2.
func readOnly(format int, kind, path string) error {
	if format == 2 {
		return fmt.Errorf("%w: %s %q is read and not written: this package writes version 3 alone", ErrZarrV2, kind, path)
	}
	return nil
}

// fortranBytes is the bytes codec of a version 2 array in Fortran order,
// whose chunks are laid out with the first dimension fastest: what version 3
// writes as a transpose before the bytes codec.
type fortranBytes struct{ BytesCodec }

func (c fortranBytes) EncodeArray(chunk any, spec ChunkSpec) ([]byte, error) {
	b, err := c.BytesCodec.EncodeArray(chunk, spec)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(b))
	reorder(out, b, spec.Shape, spec.DataType.Size(), false)
	return out, nil
}

func (c fortranBytes) DecodeArray(data []byte, spec ChunkSpec) (any, error) {
	size, err := storedBytes(spec.Shape, spec.DataType)
	if err != nil || int64(len(data)) != size {
		// Which the bytes codec refuses.
		return c.BytesCodec.DecodeArray(data, spec)
	}
	b := make([]byte, len(data))
	reorder(b, data, spec.Shape, spec.DataType.Size(), true)
	return c.BytesCodec.DecodeArray(b, spec)
}

// reorder copies the elements of src, an array of shape whose elements are
// size bytes, to dst: from Fortran order to C order if toC, and from C order
// to Fortran order if not.
func reorder(dst, src []byte, shape []int, size int, toC bool) {
	if len(shape) < 2 || product(shape) == 0 {
		copy(dst, src)
		return
	}
	// The stride of each dimension in Fortran order, in elements.
	stride := make([]int, len(shape))
	for k, n := 0, 1; k < len(shape); k++ {
		stride[k], n = n, n*shape[k]
	}
	hi := make([]int, len(shape))
	for k := range hi {
		hi[k] = shape[k] - 1
	}
	c := 0
	_ = eachIndex(make([]int, len(shape)), hi, func(idx []int) error {
		f := 0
		for k, i := range idx {
			f += i * stride[k]
		}
		if toC {
			copy(dst[c*size:(c+1)*size], src[f*size:(f+1)*size])
		} else {
			copy(dst[f*size:(f+1)*size], src[c*size:(c+1)*size])
		}
		c++
		return nil
	})
}
