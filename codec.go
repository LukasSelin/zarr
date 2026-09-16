package zarr

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

// Codec is one step of the pipeline a chunk is encoded through. A codec
// turns a chunk's elements into bytes (ArrayBytesCodec) or bytes into other
// bytes (BytesBytesCodec). An array has exactly one of the first, followed
// by any number of the second.
type Codec interface {
	Name() string
	// Configuration is what metadata keeps for the codec, marshalled to
	// JSON, or nil for none.
	Configuration() any
}

// ArrayBytesCodec turns a chunk's elements - a []T in an any, T being the
// array's data type, in C order - into bytes and back.
type ArrayBytesCodec interface {
	Codec
	EncodeArray(chunk any, spec ChunkSpec) ([]byte, error)
	// DecodeArray returns the elements of a chunk of spec's shape, as a []T
	// in an any.
	DecodeArray(data []byte, spec ChunkSpec) (any, error)
}

// ChunkSpec is what a codec is told of the chunk it encodes or decodes.
type ChunkSpec struct {
	Shape    []int
	DataType DataType
	// Fill is the array's fill value, of the Go type of DataType.
	Fill any
	// WriteEmptyChunks is the array's: whether a codec that holds several
	// chunks keeps those that are nothing but the fill value.
	WriteEmptyChunks bool
}

// BytesBytesCodec turns bytes into bytes: a compressor, a checksum.
type BytesBytesCodec interface {
	Codec
	EncodeBytes(data []byte) ([]byte, error)
	DecodeBytes(data []byte) ([]byte, error)
}

// CodecParser makes a codec from its configuration in metadata, which is
// empty when there is none, for an array of the data type.
type CodecParser func(configuration json.RawMessage, d DataType) (Codec, error)

var (
	codecsMu sync.RWMutex
	codecs   = map[string]CodecParser{}
)

// RegisterCodec makes the codec called name available to every array,
// replacing any codec of that name. Every codec an array uses must be
// registered, the built-in ones included, which are registered from the
// start.
func RegisterCodec(name string, parse CodecParser) {
	codecsMu.Lock()
	defer codecsMu.Unlock()
	codecs[name] = parse
}

func init() {
	RegisterCodec("bytes", parseBytes)
	RegisterCodec("gzip", parseGzip)
	RegisterCodec("sharding_indexed", parseSharding)
	RegisterCodec("crc32c", func(json.RawMessage, DataType) (Codec, error) { return CRC32CCodec{}, nil })
}

func parseCodec(n Named, d DataType) (Codec, error) {
	codecsMu.RLock()
	parse, ok := codecs[n.Name]
	codecsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: codec %q", ErrUnsupported, n.Name)
	}
	return parse(n.Configuration, d)
}

func namedCodec(c Codec) (Named, error) {
	n := Named{Name: c.Name()}
	if cfg := c.Configuration(); cfg != nil {
		b, err := json.Marshal(cfg)
		if err != nil {
			return n, fmt.Errorf("zarr: codec %q: %w", c.Name(), err)
		}
		n.Configuration = b
	}
	return n, nil
}

// pipeline is an array's codecs in the order a chunk is encoded through.
type pipeline struct {
	array ArrayBytesCodec
	bytes []BytesBytesCodec
}

func newPipeline(named []Named, d DataType) (pipeline, error) {
	cs := make([]Codec, len(named))
	for i, n := range named {
		c, err := parseCodec(n, d)
		if err != nil {
			return pipeline{}, err
		}
		cs[i] = c
	}
	return pipelineOf(cs)
}

func pipelineOf(cs []Codec) (pipeline, error) {
	var p pipeline
	for i, c := range cs {
		if i == 0 {
			ab, ok := c.(ArrayBytesCodec)
			if !ok {
				return p, fmt.Errorf("%w: codec %q first: only array-to-bytes codecs may begin a pipeline here", ErrUnsupported, c.Name())
			}
			p.array = ab
			continue
		}
		bb, ok := c.(BytesBytesCodec)
		if !ok {
			return p, fmt.Errorf("zarr: codec %q after the array-to-bytes codec must be bytes-to-bytes", c.Name())
		}
		p.bytes = append(p.bytes, bb)
	}
	if p.array == nil {
		return p, fmt.Errorf("zarr: a pipeline needs an array-to-bytes codec")
	}
	return p, nil
}

// codecs is the pipeline as the list it was made from.
func (p pipeline) codecs() []Codec {
	cs := []Codec{p.array}
	for _, c := range p.bytes {
		cs = append(cs, c)
	}
	return cs
}

func namedCodecs(cs []Codec) ([]Named, error) {
	named := make([]Named, len(cs))
	for i, c := range cs {
		n, err := namedCodec(c)
		if err != nil {
			return nil, err
		}
		named[i] = n
	}
	return named, nil
}

func (p pipeline) encode(chunk any, spec ChunkSpec) ([]byte, error) {
	b, err := p.array.EncodeArray(chunk, spec)
	for _, c := range p.bytes {
		if err != nil {
			break
		}
		b, err = c.EncodeBytes(b)
	}
	return b, err
}

func (p pipeline) decode(b []byte, spec ChunkSpec) (any, error) {
	var err error
	for i := len(p.bytes) - 1; i >= 0; i-- {
		if l, ok := p.bytes[i].(LimitedBytesDecoder); ok {
			b, err = l.DecodeBytesLimit(b, p.decodeLimit(spec, i))
		} else {
			b, err = p.bytes[i].DecodeBytes(b)
		}
		if err != nil {
			return nil, err
		}
	}
	return p.array.DecodeArray(b, spec)
}

// decodeLimit is the most bytes the i-th bytes codec may decode a chunk of
// spec to: what the codecs before it encode such a chunk to at most, or as
// much as a chunk may be if that cannot be said.
func (p pipeline) decodeLimit(spec ChunkSpec, i int) int64 {
	n := encodedBound(p.array, spec)
	for _, c := range p.bytes[:i] {
		if n == unbounded || n > maxStoredBytes*2 {
			break
		}
		n = bytesBound(c, n)
	}
	if n == unbounded || n > maxStoredBytes*2 {
		return maxStoredBytes
	}
	return n
}

// LimitedBytesDecoder is a bytes-to-bytes codec that can be told the most
// bytes it may decode to, and fails rather than decode to more. An array
// tells it what the codecs before it encode a chunk to at most; a
// decompressor from a store that is not trusted should be one.
type LimitedBytesDecoder interface {
	DecodeBytesLimit(data []byte, limit int64) ([]byte, error)
}

// BoundedBytesEncoder is a bytes-to-bytes codec that can say the most bytes
// it encodes n bytes to, or a negative number if it cannot. A codec that is
// not one is taken to encode to any number, which lets a limited decoder
// after it inflate a chunk as far as a chunk may be.
type BoundedBytesEncoder interface {
	EncodedBound(n int64) int64
}

// Endian is the byte order of the bytes codec.
type Endian string

const (
	Little Endian = "little"
	Big    Endian = "big"
)

// BytesCodec lays elements end to end in a byte order. Endian may be left
// empty only for types a byte wide.
type BytesCodec struct {
	Endian Endian
}

func parseBytes(cfg json.RawMessage, d DataType) (Codec, error) {
	var c BytesCodec
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &c); err != nil {
			return nil, fmt.Errorf("zarr: bytes codec: %w", err)
		}
	}
	if _, err := c.order(d); err != nil {
		return nil, err
	}
	return c, nil
}

func (BytesCodec) Name() string { return "bytes" }

func (c BytesCodec) Configuration() any {
	if c.Endian == "" {
		return nil
	}
	return map[string]Endian{"endian": c.Endian}
}

func (c *BytesCodec) UnmarshalJSON(b []byte) error {
	var j struct {
		Endian Endian `json:"endian"`
	}
	err := json.Unmarshal(b, &j)
	c.Endian = j.Endian
	return err
}

func (c BytesCodec) order(d DataType) (binary.ByteOrder, error) {
	switch {
	case c.Endian == Big:
		return binary.BigEndian, nil
	case c.Endian == Little, c.Endian == "" && d.Size() == 1:
		return binary.LittleEndian, nil
	case c.Endian == "":
		return nil, fmt.Errorf("zarr: bytes codec needs an endian for %s", d)
	}
	return nil, fmt.Errorf("zarr: bytes codec: no such endian %q", c.Endian)
}

func (c BytesCodec) EncodeArray(chunk any, spec ChunkSpec) ([]byte, error) {
	order, err := c.order(spec.DataType)
	if err != nil {
		return nil, err
	}
	return binary.Append(nil, order, chunk)
}

func (c BytesCodec) DecodeArray(data []byte, spec ChunkSpec) (any, error) {
	d := spec.DataType
	order, err := c.order(d)
	if err != nil {
		return nil, err
	}
	size, err := storedBytes(spec.Shape, d)
	if err != nil {
		return nil, err
	}
	n := int(size) / d.Size()
	if int64(len(data)) != size {
		return nil, fmt.Errorf("zarr: chunk is %d bytes, not the %d of %d %s", len(data), n*d.Size(), n, d)
	}
	s := makeSlice(d, n)
	if _, err := binary.Decode(data, order, s); err != nil {
		return nil, err
	}
	return s, nil
}

// GzipCodec compresses with gzip at a level from 0 to 9.
type GzipCodec struct {
	Level int
}

func parseGzip(cfg json.RawMessage, _ DataType) (Codec, error) {
	var j struct {
		Level *int `json:"level"`
	}
	if err := json.Unmarshal(cfg, &j); err != nil || j.Level == nil || *j.Level < 0 || *j.Level > 9 {
		return nil, fmt.Errorf("zarr: gzip codec needs a level from 0 to 9: %s", cfg)
	}
	return GzipCodec{Level: *j.Level}, nil
}

func (GzipCodec) Name() string         { return "gzip" }
func (c GzipCodec) Configuration() any { return map[string]int{"level": c.Level} }

// gzipWriters keeps writers of each level from gzip.HuffmanOnly to
// gzip.BestCompression, and gzipReaders readers: a writer is most of a
// megabyte and a reader tens of kilobytes, and either reset does what a new
// one does, byte for byte.
var (
	gzipWriters [gzip.BestCompression - gzip.HuffmanOnly + 1]sync.Pool
	gzipReaders sync.Pool
)

func (c GzipCodec) EncodeBytes(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	var w *gzip.Writer
	var pool *sync.Pool
	if c.Level >= gzip.HuffmanOnly && c.Level <= gzip.BestCompression {
		pool = &gzipWriters[c.Level-gzip.HuffmanOnly]
		if w, _ = pool.Get().(*gzip.Writer); w != nil {
			w.Reset(&buf)
		}
	}
	if w == nil {
		var err error
		if w, err = gzip.NewWriterLevel(&buf, c.Level); err != nil {
			return nil, err
		}
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	if pool != nil {
		w.Reset(io.Discard)
		pool.Put(w)
	}
	return buf.Bytes(), nil
}

// DecodeBytes inflates data, to no more bytes than a chunk may be. An array
// holds it to the bytes its chunks encode to.
func (c GzipCodec) DecodeBytes(data []byte) ([]byte, error) {
	return c.DecodeBytesLimit(data, maxStoredBytes)
}

func (GzipCodec) DecodeBytesLimit(data []byte, limit int64) ([]byte, error) {
	r, _ := gzipReaders.Get().(*gzip.Reader)
	var err error
	if r != nil {
		err = r.Reset(bytes.NewReader(data))
	} else {
		r, err = gzip.NewReader(bytes.NewReader(data))
	}
	if r != nil {
		defer gzipReaders.Put(r)
	}
	if err != nil {
		return nil, err
	}
	// A gzip trailer ends with the length its member inflates to, which for
	// a chunk of one member is the chunk's. It is only a hint: never more
	// than the limit, nor than deflate can inflate data to.
	size := min(limit, 1032*int64(len(data))+64)
	if len(data) >= 4 {
		size = min(size, int64(binary.LittleEndian.Uint32(data[len(data)-4:])))
	}
	out := make([]byte, 0, size)
	for {
		if room := min(int64(cap(out)), limit); int64(len(out)) >= room {
			// Full: whether there is more, a byte of it.
			var one [1]byte
			if _, err := io.ReadFull(r, one[:]); err == io.EOF {
				return out, nil
			} else if err != nil {
				return nil, err
			}
			if int64(len(out)) >= limit {
				return nil, fmt.Errorf("zarr: gzip: a chunk inflates past the %d bytes it may be", limit)
			}
			out = append(out, one[0])
			continue
		}
		n, err := r.Read(out[len(out):min(int64(cap(out)), limit)])
		out = out[:len(out)+n]
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// CRC32CCodec appends a CRC-32C checksum, little-endian, and checks it when
// read.
type CRC32CCodec struct{}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func (CRC32CCodec) Name() string       { return "crc32c" }
func (CRC32CCodec) Configuration() any { return nil }

func (CRC32CCodec) EncodeBytes(data []byte) ([]byte, error) {
	return binary.LittleEndian.AppendUint32(append([]byte(nil), data...), crc32.Checksum(data, castagnoli)), nil
}

func (CRC32CCodec) DecodeBytes(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("zarr: crc32c: chunk of %d bytes has no checksum", len(data))
	}
	body, sum := data[:len(data)-4], binary.LittleEndian.Uint32(data[len(data)-4:])
	if got := crc32.Checksum(body, castagnoli); got != sum {
		return nil, fmt.Errorf("zarr: crc32c: checksum %08x, chunk says %08x", got, sum)
	}
	return body, nil
}
