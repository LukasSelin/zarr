package zarr

import (
	"encoding/json"
	"fmt"
)

// ShuffleCodec lays bytes out by their place in an element rather than by
// element: every first byte, then every second, and so on. It compresses
// nothing itself and goes before a compressor, which then sees runs of bytes
// that change slowly - the high bytes of a smooth field - rather than
// elements that differ in every byte.
//
// ElementSize is how many bytes an element takes, which for the codec to be
// worth anything must be the array's data type size: a bytes-to-bytes codec
// is handed nothing but bytes, so it cannot be told.
//
//	zarr.ShuffleCodec{ElementSize: zarr.Float32.Size()}
//
// An element size of 0 or 1, or of more bytes than there are, leaves the
// bytes where they are, so the zero value is a codec that metadata names and
// that does nothing. Bytes past the last whole element - which a codec
// before it can leave, a checksum say - are kept where they are too, so that
// any bytes encode and decode again. numcodecs refuses those bytes rather
// than move them, and a chunk of whole elements has none, so this only takes
// what it would not.
//
// This is numcodecs' shuffle, which metadata names "numcodecs.shuffle" and
// zarr-python writes as zarr.codecs.numcodecs.Shuffle. The version 3 core
// specification has no shuffle of its own.
type ShuffleCodec struct {
	ElementSize int
}

func parseShuffle(cfg json.RawMessage, _ DataType) (Codec, error) {
	var j struct {
		ElementSize *int `json:"elementsize"`
	}
	if err := json.Unmarshal(cfg, &j); err != nil || j.ElementSize == nil || *j.ElementSize < 0 {
		return nil, fmt.Errorf("zarr: shuffle codec needs an element size of 0 or more: %s", cfg)
	}
	return ShuffleCodec{ElementSize: *j.ElementSize}, nil
}

func (ShuffleCodec) Name() string         { return "numcodecs.shuffle" }
func (c ShuffleCodec) Configuration() any { return map[string]int{"elementsize": c.ElementSize} }

// EncodedBound is what shuffling encodes n bytes to: n, whatever the element
// size, since it moves bytes and adds none.
func (ShuffleCodec) EncodedBound(n int64) int64 { return n }

// count is how many whole elements n bytes hold, and 0 for an element size
// that shuffles nothing: none, one byte, or more bytes than there are. Every
// loop over the element size is past that 0, which is what keeps an element
// size out of metadata from being looped over a billion times.
func (c ShuffleCodec) count(n int) (int, error) {
	if c.ElementSize < 0 {
		return 0, fmt.Errorf("zarr: shuffle codec: an element size of %d is not 0 or more", c.ElementSize)
	}
	if c.ElementSize <= 1 {
		return 0, nil
	}
	return n / c.ElementSize, nil
}

func (c ShuffleCodec) EncodeBytes(data []byte) ([]byte, error) {
	count, err := c.count(len(data))
	if err != nil || count == 0 {
		return append([]byte(nil), data...), err
	}
	out := make([]byte, len(data))
	for i := range c.ElementSize {
		to, from := out[i*count:(i+1)*count], data[i:]
		for j := range to {
			to[j] = from[j*c.ElementSize]
		}
	}
	copy(out[count*c.ElementSize:], data[count*c.ElementSize:])
	return out, nil
}

// DecodeBytes lays the bytes of each element back beside each other. An
// array holds it to the bytes its chunks encode to, which for this codec is
// the bytes it is given.
func (c ShuffleCodec) DecodeBytes(data []byte) ([]byte, error) {
	count, err := c.count(len(data))
	if err != nil || count == 0 {
		return append([]byte(nil), data...), err
	}
	out := make([]byte, len(data))
	for i := range c.ElementSize {
		from, to := data[i*count:(i+1)*count], out[i:]
		for j := range from {
			to[j*c.ElementSize] = from[j]
		}
	}
	copy(out[count*c.ElementSize:], data[count*c.ElementSize:])
	return out, nil
}

func (c ShuffleCodec) DecodeBytesLimit(data []byte, limit int64) ([]byte, error) {
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("zarr: shuffle: a chunk of %d bytes is past the %d bytes it may be", len(data), limit)
	}
	return c.DecodeBytes(data)
}
