// Package zstd is the Zarr zstd codec, with the compressor of
// github.com/klauspost/compress/zstd, which is Go without cgo.
//
// It is a module of its own so that package zarr keeps to the standard
// library. Importing it registers the codec with every array, so that arrays
// whose metadata names zstd open:
//
//	import _ "github.com/LukasSelin/zarr/zstd"
//
// and Codec is what an array is created with:
//
//	Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zstd.Codec{Level: 3}},
//
// A chunk read from a store is held to what package zarr holds a gzip chunk
// to: it may not decompress past the bytes its elements take.
package zstd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/LukasSelin/zarr"
	"github.com/klauspost/compress/zstd"
)

// MinLevel and MaxLevel are the levels zstd has. Level 0 is zstd's default,
// 3.
const (
	MinLevel = -(1 << 17)
	MaxLevel = 22
)

// maxDecoded is the most bytes a chunk decompresses to when there is nothing
// to say it may be fewer: package zarr's most for a chunk.
const maxDecoded = 1 << 31

func init() {
	zarr.RegisterCodec("zstd", parse)
}

// Codec compresses with zstd at a level from MinLevel to MaxLevel, and with
// Checksum puts a checksum of the content in each frame, which is checked
// when read.
//
// klauspost/compress has four levels of its own; a zstd level is taken to
// the nearest, so that 1 and 2 compress as each other, and 10 and 22. What
// it writes any zstd reads.
type Codec struct {
	Level    int
	Checksum bool
}

func parse(cfg json.RawMessage, _ zarr.DataType) (zarr.Codec, error) {
	var j struct {
		Level    *int  `json:"level"`
		Checksum *bool `json:"checksum"`
	}
	if err := json.Unmarshal(cfg, &j); err != nil || j.Level == nil || j.Checksum == nil {
		return nil, fmt.Errorf("zarr: zstd codec needs a level and a checksum: %s", cfg)
	}
	c := Codec{Level: *j.Level, Checksum: *j.Checksum}
	if err := c.check(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c Codec) check() error {
	if c.Level < MinLevel || c.Level > MaxLevel {
		return fmt.Errorf("zarr: zstd codec: level %d is not from %d to %d", c.Level, MinLevel, MaxLevel)
	}
	return nil
}

func (Codec) Name() string { return "zstd" }

func (c Codec) Configuration() any {
	return map[string]any{"level": c.Level, "checksum": c.Checksum}
}

// encoders keeps an encoder for each of klauspost's levels, with and
// without checksums. An encoder may encode from several goroutines at once.
var encoders [4][2]struct {
	once sync.Once
	enc  *zstd.Encoder
	err  error
}

func (c Codec) encoder() (*zstd.Encoder, error) {
	level := c.Level
	if level == 0 {
		level = 3
	}
	l := zstd.EncoderLevelFromZstd(level)
	crc := 0
	if c.Checksum {
		crc = 1
	}
	e := &encoders[l-zstd.SpeedFastest][crc]
	e.once.Do(func() {
		e.enc, e.err = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(l),
			zstd.WithEncoderCRC(c.Checksum),
			// An empty chunk is a frame too, as zstd itself writes it.
			zstd.WithZeroFrames(true),
		)
	})
	return e.enc, e.err
}

func (c Codec) EncodeBytes(data []byte) ([]byte, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	e, err := c.encoder()
	if err != nil {
		return nil, err
	}
	return e.EncodeAll(data, nil), nil
}

// EncodedBound is zstd's bound on what n bytes compress to, with a frame's
// header and checksum and room to spare.
func (Codec) EncodedBound(n int64) int64 {
	return n + n>>8 + 1024
}

// DecodeBytes decompresses data, to no more bytes than a chunk may be. An
// array holds it to the bytes its chunks encode to.
func (c Codec) DecodeBytes(data []byte) ([]byte, error) {
	return c.DecodeBytesLimit(data, maxDecoded)
}

// decoders keeps decoders, each used by one goroutine at a time.
var decoders sync.Pool

// window is the least window a chunk may ask for however small it is: the
// most the zstd command asks for at its default level with no size given.
const window = 8 << 20

func (Codec) DecodeBytesLimit(data []byte, limit int64) ([]byte, error) {
	limit = max(0, min(limit, maxDecoded))
	d, _ := decoders.Get().(*zstd.Decoder)
	if d == nil {
		var err error
		if d, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true)); err != nil {
			return nil, err
		}
	}
	defer func() {
		// Reset to nil lets go of the reader before the decoder is pooled;
		// there is nothing to be done about it failing.
		_ = d.Reset(nil)
		decoders.Put(d)
	}()
	// Decoded a block at a time, as a stream, so that the limit is kept to
	// within a block: klauspost's DecodeAll keeps the limits a decoder was
	// made with, not those it is reset with. A frame's window is held to a
	// power of two past the limit, which is what zstd asks for when it knows
	// what it compresses.
	err := d.ResetWithOptions(bytes.NewReader(data),
		zstd.WithDecoderMaxMemory(maxDecoded),
		zstd.WithDecoderMaxWindow(uint64(min(max(2*limit, window), maxDecoded))))
	if err != nil {
		return nil, fmt.Errorf("zarr: zstd: %w", err)
	}
	// A frame's header may say how many bytes it decompresses to. It is only
	// a hint: never more than the limit.
	size := min(limit, 64<<10)
	var h zstd.Header
	if h.Decode(data) == nil && h.HasFCS {
		if h.FrameContentSize > uint64(limit) {
			return nil, tooBig(limit)
		}
		size = int64(h.FrameContentSize)
	}
	out := make([]byte, 0, size)
	for {
		if int64(len(out)) >= limit {
			// Full: whether there is more, a byte of it.
			var one [1]byte
			if _, err := io.ReadFull(d, one[:]); err == io.EOF {
				return out, nil
			} else if err != nil {
				return nil, decodeErr(err, limit)
			}
			return nil, tooBig(limit)
		}
		if len(out) == cap(out) {
			out = slices.Grow(out, int(min(max(int64(cap(out)), 4<<10), limit-int64(len(out)))))
		}
		n, err := d.Read(out[len(out):min(int64(cap(out)), limit)])
		out = out[:len(out)+n]
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, decodeErr(err, limit)
		}
	}
}

func decodeErr(err error, limit int64) error {
	if errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
		return tooBig(limit)
	}
	return fmt.Errorf("zarr: zstd: %w", err)
}

func tooBig(limit int64) error {
	return fmt.Errorf("zarr: zstd: a chunk decompresses past the %d bytes it may be", limit)
}
