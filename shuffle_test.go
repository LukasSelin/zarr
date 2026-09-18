package zarr

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func TestShuffleMovesBytesAndMovesThemBack(t *testing.T) {
	// Two four-byte elements: every first byte, then every second, and so on.
	if got := must(ShuffleCodec{ElementSize: 4}.EncodeBytes([]byte{0, 1, 2, 3, 4, 5, 6, 7})); !bytes.Equal(got, []byte{0, 4, 1, 5, 2, 6, 3, 7}) {
		t.Errorf("shuffled to % x", got)
	}
	// And two bytes past the last whole element, which stay where they are.
	if got := must(ShuffleCodec{ElementSize: 4}.EncodeBytes([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})); !bytes.Equal(got, []byte{0, 4, 1, 5, 2, 6, 3, 7, 8, 9}) {
		t.Errorf("shuffled to % x", got)
	}

	r := rand.New(rand.NewPCG(1, 2))
	for _, size := range []int{0, 1, 2, 3, 4, 8, 12, 4096} {
		c := ShuffleCodec{ElementSize: size}
		for _, n := range []int{0, 1, 3, 7, 8, 12, 64, 196, 4095} {
			data := make([]byte, n)
			for i := range data {
				data[i] = byte(r.Uint32())
			}
			enc := must(c.EncodeBytes(data))
			if len(enc) != n || c.EncodedBound(int64(n)) != int64(n) {
				t.Fatalf("%+v: %d bytes encoded to %d, bound %d", c, n, len(enc), c.EncodedBound(int64(n)))
			}
			if size <= 1 && !bytes.Equal(enc, data) {
				t.Fatalf("%+v: %d bytes did not pass through", c, n)
			}
			if back := must(c.DecodeBytesLimit(enc, int64(n))); !bytes.Equal(back, data) {
				t.Fatalf("%+v: %d bytes read back as % x, not % x", c, n, back, data)
			}
			// Decoding first is the other way round, and encodes back too.
			if back := must(c.EncodeBytes(must(c.DecodeBytes(data)))); !bytes.Equal(back, data) {
				t.Fatalf("%+v: %d bytes unshuffled and shuffled to % x", c, n, back)
			}
			if n > 0 {
				if _, err := c.DecodeBytesLimit(enc, int64(n)-1); err == nil {
					t.Errorf("%+v: %d bytes decoded within a limit of one fewer", c, n)
				}
			}
		}
	}
}

func TestShuffleConfigurations(t *testing.T) {
	for _, cfg := range []string{``, `{}`, `null`, `{"elementsize": -1}`, `{"elementsize": "4"}`,
		`{"elementsize": 1.5}`, `{"elementsize": null}`} {
		if c, err := parseShuffle(json.RawMessage(cfg), Float64); err == nil {
			t.Errorf("configuration %q parsed as %+v", cfg, c)
		}
	}
	// What a codec writes is what parsing it reads: an array created with one
	// opens again as the same.
	for _, size := range []int{0, 1, 4, 8} {
		c := ShuffleCodec{ElementSize: size}
		cfg := must(json.Marshal(c.Configuration()))
		back, err := parseShuffle(cfg, Float64)
		if err != nil || back != Codec(c) {
			t.Errorf("%+v wrote %s, which parsed as %+v: %v", c, cfg, back, err)
		}
	}
	s := NewMemoryStore()
	s.Set(ctx, "zarr.json", arrayJSON("[4]", "[2]",
		`[{"name": "bytes", "configuration": {"endian": "little"}}, {"name": "numcodecs.shuffle"}]`))
	if _, err := OpenArray(ctx, s, ""); err == nil {
		t.Error("an array whose shuffle has no element size opened")
	}
}

func TestShuffleArrays(t *testing.T) {
	data := make([]float64, 9*7)
	for i := range data {
		data[i] = float64(i%11) * 0.5
	}
	for name, codecs := range map[string][]Codec{
		"chunks": {BytesCodec{Endian: Little}, ShuffleCodec{ElementSize: 8}, GzipCodec{Level: 5}},
		// A chunk of 4 by 6 float64 is 192 bytes, and 196 with a checksum: the
		// four bytes past the last whole element go through the shuffle too.
		"after a checksum": {BytesCodec{Endian: Little}, CRC32CCodec{}, ShuffleCodec{ElementSize: 8}},
		"within shards": {&ShardingCodec{ChunkShape: []int{2, 3},
			Codecs: []Codec{BytesCodec{Endian: Big}, ShuffleCodec{ElementSize: 8}, GzipCodec{Level: 1}}}},
		"after shards": {&ShardingCodec{ChunkShape: []int{2, 3}}, ShuffleCodec{ElementSize: 8}},
	} {
		t.Run(name, func(t *testing.T) {
			s := NewMemoryStore()
			a := mustArray(t, s, "a", ArrayOptions{Shape: []int{9, 7}, ChunkShape: []int{4, 6},
				DataType: Float64, FillValue: 0.5, Codecs: codecs})
			if err := Write(ctx, a, nil, nil, data); err != nil {
				t.Fatal(err)
			}
			meta := string(must(s.Get(ctx, "a/zarr.json")))
			if !strings.Contains(meta, `"numcodecs.shuffle"`) || !strings.Contains(meta, `"elementsize": 8`) {
				t.Errorf("metadata has no shuffle:\n%s", meta)
			}
			b := mustOpen(t, s, "a")
			if got, want := must(json.Marshal(b.Metadata().Codecs)), must(json.Marshal(a.Metadata().Codecs)); !bytes.Equal(got, want) {
				t.Errorf("codecs opened as %s, not %s", got, want)
			}
			if got, err := Read[float64](ctx, b, nil, nil); err != nil || !slices.Equal(got, data) {
				t.Fatalf("read %v: %v", got, err)
			}
		})
	}
}
