package zarr

import (
	"bytes"
	"math"
	"slices"
	"testing"
)

func TestBytesCodecOrder(t *testing.T) {
	for _, c := range []struct {
		endian Endian
		d      DataType
		elems  any
		data   []byte
	}{
		{Little, Uint16, []uint16{0x0102, 0xa0b0}, []byte{2, 1, 0xb0, 0xa0}},
		{Big, Uint16, []uint16{0x0102, 0xa0b0}, []byte{1, 2, 0xa0, 0xb0}},
		{Little, Int32, []int32{-2}, []byte{0xfe, 0xff, 0xff, 0xff}},
		{Big, Int32, []int32{-2}, []byte{0xff, 0xff, 0xff, 0xfe}},
		{Little, Uint64, []uint64{0x0102030405060708}, []byte{8, 7, 6, 5, 4, 3, 2, 1}},
		{Big, Uint64, []uint64{0x0102030405060708}, []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		{Big, Float32, []float32{1}, []byte{0x3f, 0x80, 0, 0}},
		{Little, Float64, []float64{math.Inf(-1)}, []byte{0, 0, 0, 0, 0, 0, 0xf0, 0xff}},
		{Big, Int8, []int8{-1, 2}, []byte{0xff, 2}},
		{"", Uint8, []uint8{7, 8}, []byte{7, 8}},
		{Big, Bool, []bool{true, false}, []byte{1, 0}},
	} {
		codec := BytesCodec{Endian: c.endian}
		spec := ChunkSpec{Shape: []int{len(c.data) / c.d.Size()}, DataType: c.d}
		if got := must(codec.EncodeArray(c.elems, spec)); !bytes.Equal(got, c.data) {
			t.Errorf("%s %s: encoded %v to %x, not %x", c.endian, c.d, c.elems, got, c.data)
		}
		got := must(codec.DecodeArray(c.data, spec))
		if back := must(codec.EncodeArray(got, spec)); !bytes.Equal(back, c.data) {
			t.Errorf("%s %s: decoded %x to %v, not %v", c.endian, c.d, c.data, got, c.elems)
		}
	}
}

func TestBytesCodecBool(t *testing.T) {
	// Any byte but 0 is true, as encoding/binary has it.
	got := must(BytesCodec{}.DecodeArray([]byte{0, 1, 2, 0xff}, ChunkSpec{Shape: []int{4}, DataType: Bool}))
	if want := []bool{false, true, true, true}; !slices.Equal(got.([]bool), want) {
		t.Fatalf("decoded to %v, not %v", got, want)
	}
}

func TestCRC32CLeavesInput(t *testing.T) {
	buf := []byte("zarr....")
	out := must(CRC32CCodec{}.EncodeBytes(buf[:4]))
	if string(buf) != "zarr...." {
		t.Fatalf("the bytes past the input became %q", buf[4:])
	}
	if len(out) != 8 || cap(out) != 8 || string(out[:4]) != "zarr" {
		t.Fatalf("encoded to %q, of capacity %d", out, cap(out))
	}
}
