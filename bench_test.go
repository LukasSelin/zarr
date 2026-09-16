package zarr

import (
	"math"
	"testing"
)

// The benchmarks are on a 512 by 1024 float64 array in chunks of 64,
// compressed with gzip at 5, the shape and codecs of a world's field, as a
// whole array and as shards of 4 by 4 chunks.

var benchShape = []int{512, 1024}

func benchOptions(sharded bool) ArrayOptions {
	o := ArrayOptions{
		Shape:      benchShape,
		ChunkShape: []int{64, 64},
		DataType:   Float64,
		Codecs:     []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 5}},
	}
	if sharded {
		o.ShardShape = []int{256, 256}
	}
	return o
}

// benchData is a smooth field with some noise in it, which gzip neither
// crushes to nothing nor leaves alone.
func benchData() []float64 {
	data := make([]float64, product(benchShape))
	for y := range benchShape[0] {
		for x := range benchShape[1] {
			i := y*benchShape[1] + x
			data[i] = 1000*math.Sin(float64(x)/97)*math.Cos(float64(y)/61) + float64(i*2654435761%1000)/100
		}
	}
	return data
}

func benchArray(b *testing.B, s Store, sharded bool) *Array {
	b.Helper()
	a, err := CreateArray(ctx, s, "height", benchOptions(sharded))
	if err != nil {
		b.Fatal(err)
	}
	if err := Write(ctx, a, nil, nil, benchData()); err != nil {
		b.Fatal(err)
	}
	return a
}

func eachSharding(b *testing.B, f func(b *testing.B, sharded bool)) {
	b.Run("chunks", func(b *testing.B) { f(b, false) })
	b.Run("shards", func(b *testing.B) { f(b, true) })
}

func BenchmarkWrite(b *testing.B) {
	eachSharding(b, func(b *testing.B, sharded bool) {
		data := benchData()
		b.SetBytes(int64(8 * len(data)))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			s := NewMemoryStore()
			a, err := CreateArray(ctx, s, "height", benchOptions(sharded))
			if err != nil {
				b.Fatal(err)
			}
			if err := Write(ctx, a, nil, nil, data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRead(b *testing.B) {
	eachSharding(b, func(b *testing.B, sharded bool) {
		a := benchArray(b, NewMemoryStore(), sharded)
		b.SetBytes(int64(8 * product(benchShape)))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := Read[float64](ctx, a, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkReadRegion reads 3 by 3 elements across the corner of four chunks
// in the middle of a shard, from a sharded array in a directory: the read of
// a map's neighbourhood.
func BenchmarkReadRegion(b *testing.B) {
	s := NewDirStore(b.TempDir())
	benchArray(b, s, true)
	a, err := OpenArray(ctx, s, "height")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := Read[float64](ctx, a, []int{127, 383}, []int{3, 3}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadChunk(b *testing.B) {
	eachSharding(b, func(b *testing.B, sharded bool) {
		a := benchArray(b, NewMemoryStore(), sharded)
		b.SetBytes(8 * 64 * 64)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ReadChunk[float64](ctx, a, []int{3, 7}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkWriteChunk(b *testing.B) {
	eachSharding(b, func(b *testing.B, sharded bool) {
		a := benchArray(b, NewMemoryStore(), sharded)
		chunk, err := ReadChunk[float64](ctx, a, []int{3, 7})
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(8 * 64 * 64)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := WriteChunk(ctx, a, []int{3, 7}, chunk); err != nil {
				b.Fatal(err)
			}
		}
	})
}
