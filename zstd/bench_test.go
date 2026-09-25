package zstd

import (
	"fmt"
	"math"
	"testing"

	"github.com/LukasSelin/zarr"
)

// The benchmarks are the core's raster benchmarks (bench_raster_test.go
// there) with zstd in gzip's place: a 2048 by 2048 float32 DEM in chunks
// of 512, read and written whole, and one chunk through the codec.

const side = 2048

func dem() []float32 {
	f := make([]float32, side*side)
	seed := uint64(0x9e3779b97f4a7c15)
	for y := range side {
		for x := range side {
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			fx, fy := float64(x), float64(y)
			v := 2000 + 1200*math.Sin(fx/173)*math.Cos(fy/131) + 600*math.Sin((fx+fy)/59)
			f[y*side+x] = float32(v + float64(seed>>11)/(1<<53))
		}
	}
	return f
}

func pipelines() []struct {
	name   string
	codecs []zarr.Codec
} {
	return []struct {
		name   string
		codecs []zarr.Codec
	}{
		{"zstd1", []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, Codec{Level: 1}}},
		{"zstd3", []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, Codec{Level: 3}}},
		{"shuffle+zstd3", []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.ShuffleCodec{ElementSize: 4}, Codec{Level: 3}}},
	}
}

func BenchmarkRaster(b *testing.B) {
	data := dem()
	for _, p := range pipelines() {
		for _, conc := range []int{1, 0} {
			o := zarr.ArrayOptions{Shape: []int{side, side}, ChunkShape: []int{512, 512}, DataType: zarr.Float32, Codecs: p.codecs}
			b.Run(fmt.Sprintf("codec=%s/conc=%d/write", p.name, conc), func(b *testing.B) {
				a, err := zarr.CreateArray(ctx, zarr.NewMemoryStore(), "r", o)
				if err != nil {
					b.Fatal(err)
				}
				a.Concurrency = conc
				b.SetBytes(4 * side * side)
				b.ReportAllocs()
				for range b.N {
					if err := zarr.Write(ctx, a, nil, nil, data); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("codec=%s/conc=%d/read", p.name, conc), func(b *testing.B) {
				s := zarr.NewMemoryStore()
				a, err := zarr.CreateArray(ctx, s, "r", o)
				if err != nil {
					b.Fatal(err)
				}
				if err := zarr.Write(ctx, a, nil, nil, data); err != nil {
					b.Fatal(err)
				}
				a.Concurrency = conc
				var stored int
				if err := s.List(ctx, "r/c", func(k string) error {
					v, err := s.Get(ctx, k)
					stored += len(v)
					return err
				}); err != nil {
					b.Fatal(err)
				}
				b.SetBytes(4 * side * side)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := zarr.Read[float32](ctx, a, nil, nil); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(4*side*side)/float64(stored), "ratio")
			})
		}
	}
}

func BenchmarkCodec(b *testing.B) {
	data := dem()
	chunk := make([]float32, 0, 512*512)
	for r := range 512 {
		chunk = append(chunk, data[r*side:r*side+512]...)
	}
	raw, err := zarr.BytesCodec{Endian: zarr.Little}.EncodeArray(chunk, zarr.ChunkSpec{Shape: []int{512, 512}, DataType: zarr.Float32})
	if err != nil {
		b.Fatal(err)
	}
	for _, level := range []int{1, 3, 9} {
		c := Codec{Level: level}
		enc, err := c.EncodeBytes(raw)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("zstd%d/encode", level), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for range b.N {
				if _, err := c.EncodeBytes(raw); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(raw))/float64(len(enc)), "ratio")
		})
		b.Run(fmt.Sprintf("zstd%d/decode", level), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for range b.N {
				if _, err := c.DecodeBytes(enc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
