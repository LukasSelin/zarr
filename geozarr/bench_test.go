package geozarr

import (
	"context"
	"fmt"
	"testing"

	"github.com/LukasSelin/zarr"
)

// The benchmarks are of the paths the package is for: writing a 4096 by
// 4096 float32 raster whole, by Ingest and by Write of row strips as a
// reader of a GeoTIFF would; reading it at points, cold and warm; and
// reading one unit's year of days out of a hundred thousand units.

const benchSide = 4096

func benchStores(b *testing.B) map[string]func() zarr.Store {
	return map[string]func() zarr.Store{
		"memory": func() zarr.Store { return zarr.NewMemoryStore() },
		"dir":    func() zarr.Store { return zarr.NewDirStore(b.TempDir()) },
	}
}

func benchRaster(b *testing.B, s zarr.Store, shard int) *Raster {
	b.Helper()
	g, err := zarr.CreateGroup(context.Background(), s, "", nil)
	if err != nil {
		b.Fatal(err)
	}
	r, err := CreateRaster(context.Background(), g, "dem", RasterOptions{
		Width: benchSide, Height: benchSide, DataType: zarr.Float32, CRS: utm, Transform: utmGrid,
		ChunkSize: 256, ShardSize: shard,
		Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.GzipCodec{Level: 1}},
	})
	if err != nil {
		b.Fatal(err)
	}
	return r
}

func dem(row, col int) float32 { return float32(row%97*13 + col%89*7) }

func BenchmarkIngest(b *testing.B) {
	ctx := context.Background()
	for name, store := range benchStores(b) {
		for _, shard := range []int{0, 1024} {
			b.Run(fmt.Sprintf("%s/shard=%d/Ingest", name, shard), func(b *testing.B) {
				b.SetBytes(benchSide * benchSide * 4)
				for range b.N {
					r := benchRaster(b, store(), shard)
					if err := Ingest(ctx, r, func(blk Block, dst []float32) error {
						for i := range dst {
							dst[i] = dem(blk.Row+i/blk.Width, blk.Col+i%blk.Width)
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
			// Strips of 64 rows, each cutting across every chunk of a row of
			// them, written one after another.
			b.Run(fmt.Sprintf("%s/shard=%d/strips", name, shard), func(b *testing.B) {
				b.SetBytes(benchSide * benchSide * 4)
				const rows = 64
				buf := make([]float32, rows*benchSide)
				for range b.N {
					r := benchRaster(b, store(), shard)
					for row0 := 0; row0 < benchSide; row0 += rows {
						for i := range buf {
							buf[i] = dem(row0+i/benchSide, i%benchSide)
						}
						if err := zarr.Write(ctx, r.Array, []int{row0, 0}, []int{rows, benchSide}, buf); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkSampler(b *testing.B) {
	ctx := context.Background()
	for _, shard := range []int{0, 1024} {
		r := benchRaster(b, zarr.NewMemoryStore(), shard)
		if err := Ingest(ctx, r, func(blk Block, dst []float32) error {
			for i := range dst {
				dst[i] = dem(blk.Row+i/blk.Width, blk.Col+i%blk.Width)
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		at := func(i int) (float64, float64) {
			return utmGrid.Apply(float64(i*7919%benchSide)+0.5, float64(i*104729%benchSide)+0.5)
		}
		b.Run(fmt.Sprintf("shard=%d/cold", shard), func(b *testing.B) {
			for i := range b.N {
				s, _ := NewSampler[float32](r, 1)
				x, y := at(i)
				if _, _, err := s.At(ctx, nil, x, y); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("shard=%d/warm", shard), func(b *testing.B) {
			s, _ := NewSampler[float32](r, 512)
			for i := range 4096 {
				x, y := at(i)
				_, _, _ = s.At(ctx, nil, x, y)
			}
			b.ResetTimer()
			for i := range b.N {
				x, y := at(i)
				if _, _, err := s.At(ctx, nil, x, y); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("shard=%d/batch1000", shard), func(b *testing.B) {
			xs, ys := make([]float64, 1000), make([]float64, 1000)
			for i := range xs {
				xs[i], ys[i] = at(i)
			}
			for range b.N {
				s, _ := NewSampler[float32](r, 512)
				if _, _, err := s.Points(ctx, nil, xs, ys); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadUnit(b *testing.B) {
	ctx := context.Background()
	const n, days = 100_000, 365
	g, err := zarr.CreateGroup(ctx, zarr.NewMemoryStore(), "", nil)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i * 3)
	}
	u, err := CreateUnits(ctx, g, "regions", UnitsOptions{IDs: ids})
	if err != nil {
		b.Fatal(err)
	}
	v, err := u.CreateVariable(ctx, "rain", VariableOptions{
		Leading: []Dim{{Name: "time", Size: days}}, DataType: zarr.Float32, UnitChunk: 64, UnitShard: 4096,
		Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.GzipCodec{Level: 1}},
	})
	if err != nil {
		b.Fatal(err)
	}
	all := make([]float32, n*days)
	for i := range all {
		all[i] = float32(i % 1013)
	}
	if err := zarr.Write(ctx, v.Array, nil, nil, all); err != nil {
		b.Fatal(err)
	}
	b.Run("one", func(b *testing.B) {
		for i := range b.N {
			if _, err := ReadUnit[float32](ctx, v, ids[i*7919%n]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("batch100", func(b *testing.B) {
		batch := make([]int64, 100)
		for i := range batch {
			batch[i] = ids[i*7919%n]
		}
		for range b.N {
			if _, err := ReadUnits[float32](ctx, v, batch); err != nil {
				b.Fatal(err)
			}
		}
	})
}
