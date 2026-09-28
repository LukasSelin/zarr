package zarr

import (
	"fmt"
	"math"
	"testing"
)

// The raster benchmarks are a 2048 by 2048 raster, a quarter of a
// Sentinel-2 tile at 10 m or a 2048 tile of a DEM, read and written whole,
// by window and by tile, across the data types, codecs, chunk shapes,
// shardings and stores a raster is kept in. Each reports MB/s of the
// decoded raster and Mcells/s.

const rasterSide = 2048

var rasterShape = []int{rasterSide, rasterSide}

// rasterField is the raster's values before they are a data type: a DEM of
// ridges and valleys from 0 to 4000 with a metre of noise in it, or radar
// backscatter, speckled, which compresses the least.
func rasterField(kind string) []float64 {
	f := make([]float64, rasterSide*rasterSide)
	seed := uint64(0x9e3779b97f4a7c15)
	next := func() float64 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return float64(seed>>11) / (1 << 53)
	}
	for y := range rasterSide {
		for x := range rasterSide {
			fx, fy := float64(x), float64(y)
			v := 2000 + 1200*math.Sin(fx/173)*math.Cos(fy/131) + 600*math.Sin((fx+fy)/59)
			switch kind {
			case "dem":
				f[y*rasterSide+x] = v + next()
			case "speckle":
				// Exponential speckle on a smooth backscatter, in linear power.
				f[y*rasterSide+x] = v / 4000 * -math.Log(1-next())
			}
		}
	}
	return f
}

// rasterOf is the field as T: rescaled to the type's range for the
// integers, as a reflectance or a DN is.
func rasterOf[T interface {
	Element
	~int16 | ~uint8 | ~uint16 | ~float32 | ~float64
}](f []float64) []T {
	out := make([]T, len(f))
	var scale float64
	switch any(out).(type) {
	case []uint8:
		scale = 255.0 / 4000
	case []int16:
		scale = 8
	case []uint16:
		scale = 16
	default:
		scale = 1
	}
	for i, v := range f {
		out[i] = T(v * scale)
	}
	return out
}

// rasterCase is one way of keeping the raster.
type rasterCase struct {
	name     string
	dtype    DataType
	kind     string // "dem" or "speckle"
	chunk    int
	shard    int // 0 is not sharded
	codecs   []Codec
	dirStore bool
}

func (c rasterCase) options() ArrayOptions {
	o := ArrayOptions{
		Shape:      rasterShape,
		ChunkShape: []int{c.chunk, c.chunk},
		DataType:   c.dtype,
		Codecs:     c.codecs,
	}
	if c.shard != 0 {
		o.ShardShape = []int{c.shard, c.shard}
	}
	return o
}

func bytesGzip(level int) []Codec {
	return []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: level}}
}

// rasterCases is the matrix: each group varies one thing from float32, a
// DEM, chunks of 512, gzip at 5, in memory.
func rasterCases() []rasterCase {
	base := rasterCase{dtype: Float32, kind: "dem", chunk: 512, codecs: bytesGzip(5)}
	with := func(name string, f func(c *rasterCase)) rasterCase {
		c := base
		c.name = name
		f(&c)
		return c
	}
	var cs []rasterCase
	for _, d := range []DataType{Float32, Float64, Uint16, Int16, Uint8} {
		cs = append(cs, with("dtype="+string(d), func(c *rasterCase) { c.dtype = d }))
	}
	cs = append(cs,
		with("codec=none", func(c *rasterCase) { c.codecs = nil }),
		with("codec=gzip1", func(c *rasterCase) { c.codecs = bytesGzip(1) }),
		with("codec=gzip5", func(c *rasterCase) {}),
		with("codec=gzip9", func(c *rasterCase) { c.codecs = bytesGzip(9) }),
		with("codec=shuffle+gzip1", func(c *rasterCase) {
			c.codecs = []Codec{BytesCodec{Endian: Little}, ShuffleCodec{ElementSize: 4}, GzipCodec{Level: 1}}
		}),
		with("codec=shuffle+gzip5", func(c *rasterCase) {
			c.codecs = []Codec{BytesCodec{Endian: Little}, ShuffleCodec{ElementSize: 4}, GzipCodec{Level: 5}}
		}),
		with("codec=crc32c", func(c *rasterCase) { c.codecs = []Codec{BytesCodec{Endian: Little}, CRC32CCodec{}} }),
		with("codec=bigendian", func(c *rasterCase) { c.codecs = []Codec{BytesCodec{Endian: Big}} }),
		with("data=speckle", func(c *rasterCase) { c.kind = "speckle" }),
		with("data=speckle/codec=shuffle+gzip5", func(c *rasterCase) {
			c.kind = "speckle"
			c.codecs = []Codec{BytesCodec{Endian: Little}, ShuffleCodec{ElementSize: 4}, GzipCodec{Level: 5}}
		}),
	)
	for _, n := range []int{128, 256, 1024} {
		cs = append(cs, with(fmt.Sprintf("chunk=%d", n), func(c *rasterCase) { c.chunk = n }))
	}
	cs = append(cs,
		with("shard=1024/chunk=256", func(c *rasterCase) { c.chunk, c.shard = 256, 1024 }),
		with("shard=1024/chunk=256/codec=none", func(c *rasterCase) { c.chunk, c.shard, c.codecs = 256, 1024, nil }),
		with("store=dir", func(c *rasterCase) { c.dirStore = true }),
		with("store=dir/codec=none", func(c *rasterCase) { c.dirStore, c.codecs = true, nil }),
		with("store=dir/shard=1024/chunk=256", func(c *rasterCase) { c.dirStore, c.chunk, c.shard = true, 256, 1024 }),
	)
	return cs
}

func (c rasterCase) store(b *testing.B) Store {
	if c.dirStore {
		return NewDirStore(b.TempDir())
	}
	return NewMemoryStore()
}

// eachRasterCase runs f for each case at the array's default concurrency and
// at one, which is the cost on one core.
func eachRasterCase(b *testing.B, f func(b *testing.B, c rasterCase, concurrency int)) {
	for _, c := range rasterCases() {
		for _, conc := range []int{1, 0} {
			name := "conc=1"
			if conc == 0 {
				name = "conc=default"
			}
			b.Run(c.name+"/"+name, func(b *testing.B) { f(b, c, conc) })
		}
	}
}

func reportCells(b *testing.B, cells int) {
	b.ReportMetric(float64(cells)*float64(b.N)/b.Elapsed().Seconds()/1e6, "Mcells/s")
}

// byType calls the f of the case's data type.
func byType(b *testing.B, c rasterCase, f32 func(), f64 func(), u16 func(), i16 func(), u8 func()) {
	switch c.dtype {
	case Float32:
		f32()
	case Float64:
		f64()
	case Uint16:
		u16()
	case Int16:
		i16()
	case Uint8:
		u8()
	default:
		b.Fatalf("no %s", c.dtype)
	}
}

func BenchmarkRasterWrite(b *testing.B) {
	eachRasterCase(b, func(b *testing.B, c rasterCase, conc int) {
		f := rasterField(c.kind)
		byType(b, c,
			func() { rasterWrite(b, c, conc, rasterOf[float32](f)) },
			func() { rasterWrite(b, c, conc, rasterOf[float64](f)) },
			func() { rasterWrite(b, c, conc, rasterOf[uint16](f)) },
			func() { rasterWrite(b, c, conc, rasterOf[int16](f)) },
			func() { rasterWrite(b, c, conc, rasterOf[uint8](f)) },
		)
	})
}

func rasterWrite[T Element](b *testing.B, c rasterCase, conc int, data []T) {
	s := c.store(b)
	a, err := CreateArray(ctx, s, "r", c.options())
	if err != nil {
		b.Fatal(err)
	}
	a.Concurrency = conc
	b.SetBytes(int64(len(data) * c.dtype.Size()))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := Write(ctx, a, nil, nil, data); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportCells(b, len(data))
	b.ReportMetric(storedRatio(b, s, len(data)*c.dtype.Size()), "ratio")
}

// storedRatio is how many times smaller the raster is stored than held.
func storedRatio(b *testing.B, s Store, raw int) float64 {
	var stored int
	err := s.List(ctx, "r/c", func(key string) error {
		v, err := s.Get(ctx, key)
		stored += len(v)
		return err
	})
	if err != nil {
		b.Fatal(err)
	}
	return float64(raw) / float64(stored)
}

func BenchmarkRasterRead(b *testing.B) {
	eachRasterCase(b, func(b *testing.B, c rasterCase, conc int) {
		f := rasterField(c.kind)
		byType(b, c,
			func() { rasterRead(b, c, conc, rasterOf[float32](f)) },
			func() { rasterRead(b, c, conc, rasterOf[float64](f)) },
			func() { rasterRead(b, c, conc, rasterOf[uint16](f)) },
			func() { rasterRead(b, c, conc, rasterOf[int16](f)) },
			func() { rasterRead(b, c, conc, rasterOf[uint8](f)) },
		)
	})
}

func rasterArray[T Element](b *testing.B, c rasterCase, data []T) *Array {
	b.Helper()
	s := c.store(b)
	if _, err := CreateArray(ctx, s, "r", c.options()); err != nil {
		b.Fatal(err)
	}
	a, err := OpenArray(ctx, s, "r")
	if err != nil {
		b.Fatal(err)
	}
	if err := Write(ctx, a, nil, nil, data); err != nil {
		b.Fatal(err)
	}
	return a
}

func rasterRead[T Element](b *testing.B, c rasterCase, conc int, data []T) {
	a := rasterArray(b, c, data)
	a.Concurrency = conc
	b.SetBytes(int64(len(data) * c.dtype.Size()))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := Read[T](ctx, a, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportCells(b, len(data))
}

// BenchmarkRasterWindow reads windows of the raster, float32 in chunks of
// 512 compressed with gzip, the way a map, a tile server or a focal
// operation does: one pixel, a 3 by 3 neighbourhood across the corner of
// four chunks, a 256 tile inside a chunk and across four, a row and a
// column, and a 1024 tile of 2 by 2 chunks. cells/op is what was asked for;
// decoded/op the cells decoded to get it.
func BenchmarkRasterWindow(b *testing.B) {
	windows := []struct {
		name         string
		start, shape []int
	}{
		{"pixel", []int{700, 700}, []int{1, 1}},
		{"3x3corner", []int{511, 511}, []int{3, 3}},
		{"256aligned", []int{512, 512}, []int{256, 256}},
		{"256across4", []int{384, 384}, []int{256, 256}},
		{"row", []int{1000, 0}, []int{1, rasterSide}},
		{"column", []int{0, 1000}, []int{rasterSide, 1}},
		{"1024aligned", []int{1024, 1024}, []int{1024, 1024}},
	}
	f := rasterOf[float32](rasterField("dem"))
	for _, c := range []rasterCase{
		{name: "chunk=512", dtype: Float32, kind: "dem", chunk: 512, codecs: bytesGzip(5)},
		{name: "chunk=256", dtype: Float32, kind: "dem", chunk: 256, codecs: bytesGzip(5)},
		{name: "shard=2048/chunk=256", dtype: Float32, kind: "dem", chunk: 256, shard: 2048, codecs: bytesGzip(5)},
		{name: "chunk=512/codec=none", dtype: Float32, kind: "dem", chunk: 512},
	} {
		b.Run(c.name, func(b *testing.B) {
			a := rasterArray(b, c, f)
			for _, w := range windows {
				b.Run(w.name, func(b *testing.B) {
					cells := product(w.shape)
					_, _, lo, hi, _, err := a.region(w.start, w.shape, a.chunks)
					if err != nil {
						b.Fatal(err)
					}
					decoded := spanLen(lo, hi) * product(a.chunks)
					b.SetBytes(int64(4 * cells))
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, err := Read[float32](ctx, a, w.start, w.shape); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(decoded), "decoded/op")
					b.ReportMetric(float64(decoded)/float64(cells), "amplification")
				})
			}
		})
	}
}

// BenchmarkRasterWriteTiles writes the raster a tile at a time, as a tiled
// engine hands its output over: tiles the size of a chunk, which replace
// it, and tiles of half a chunk, each of which reads its chunk, patches it
// and writes it back.
func BenchmarkRasterWriteTiles(b *testing.B) {
	f := rasterOf[float32](rasterField("dem"))
	for _, c := range []rasterCase{
		{name: "chunk=512", dtype: Float32, kind: "dem", chunk: 512, codecs: bytesGzip(5)},
		{name: "shard=1024/chunk=256", dtype: Float32, kind: "dem", chunk: 256, shard: 1024, codecs: bytesGzip(5)},
	} {
		for _, tile := range []int{512, 256} {
			b.Run(fmt.Sprintf("%s/tile=%d", c.name, tile), func(b *testing.B) {
				a, err := CreateArray(ctx, NewMemoryStore(), "r", c.options())
				if err != nil {
					b.Fatal(err)
				}
				tiles := make([][]float32, 0)
				for y := 0; y < rasterSide; y += tile {
					for x := 0; x < rasterSide; x += tile {
						t := make([]float32, 0, tile*tile)
						for r := range tile {
							t = append(t, f[(y+r)*rasterSide+x:(y+r)*rasterSide+x+tile]...)
						}
						tiles = append(tiles, t)
					}
				}
				per := rasterSide / tile
				b.SetBytes(int64(4 * len(f)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					for i, t := range tiles {
						if err := Write(ctx, a, []int{i / per * tile, i % per * tile}, []int{tile, tile}, t); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				reportCells(b, len(f))
			})
		}
	}
}

// BenchmarkRasterReadSparse reads a raster no chunk of which was written:
// every one of them is a miss in the store and the fill value.
func BenchmarkRasterReadSparse(b *testing.B) {
	for _, fill := range []any{float32(0), float32(math.NaN())} {
		b.Run(fmt.Sprintf("fill=%v", fill), func(b *testing.B) {
			a, err := CreateArray(ctx, NewMemoryStore(), "r", ArrayOptions{
				Shape: rasterShape, ChunkShape: []int{512, 512}, DataType: Float32, FillValue: fill, Codecs: bytesGzip(5),
			})
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(4 * rasterSide * rasterSide)
			b.ReportAllocs()
			for range b.N {
				if _, err := Read[float32](ctx, a, nil, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCodec is each codec on one chunk of 512 by 512 of the raster,
// the DEM as float32: where the time of a chunk read or written goes.
func BenchmarkCodec(b *testing.B) {
	f32 := rasterOf[float32](rasterField("dem"))
	chunk := make([]float32, 0, 512*512)
	for r := range 512 {
		chunk = append(chunk, f32[r*rasterSide:r*rasterSide+512]...)
	}
	spec := ChunkSpec{Shape: []int{512, 512}, DataType: Float32, Fill: float32(0)}
	raw, err := BytesCodec{Endian: Little}.EncodeArray(chunk, spec)
	if err != nil {
		b.Fatal(err)
	}
	for _, e := range []Endian{Little, Big} {
		c := BytesCodec{Endian: e}
		enc, err := c.EncodeArray(chunk, spec)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("bytes-%s/encode", e), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for range b.N {
				if _, err := c.EncodeArray(chunk, spec); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("bytes-%s/decode", e), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for range b.N {
				if _, err := c.DecodeArray(enc, spec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	for _, c := range []struct {
		name string
		c    BytesBytesCodec
	}{
		{"gzip1", GzipCodec{Level: 1}},
		{"gzip5", GzipCodec{Level: 5}},
		{"gzip9", GzipCodec{Level: 9}},
		{"shuffle4", ShuffleCodec{ElementSize: 4}},
		{"crc32c", CRC32CCodec{}},
	} {
		enc, err := c.c.EncodeBytes(raw)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(c.name+"/encode", func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for range b.N {
				if _, err := c.c.EncodeBytes(raw); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(raw))/float64(len(enc)), "ratio")
		})
		b.Run(c.name+"/decode", func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for range b.N {
				if _, err := c.c.DecodeBytes(enc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	// allFill is what every chunk written is checked with, and scans the
	// whole of a chunk that is all fill.
	b.Run("allFill", func(b *testing.B) {
		zeros := make([]float32, 512*512)
		b.SetBytes(int64(len(raw)))
		for range b.N {
			if !allFill(zeros, 0) {
				b.Fatal("not all fill")
			}
		}
	})
}
