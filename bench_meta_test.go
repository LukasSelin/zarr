package zarr

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// The metadata benchmarks are what opening, creating and walking a
// hierarchy costs apart from its chunks: the zarr.json of each node, parsed
// and written, and the round trips to the store that takes. Each reports
// gets/op and sets/op, the round trips, since over a network those are the
// whole of it.

// tallyStore counts the calls made of a store.
type tallyStore struct {
	Store
	gets, sets, lists atomic.Int64
}

func (s *tallyStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, key)
}

func (s *tallyStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.gets.Add(1)
	return s.Store.(RangeGetter).GetRange(ctx, key, offset, length)
}

func (s *tallyStore) Set(ctx context.Context, key string, value []byte) error {
	s.sets.Add(1)
	return s.Store.Set(ctx, key, value)
}

func (s *tallyStore) List(ctx context.Context, prefix string, fn func(key string) error) error {
	s.lists.Add(1)
	return s.Store.List(ctx, prefix, fn)
}

func (s *tallyStore) ListDir(ctx context.Context, prefix string, fn func(name string) error) error {
	s.lists.Add(1)
	return ListDir(ctx, s.Store, prefix, fn)
}

func (s *tallyStore) reset() { s.gets.Store(0); s.sets.Store(0); s.lists.Store(0) }

func (s *tallyStore) report(b *testing.B) {
	b.ReportMetric(float64(s.gets.Load())/float64(b.N), "gets/op")
	b.ReportMetric(float64(s.sets.Load())/float64(b.N), "sets/op")
	b.ReportMetric(float64(s.lists.Load())/float64(b.N), "lists/op")
}

// eachMetaStore runs f with a maker of empty memory stores, and of empty
// directories. A benchmark function runs more than once as b.N grows, so
// each run makes a store of its own.
func eachMetaStore(b *testing.B, f func(b *testing.B, store func(b *testing.B) *tallyStore)) {
	b.Run("store=memory", func(b *testing.B) {
		f(b, func(*testing.B) *tallyStore { return &tallyStore{Store: NewMemoryStore()} })
	})
	b.Run("store=dir", func(b *testing.B) {
		f(b, func(b *testing.B) *tallyStore { return &tallyStore{Store: NewDirStore(b.TempDir())} })
	})
}

// metaAttrs is n attributes of the kind a raster carries: a CRS as WKT, a
// transform, and scalars.
func metaAttrs(n int) map[string]any {
	attrs := map[string]any{}
	if n == 0 {
		return attrs
	}
	attrs["crs_wkt"] = `PROJCRS["WGS 84 / UTM zone 33N",BASEGEOGCRS["WGS 84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]]],CONVERSION["UTM zone 33N",METHOD["Transverse Mercator"],PARAMETER["Latitude of natural origin",0],PARAMETER["Longitude of natural origin",15],PARAMETER["Scale factor at natural origin",0.9996],PARAMETER["False easting",500000],PARAMETER["False northing",0]],CS[Cartesian,2],AXIS["easting",east],AXIS["northing",north],UNIT["metre",1],ID["EPSG",32633]]`
	attrs["transform"] = []float64{10, 0, 600000, 0, -10, 6800000}
	for i := len(attrs); i < n; i++ {
		attrs[fmt.Sprintf("attr_%03d", i)] = map[string]any{"value": float64(i) * 1.5, "units": "m", "valid": []int{0, 10000}}
	}
	return attrs
}

// metaOptions is a raster's array: 10980 square, as a Sentinel-2 tile at
// 10 m, in chunks of 512, float32, with its attributes.
func metaOptions(sharded bool, attrs int) ArrayOptions {
	o := ArrayOptions{
		Shape:          []int{10980, 10980},
		ChunkShape:     []int{512, 512},
		DataType:       Float32,
		FillValue:      float32(-9999),
		Codecs:         []Codec{BytesCodec{Endian: Little}, GzipCodec{Level: 5}},
		DimensionNames: []string{"y", "x"},
		Attributes:     metaAttrs(attrs),
	}
	if sharded {
		o.ShardShape = []int{2048, 2048}
	}
	return o
}

func eachMetaShape(b *testing.B, f func(b *testing.B, sharded bool, attrs int)) {
	for _, sharded := range []bool{false, true} {
		for _, attrs := range []int{0, 2, 50} {
			b.Run(fmt.Sprintf("sharded=%t/attrs=%d", sharded, attrs), func(b *testing.B) { f(b, sharded, attrs) })
		}
	}
}

func BenchmarkMetaOpenArray(b *testing.B) {
	eachMetaStore(b, func(b *testing.B, store func(b *testing.B) *tallyStore) {
		eachMetaShape(b, func(b *testing.B, sharded bool, attrs int) {
			s := store(b)
			path := fmt.Sprintf("open_%t_%d", sharded, attrs)
			if _, err := CreateArray(ctx, s, path, metaOptions(sharded, attrs)); err != nil {
				b.Fatal(err)
			}
			s.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := OpenArray(ctx, s, path); err != nil {
					b.Fatal(err)
				}
			}
			s.report(b)
		})
	})
}

func BenchmarkMetaCreateArray(b *testing.B) {
	eachMetaStore(b, func(b *testing.B, store func(b *testing.B) *tallyStore) {
		eachMetaShape(b, func(b *testing.B, sharded bool, attrs int) {
			s := store(b)
			o := metaOptions(sharded, attrs)
			paths := make([]string, b.N)
			for i := range paths {
				paths[i] = fmt.Sprintf("create_%t_%d_%d", sharded, attrs, i)
			}
			s.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := CreateArray(ctx, s, paths[i], o); err != nil {
					b.Fatal(err)
				}
			}
			s.report(b)
		})
	})
}

func BenchmarkMetaOpenGroup(b *testing.B) {
	eachMetaStore(b, func(b *testing.B, store func(b *testing.B) *tallyStore) {
		for _, attrs := range []int{0, 50} {
			b.Run(fmt.Sprintf("attrs=%d", attrs), func(b *testing.B) {
				s := store(b)
				path := fmt.Sprintf("g%d", attrs)
				if _, err := CreateGroup(ctx, s, path, metaAttrs(attrs)); err != nil {
					b.Fatal(err)
				}
				s.reset()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := OpenGroup(ctx, s, path); err != nil {
						b.Fatal(err)
					}
				}
				s.report(b)
			})
		}
	})
}

// BenchmarkMetaSetAttributes sets one attribute on an array that has
// attrs of them already: the whole zarr.json is written again.
func BenchmarkMetaSetAttributes(b *testing.B) {
	eachMetaStore(b, func(b *testing.B, store func(b *testing.B) *tallyStore) {
		for _, attrs := range []int{0, 50} {
			b.Run(fmt.Sprintf("attrs=%d", attrs), func(b *testing.B) {
				s := store(b)
				a, err := CreateArray(ctx, s, fmt.Sprintf("set%d", attrs), metaOptions(false, attrs))
				if err != nil {
					b.Fatal(err)
				}
				s.reset()
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if err := a.SetAttributes(ctx, map[string]any{"updated": i}); err != nil {
						b.Fatal(err)
					}
				}
				s.report(b)
			})
		}
	})
}

// BenchmarkMetaAttribute decodes one attribute of an open array: nothing
// but JSON, the store not touched.
func BenchmarkMetaAttribute(b *testing.B) {
	a, err := CreateArray(ctx, NewMemoryStore(), "a", metaOptions(false, 50))
	if err != nil {
		b.Fatal(err)
	}
	b.Run("transform", func(b *testing.B) {
		b.ReportAllocs()
		var t []float64
		for range b.N {
			if ok, err := a.Attribute("transform", &t); !ok || err != nil {
				b.Fatal(ok, err)
			}
		}
	})
	b.Run("crs_wkt", func(b *testing.B) {
		b.ReportAllocs()
		var wkt string
		for range b.N {
			if ok, err := a.Attribute("crs_wkt", &wkt); !ok || err != nil {
				b.Fatal(ok, err)
			}
		}
	})
}

// catalog is a group of n arrays, each a band of a scene, as a raster
// catalogue holds them.
func catalog(b *testing.B, s Store, n int) *Group {
	b.Helper()
	g, err := CreateGroup(ctx, s, "scene", metaAttrs(2))
	if err != nil {
		b.Fatal(err)
	}
	for i := range n {
		if _, err := g.CreateArray(ctx, fmt.Sprintf("B%03d", i), metaOptions(false, 2)); err != nil {
			b.Fatal(err)
		}
	}
	return g
}

// BenchmarkMetaChildren lists the children of a group of n arrays, which
// reads each child's zarr.json to say what it is.
func BenchmarkMetaChildren(b *testing.B) {
	eachMetaStore(b, func(b *testing.B, store func(b *testing.B) *tallyStore) {
		for _, n := range []int{10, 100, 1000} {
			b.Run(fmt.Sprintf("children=%d", n), func(b *testing.B) {
				if n > 100 && testing.Short() {
					b.Skip("-short")
				}
				sub := store(b)
				catalog(b, sub, n)
				g, err := OpenGroup(ctx, sub, "scene")
				if err != nil {
					b.Fatal(err)
				}
				sub.reset()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					cs, err := g.Children(ctx)
					if err != nil || len(cs) != n {
						b.Fatal(len(cs), err)
					}
				}
				sub.report(b)
			})
		}
	})
}

// BenchmarkMetaOpenCatalog opens a group and every array in it, as a reader
// of a scene does before it reads a pixel.
func BenchmarkMetaOpenCatalog(b *testing.B) {
	eachMetaStore(b, func(b *testing.B, store func(b *testing.B) *tallyStore) {
		for _, n := range []int{10, 100} {
			b.Run(fmt.Sprintf("arrays=%d", n), func(b *testing.B) {
				sub := store(b)
				catalog(b, sub, n)
				sub.reset()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					g, err := OpenGroup(ctx, sub, "scene")
					if err != nil {
						b.Fatal(err)
					}
					cs, err := g.Children(ctx)
					if err != nil {
						b.Fatal(err)
					}
					for _, c := range cs {
						if _, err := g.OpenArray(ctx, c.Name); err != nil {
							b.Fatal(err)
						}
					}
				}
				sub.report(b)
			})
		}
	})
}

// BenchmarkMetaOpenMissing opens what is not there: the three extra gets
// that look for version 2 metadata to say so are here.
func BenchmarkMetaOpenMissing(b *testing.B) {
	s := &tallyStore{Store: NewMemoryStore()}
	b.ReportAllocs()
	for range b.N {
		if _, err := OpenArray(ctx, s, "nothing"); err == nil {
			b.Fatal("opened nothing")
		}
	}
	s.report(b)
}
