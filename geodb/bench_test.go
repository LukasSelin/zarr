package geodb

import (
	"cmp"
	"context"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/LukasSelin/zarr"
)

// latent is a store each read of which takes a while, as one far away does.
type latent struct {
	*zarr.MemoryStore
	delay time.Duration
}

func (l latent) Get(ctx context.Context, k string) ([]byte, error) {
	time.Sleep(l.delay)
	return l.MemoryStore.Get(ctx, k)
}

func (l latent) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	time.Sleep(l.delay)
	return l.MemoryStore.GetRange(ctx, k, off, n)
}

// benchTile is a tile of side pixels with scenes scenes of NDVI and parcels
// parcels of about 40 by 40 pixels on it, in a memory store.
func benchTile(b *testing.B, side, scenes, parcels int) (*DB, *zarr.MemoryStore) {
	b.Helper()
	ctx := context.Background()
	s := zarr.NewMemoryStore()
	db, err := Create(ctx, s)
	if err != nil {
		b.Fatal(err)
	}
	g := Grid{CRS: "EPSG:32633", X0: 380000, Y0: 6170000, Dx: 10, Dy: 10, Width: side, Height: side}
	r := rand.New(rand.NewPCG(1, 1))
	ps := make([]Parcel, parcels)
	for i := range ps {
		c, row := r.Float64()*float64(side-40), r.Float64()*float64(side-40)
		ps[i] = parcelFromPixels(g, int64(i), rect(c, row, c+20+r.Float64()*20, row+20+r.Float64()*20))
	}
	if err := db.PutParcels(ctx, ps); err != nil {
		b.Fatal(err)
	}
	tile, err := db.CreateTile(ctx, "t", TileOptions{Grid: g, Variables: []Variable{{Name: "ndvi", Scale: 1e-4}}})
	if err != nil {
		b.Fatal(err)
	}
	for i := range scenes {
		if _, err := tile.AppendScene(ctx, benchScene(g, i)); err != nil {
			b.Fatal(err)
		}
	}
	if scenes > 0 {
		if err := tile.UpdateComposites(ctx, time.Unix(0, 0), time.Now()); err != nil {
			b.Fatal(err)
		}
	}
	return db, s
}

func benchScene(g Grid, i int) Scene {
	r := rand.New(rand.NewPCG(uint64(i), 7))
	vals := make([]float32, g.Width*g.Height)
	for k := range vals {
		col, row := k%g.Width, k/g.Width
		vals[k] = float32(0.3 + 0.2*math.Sin(float64(col)/50+float64(i)/9) + 0.1*math.Cos(float64(row)/70) + 0.02*r.Float64())
		if (col/200+row/200+i)%5 == 0 { // a block of cloud
			vals[k] = float32(math.NaN())
		}
	}
	return Scene{
		Time:   time.Date(2024, 3, 1, 10, 0, 0, 0, time.UTC).Add(time.Duration(i) * 5 * 24 * time.Hour),
		Values: map[string][]float32{"ndvi": vals},
	}
}

// BenchmarkAppendScene is a scene of a tile of 2048 by 2048 pixels, with
// statistics of 2000 parcels: encoding, writing every chunk and reducing
// every parcel.
func BenchmarkAppendScene(b *testing.B) {
	db, _ := benchTile(b, 2048, 0, 2000)
	tile := db.Tile("t")
	g := tile.Grid()
	scenes := make([]Scene, 8)
	for i := range scenes {
		scenes[i] = benchScene(g, i)
	}
	b.SetBytes(int64(4 * g.Width * g.Height))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc := scenes[i%len(scenes)]
		sc.Time = sc.Time.Add(time.Duration(i) * 365 * 24 * time.Hour)
		if _, err := tile.AppendScene(context.Background(), sc); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHistory compares the ways of answering the history of a place
// over 72 scenes - a year of passes - from a store whose reads take 5 ms,
// as an object store's do: a parcel's statistics, a pixel's composites, and
// a pixel's value in every scene.
func BenchmarkHistory(b *testing.B) {
	db, mem := benchTile(b, 1024, 72, 500)
	ctx := context.Background()
	slow, err := Open(ctx, latent{mem, 5 * time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}
	tile := slow.Tile("t")
	_ = db
	r := rand.New(rand.NewPCG(9, 9))
	b.Run("parcel statistics", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			obs, err := slow.ParcelSeries(ctx, int64(r.IntN(500)), "ndvi")
			if err != nil || len(obs) != 72 {
				b.Fatal(len(obs), err)
			}
		}
	})
	b.Run("pixel composites", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := tile.CompositeHistory(ctx, "ndvi", r.IntN(1024), r.IntN(1024)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("pixel in every scene", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := tile.PixelHistory(ctx, "ndvi", r.IntN(1024), r.IntN(1024)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkParcelsIn is the parcels in a box of a fiftieth of the area, of a
// million: about 20 000 of them.
func BenchmarkParcelsIn(b *testing.B) {
	r := rand.New(rand.NewPCG(5, 5))
	boxes := make([]BBox, 1_000_000)
	for i := range boxes {
		x, y := r.Float64()*10, 55+r.Float64()*5
		boxes[i] = BBox{x, y, x + 0.003, y + 0.002}
	}
	keys := hilbertKeys(boxes)
	order := make([]int, len(boxes))
	for i := range order {
		order[i] = i
	}
	sorted := make([]BBox, len(boxes))
	slices.SortFunc(order, func(a, b int) int { return cmp.Compare(keys[a], keys[b]) })
	for k, i := range order {
		sorted[k] = boxes[i]
	}
	tree := newRTree(sorted)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x, y := r.Float64()*9, 55+r.Float64()*4
		n := 0
		tree.search(BBox{x, y, x + 1, y + 1}, func(int) { n++ })
		if n == 0 {
			b.Fatal("found none")
		}
	}
}
