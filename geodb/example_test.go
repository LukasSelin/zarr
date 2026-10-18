package geodb_test

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/geodb"
)

func Example() {
	ctx := context.Background()
	db, err := geodb.Create(ctx, zarr.NewMemoryStore()) // or zarr.NewDirStore, s3.New, ...
	if err != nil {
		panic(err)
	}

	// A tile of 100 by 100 pixels of 10 m in UTM 33N, south of Malmö.
	tile, err := db.CreateTile(ctx, "33UUB", geodb.TileOptions{
		Grid:      geodb.Grid{CRS: "EPSG:32633", X0: 370000, Y0: 6160000, Dx: 10, Dy: 10, Width: 100, Height: 100},
		Variables: []geodb.Variable{{Name: "ndvi", Scale: 1e-4, Composite: "max"}},
	})
	if err != nil {
		panic(err)
	}

	// A field, in longitude and latitude.
	field := geodb.Parcel{ID: 42, Rings: [][]geodb.Point{{
		{X: 12.9440, Y: 55.5630}, {X: 12.9490, Y: 55.5630}, {X: 12.9490, Y: 55.5660}, {X: 12.9440, Y: 55.5660},
	}}}
	if err := db.PutParcels(ctx, []geodb.Parcel{field}); err != nil {
		panic(err)
	}

	// Three passes, the second under cloud.
	for i, ndvi := range []float32{0.41, float32(math.NaN()), 0.47} {
		vals := make([]float32, 100*100)
		for k := range vals {
			vals[k] = ndvi
		}
		day := time.Date(2025, 5, 3+5*i, 10, 20, 0, 0, time.UTC)
		if _, err := tile.AppendScene(ctx, geodb.Scene{Time: day, Values: map[string][]float32{"ndvi": vals}}); err != nil {
			panic(err)
		}
	}

	series, err := db.ParcelSeries(ctx, 42, "ndvi")
	if err != nil {
		panic(err)
	}
	for _, o := range series {
		fmt.Printf("%s valid %.0f%% mean %.2f\n", o.Time.Format("2006-01-02"), 100*o.ValidFraction, o.Mean)
	}
	// Output:
	// 2025-05-03 valid 100% mean 0.41
	// 2025-05-08 valid 0% mean NaN
	// 2025-05-13 valid 100% mean 0.47
}
