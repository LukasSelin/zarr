# geodb

A store for satellite time series, weather and parcels over one area, laid
out in Zarr version 3 with [`github.com/LukasSelin/zarr`](..), so that the
questions asked most often are a few chunk reads:

- the history of a parcel (NDVI and the like): one chunk per 128 scenes;
- the history of a pixel: one chunk per 64 composite periods;
- a map of one date: the chunks of the scene it covers;
- wind or irradiance at a parcel: one chunk per 720 steps.

It is a module of its own, so the core stays with the standard library;
geodb needs nothing past the core either. The store it writes is plain Zarr
that xarray opens (`xr.open_zarr(store, group="tiles/33UUB/scenes")`), with
CF attributes for scaled integers and dimension names on every array:
xarray 2026.9 with zarr-python 3.4 reads every group, decoding the times,
the scaling, and the fill as NaN.

## How it is laid out

Each kind of data is kept on the grid it comes on, and parcels tie them
together:

| | Grid | Chunks | For |
|---|---|---|---|
| `tiles/<t>/scenes/<var>` | the tile's own, e.g. UTM | 1 scene by 512 by 512 | a map of one date |
| `tiles/<t>/composites/<var>` | the tile's own | 64 periods by 64 by 64, in shards of 512 | a pixel's history |
| `tiles/<t>/stats/<var>` | parcels in the tile by scenes by 6 | 128 by 128 by 6 | a parcel's history |
| `weather/<s>/<var>` | longitude and latitude | 720 steps by 32 by 32 | weather at a point |
| `parcel_weather/<s>/<var>` | parcels by steps | 256 by 720 | weather at a parcel |
| `parcels/*` | ragged arrays, Hilbert-sorted | 65 536 parcels | the polygons |

Scenes come when the satellite passes, so their time axis is the real
acquisition times. A pixel the cloud mask throws out is stored as the fill
value, and a chunk of nothing but fill is never written: a scene under
cloud costs almost nothing. Composites put the scenes on a regular axis of
periods (ten days by default) by median, max or mean of the valid values.

Every scene appended is reduced, while still in memory, to six statistics
of every parcel it covers: the valid fraction, mean, standard deviation and
10th, 50th and 90th percentiles. Every weather step appended is sampled at
every parcel, bilinearly at the centre of its box. That is what makes a
parcel's history cheap: the work is done once, at ingest.

See [doc.go](doc.go) for the whole layout.

## Using it

```go
db, _ := geodb.Create(ctx, zarr.NewDirStore("/data/geo.zarr")) // or geodb.Open

// Parcels, in longitude and latitude. Put them before the scenes: putting
// them after works the statistics out again from every scene.
db.PutParcels(ctx, parcels)

// A Sentinel-2 tile in its UTM zone.
tile, _ := db.CreateTile(ctx, "33UUB", geodb.TileOptions{
	Grid: geodb.Grid{CRS: "EPSG:32633", X0: 300000, Y0: 6200040, Dx: 10, Dy: 10, Width: 10980, Height: 10980},
	Variables: []geodb.Variable{
		{Name: "ndvi", Scale: 1e-4, Composite: "max"},
		{Name: "ndmi", Scale: 1e-4},
	},
})
tile.AppendScene(ctx, geodb.Scene{Time: acquired, Values: map[string][]float32{"ndvi": ndvi, "ndmi": ndmi}})
tile.UpdateComposites(ctx, acquired, acquired)

// Wind and irradiance, hourly.
era5, _ := db.CreateWeather(ctx, "era5", geodb.WeatherOptions{
	Grid:      geodb.Grid{CRS: "EPSG:4326", X0: 10, Y0: 70, Dx: 0.25, Dy: 0.25, Width: 100, Height: 64},
	Start:     time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
	Step:      time.Hour,
	Variables: []string{"wind100", "ssrd"},
})
era5.Append(ctx, 720, monthOfValues)

// Questions.
obs, _ := db.ParcelSeries(ctx, 4021, "ndvi")                    // statistics in every scene
wind, _ := db.ParcelWeather(ctx, "era5", "wind100", 4021, from, to)
col, row, _ := tile.Pixel(lon, lat)
hist, _ := tile.CompositeHistory(ctx, "ndvi", col, row)
ids := db.ParcelsIn(geodb.BBox{MinX: 12.9, MinY: 55.5, MaxX: 13.1, MaxY: 55.7})
```

Values are `float32` with NaN for no value. A tile's variables are stored
as `int16` with a scale and an offset, so pick a scale that keeps the
precision you need: 1e-4 keeps NDVI to four places over -3.2 to 3.2.

## Serving it

`cmd/geodbd` serves a store as JSON, reading through a cache:

```sh
go run ./cmd/geodbd -dir /data/geo.zarr -addr :8080 -cache-mb 1024
curl 'localhost:8080/v1/parcels/4021/series?var=ndvi&min_valid=0.6'
curl 'localhost:8080/v1/parcels/4021/weather/era5?var=wind100&from=2025-06-01T00:00:00Z'
curl 'localhost:8080/v1/tiles/33UUB/history?var=ndvi&lon=13.01&lat=55.61'
```

The routes are listed on `NewHandler`. Missing values are `null`.

## Writing safely

Each kind of data has a commit marker, written last: the `time` array of a
tile's scenes, the `steps` attribute of a weather source and of its parcel
samples, the `count` of the parcels. An append that fails part way leaves
the marker as it was, so readers see none of it, and the next append cuts
back what it left before it writes. Readers never look past a marker.

So one writer per tile and one per weather source is safe against any
number of readers, in any number of processes. Two writers to one tile are
not: the core's locks are in the process. Different tiles and sources may
be written at once, from anywhere. A DB serves reads from a snapshot;
`Refresh` takes a new one to see what other processes wrote.

## Caching

`NewCachedStore` keeps what is read in memory, least recently used out
first, and remembers keys that are not there too, which for scenes under
cloud is most of the saving. Reads of one key at once are one read of the
store. `zarr.json` is not cached unless `CacheOptions.Metadata` says so, so
a `Refresh` sees new scenes.

## Coordinates

Parcels and weather are in longitude and latitude. A tile is in its own CRS:
EPSG:4326 and the 120 UTM zones on WGS84 are built in (the projection is
Krüger's series, within a millimetre of PROJ, checked in `crs_test.go`), and
`RegisterProjection` adds any other from a function, EPSG:3035 for a
pan-European grid for instance.

## How fast

`go test -run '^$' -bench . -benchtime 20x -count 3`, median of three, on a
cloud VM of 2 vCPUs (Xeon, 2.8 GHz), Go 1.24:

| | |
|---|---:|
| `AppendScene`, 2048 by 2048 pixels, 2000 parcels | 385 ms (43 MB/s of float32) |
| A parcel's statistics in 72 scenes, reads of 5 ms | 8.7 ms |
| A pixel's composites over 72 scenes, reads of 5 ms | 13.5 ms |
| A pixel's value in each of 72 scenes, reads of 5 ms | 137 ms |
| `ParcelsIn` of a million parcels, a box holding 20 000 of them | 0.27 ms |

The three rows of history are the reason for the layout: from a store
whose reads take as long as an object store's, the precomputed statistics
and composites answer ten to fifteen times faster than reading every
scene. Appending is bound by gzip, which writes about 60 MB/s a core in the
core's own [benchmarks](../BENCHMARKS.md); a 10 980 square Sentinel-2 tile is
about 30 times the benchmark's scene.

## What it does not do

- Read Sentinel-2, ERA5 or any other product: hand it rasters as `[]float32`.
- Version the store. The commit markers make appends safe, but there are
  no snapshots to go back to; Icechunk's model is the one to look at.
- Several writers to one tile, or a parcel set that changes a parcel at a
  time: `PutParcels` replaces them all and works everything out again.
- Reproject between CRSs other than to a tile's own from longitude and
  latitude.
