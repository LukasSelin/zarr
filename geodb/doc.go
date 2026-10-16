// Package geodb is a store for satellite time series, weather and parcels
// over one area, laid out in Zarr version 3 so that the questions asked of
// it most often are a few chunk reads.
//
// It holds three kinds of data, each on the grid it comes on:
//
//   - Tiles: scenes of a satellite product (NDVI and the like) on the native
//     grid of one tile, a Sentinel-2 MGRS tile in its UTM zone for instance.
//     Scenes come when the satellite passes, not on a regular axis, and a
//     pixel the cloud mask throws out is stored as the fill value, so a chunk
//     under cloud is not stored at all. Each tile also keeps composites:
//     the scenes reduced to a regular axis of periods (ten days, say),
//     chunked long in time for the history of a pixel.
//   - Weather: gridded, regular series such as wind at hub height or
//     irradiance, in longitude and latitude, chunked long in time.
//   - Parcels: polygons in longitude and latitude, kept in the store as
//     ragged arrays sorted along a Hilbert curve, and indexed in memory with
//     a packed R-tree.
//
// Parcels are what ties the others together. Every scene appended to a tile
// is reduced, while it is still in memory, to statistics of each parcel it
// covers (the valid fraction, mean, standard deviation and three
// percentiles), and every step of a weather source is sampled at each
// parcel. Both are kept as arrays with a parcel axis, so the history of a
// parcel is a handful of chunk reads rather than a scan of the rasters.
//
// # Layout
//
//	/                                group: {"geodb": 1}
//	parcels/                         group: {"count": P}
//	  id            int64   (P)
//	  bbox          float64 (P, 4)   min lon, min lat, max lon, max lat
//	  ring_offset   int64   (P+1)    rings of parcel i: ring_offset[i:i+2]
//	  coord_offset  int64   (R+1)    points of ring j: coord_offset[j:j+2]
//	  coords        float64 (C, 2)   longitude, latitude
//	tiles/<tile>/                    group: grid, variables, composite period
//	  scenes/time   int64   (N)      Unix seconds; the commit marker
//	  scenes/<var>  int16   (N, H, W)        chunks (1, M, M)
//	  composites/<var> int16 (K, H, W)       chunks (T, C, C), shards (T, M, M)
//	  stats/parcel  int64   (Pt)     which parcel each row of stats is
//	  stats/npix    int32   (Pt)     pixels of the parcel in the tile
//	  stats/span_offset int64 (Pt+1)
//	  stats/spans   int32   (S, 3)   row, first column, column past the last
//	  stats/<var>   float32 (Pt, N, 6)
//	weather/<source>/                group: grid, start, step, steps
//	  <var>         float32 (T, H, W)        chunks (720, 32, 32)
//	parcel_weather/<source>/         group: {"steps": T}
//	  <var>         float32 (P, T)           chunks (256, 720)
//
// Values of a tile's variables are int16 with the CF attributes
// scale_factor, add_offset and _FillValue, so xarray decodes them as it
// would a NetCDF file. Every array has dimension names.
//
// # Writing
//
// Each kind of data has a commit marker that is written last: the time
// array of a tile's scenes, the steps attribute of a weather source and of
// its parcel samples, the count attribute of the parcels. An append that
// fails part way leaves the marker as it was, so readers see nothing of it,
// and the next append cuts back what it left before writing. Readers never
// look past a marker.
//
// That makes one writer per tile, and one per weather source, safe against
// any number of readers in any number of processes. Two writers to the same
// tile are not: run ingest for a tile in one place. Writes to different
// tiles and sources may run at once.
//
// # Reading
//
// A DB keeps a snapshot of what it has opened - the times of each tile's
// scenes, its arrays, the parcel index - and serves reads from it, so a
// read is the chunk reads it needs and no metadata. Refresh takes a new
// snapshot to see what other processes have written; appends made through
// the DB itself are seen at once.
//
// Wrap the store in NewCachedStore to keep the chunks read most in memory.
//
// # Coordinates
//
// Parcels and weather are in longitude and latitude (EPSG:4326). A tile is
// in its own CRS; EPSG:4326 and the UTM zones on WGS84 (EPSG:32601 to
// 32660 and 32701 to 32760) are known, and RegisterProjection adds others,
// such as EPSG:3035, from a function that projects longitude and latitude.
package geodb
