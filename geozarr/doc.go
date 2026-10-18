// Package geozarr keeps data that is tied to places on the earth in Zarr
// stores, in the two shapes such data comes in.
//
// A Raster is an array whose last two dimensions are rows and columns of
// pixels, with a CRS and an affine Transform that put each pixel on the map.
// They are written as the attributes of GeoZarr's proj: and spatial:
// conventions (proj:code, proj:wkt2, spatial:transform, spatial:bbox, ...),
// and as GDAL's _CRS, and with RasterOptions.Coordinates as the x, y and
// spatial_ref arrays that xarray and rioxarray read. A raster is read by a
// bounding box in map coordinates (ReadBBox), which fetches only the chunks
// the box is in; at points (Sampler), which keeps the chunks it has decoded;
// and written whole by Ingest, one stored object at a time and many at once,
// so that nothing written is read back.
//
// Units are the places data is kept for when it is not on a grid: regions,
// postcodes, catchments, the cells of a discrete global grid. Each has an id
// and may have a bounding box, and a Variable is an array along them - a
// time series for each, say - laid out so that one unit's values are one
// chunk (ReadUnit, ReadUnits). Zonal gathers the statistics of a raster's
// pixels for each unit from a raster of unit ids on the same grid.
//
// It is built on github.com/LukasSelin/zarr alone, and the standard library.
// It does not reproject: a raster, its zones and a bounding box asked of it
// are all in one CRS.
package geozarr
