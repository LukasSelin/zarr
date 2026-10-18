"""Interop of geozarr rasters with xarray, rioxarray and GDAL: see geozarr/interop_test.go.

    python geozarr.py read DIR    read every raster geozarr wrote under DIR, and check it
    python geozarr.py write PATH  write a raster with rioxarray, for geozarr to read

Both sides make the pixel at (band, row, col) band*100000 + row*1000 + col,
and both take the CRS from epsg32633.wkt beside this file.

read opens each raster twice: with xarray and rioxarray, which find the CRS
through the grid_mapping attribute and spatial_ref (so decode_coords="all",
as for a raster rioxarray wrote itself), and with GDAL's Zarr driver through
rasterio, which finds the CRS in _CRS and in proj: (each is checked alone)
and the geotransform in the x and y arrays. A GDAL that cannot decode a sharded array is reported, not failed.
"""

import json
import pathlib
import shutil
import sys
import tempfile

import numpy as np
import rasterio
import rioxarray  # noqa: F401 - the .rio accessor
import xarray as xr
from affine import Affine
from rasterio.crs import CRS
from zarr.codecs import GzipCodec

HERE = pathlib.Path(__file__).parent
EPSG = 32633
WKT = (HERE / "epsg32633.wkt").read_text(encoding="utf-8")

# The rasters geozarr writes in interop_test.go: store, array, leading
# dimensions, (height, width), transform, dtype, fill value.
RASTERS = [
    dict(store="plain.zarr", name="elevation", leading={}, shape=(50, 70),
         transform=Affine(10, 0, 500000, 0, -20, 6600000), dtype="uint16", fill=65535, sharded=False),
    dict(store="sharded.zarr", name="bands", leading={"band": 2}, shape=(40, 48),
         transform=Affine(30, 0, 400000, 0, -30, 6500000), dtype="float32", fill=0, sharded=True),
]


def pixels(leading, shape, dtype):
    bands = np.arange(np.prod(list(leading.values()), dtype=int)).reshape(*leading.values(), 1, 1)
    rows, cols = np.arange(shape[0])[:, None], np.arange(shape[1])[None, :]
    return (bands * 100_000 + rows * 1000 + cols).astype(dtype)


def centres(t, shape):
    """The x and y of pixel centres along the first row and column."""
    x = np.array([(t * (c + 0.5, 0.5))[0] for c in range(shape[1])])
    y = np.array([(t * (0.5, r + 0.5))[1] for r in range(shape[0])])
    return x, y


def check_xarray(path, r, failures):
    ds = xr.open_zarr(path, consolidated=False, decode_coords="all")
    da = ds[r["name"]]
    want = pixels(r["leading"], r["shape"], r["dtype"])
    dims = (*r["leading"], "y", "x")
    if da.dims != dims:
        failures.append(f"xarray: dims {da.dims}, want {dims}")
    if da.dtype != want.dtype or not np.array_equal(da.values, want):
        failures.append(f"xarray: pixels {da.dtype} {da.shape}, want {want.dtype} {want.shape}")
    x, y = centres(r["transform"], r["shape"])
    if not np.array_equal(da.x.values, x) or not np.array_equal(da.y.values, y):
        failures.append(f"xarray: x {da.x.values[:3]}..., y {da.y.values[:3]}..., want {x[:3]}..., {y[:3]}...")
    if da.rio.crs != CRS.from_epsg(EPSG) or da.rio.crs.to_epsg() != EPSG:
        failures.append(f"rioxarray: CRS {da.rio.crs}")
    if not da.rio.transform().almost_equals(r["transform"]):
        failures.append(f"rioxarray: transform {da.rio.transform()}, want {r['transform']}")
    if not np.allclose(da.rio.bounds(), da.attrs["spatial:bbox"], rtol=0, atol=1e-6):
        failures.append(f"rioxarray: bounds {da.rio.bounds()}, spatial:bbox {da.attrs['spatial:bbox']}")
    if da.rio.x_dim != "x" or da.rio.y_dim != "y":
        failures.append(f"rioxarray: dims {da.rio.x_dim}, {da.rio.y_dim}")
    # Without decode_coords="all" the grid mapping is a variable, not a
    # coordinate, but the grid is still the x and y.
    plain = xr.open_zarr(path, consolidated=False)[r["name"]]
    if not plain.rio.transform().almost_equals(r["transform"]):
        failures.append(f"rioxarray, coordinates not decoded: transform {plain.rio.transform()}")


def check_gdal(path, r, failures, notes):
    try:
        src = rasterio.open(f"{path}/{r['name']}")
    except rasterio.RasterioIOError as e:
        if r["sharded"] and "sharding_indexed" in str(e):
            notes.append(f"GDAL {rasterio.__gdal_version__} cannot read sharded Zarr: {e}")
            return
        raise
    with src:
        want = pixels(r["leading"], r["shape"], r["dtype"]).reshape(-1, *r["shape"])
        if src.driver != "Zarr":
            failures.append(f"GDAL: driver {src.driver}")
        if src.crs is None or src.crs != CRS.from_epsg(EPSG) or src.crs.to_epsg() != EPSG:
            failures.append(f"GDAL: CRS {src.crs}")
        if not src.transform.almost_equals(r["transform"]):
            failures.append(f"GDAL: geotransform {src.transform}, want {r['transform']}")
        if (src.count, src.height, src.width) != want.shape:
            failures.append(f"GDAL: {src.count} bands of {src.height} by {src.width}, want {want.shape}")
        elif not np.array_equal(src.read(), want):
            failures.append("GDAL: pixels differ")
        if src.nodata != r["fill"]:
            failures.append(f"GDAL: nodata {src.nodata}, want {r['fill']}")
    # GDAL takes the CRS from _CRS and from proj: both, so give it each alone.
    for alone, drop in (("_CRS", ("proj:code", "proj:wkt2")), ("proj:", ("_CRS",))):
        with tempfile.TemporaryDirectory() as tmp:
            copy = pathlib.Path(tmp) / "copy.zarr"
            shutil.copytree(path, copy)
            meta = copy / r["name"] / "zarr.json"
            m = json.loads(meta.read_text(encoding="utf-8"))
            for k in drop:
                del m["attributes"][k]
            meta.write_text(json.dumps(m), encoding="utf-8")
            with rasterio.open(str(copy / r["name"])) as src:
                if src.crs is None or src.crs.to_epsg() != EPSG:
                    failures.append(f"GDAL, {alone} alone: CRS {src.crs}")


def read(dir):
    failures, notes = [], []
    for r in RASTERS:
        path = str(pathlib.Path(dir) / r["store"])
        for check in (check_xarray, check_gdal):
            fs = []
            try:
                if check is check_gdal:
                    check(path, r, fs, notes)
                else:
                    check(path, r, fs)
            except Exception as e:  # report every check, not just the first
                fs.append(f"{check.__name__}: {type(e).__name__}: {e}")
            failures += [f"{r['store']}/{r['name']}: {f}" for f in fs]
    print("\n".join(notes))
    if failures:
        print("\n".join(failures))
        sys.exit(1)
    print(f"read {len(RASTERS)} rasters with xarray {xr.__version__}, rioxarray {rioxarray.__version__}, GDAL {rasterio.__gdal_version__}")


def write(path):
    """Write the raster geozarr reads in interop_test.go, as rioxarray does."""
    t, shape = Affine(10, 0, 500000, 0, -20, 6600000), (50, 70)
    x, y = centres(t, shape)
    da = xr.DataArray(pixels({}, shape, "float32"), dims=("y", "x"), coords={"y": y, "x": x}, name="elevation")
    da = da.rio.write_crs(CRS.from_wkt(WKT)).rio.write_transform(t)
    # zarr-python compresses with zstd unless told otherwise, and zstd is a
    # module of its own that geozarr's tests do not import.
    # rioxarray keeps grid_mapping in the encoding, so add to it rather than
    # replace it.
    ds = da.to_dataset()
    for v in ds.variables:
        ds[v].encoding["compressors"] = [GzipCodec()]
    ds.to_zarr(path, mode="w", zarr_format=3, consolidated=False)
    print(f"wrote {path} with rioxarray {rioxarray.__version__}")


if __name__ == "__main__":
    {"write": write, "read": read}[sys.argv[1]](sys.argv[2])
