module github.com/LukasSelin/zarr/zstd

go 1.25

toolchain go1.25.13

require (
	github.com/LukasSelin/zarr v0.0.0-00010101000000-000000000000
	github.com/klauspost/compress v1.20.0
)

require go.uber.org/goleak v1.3.0

// Until package zarr is tagged with LimitedBytesDecoder and
// BoundedBytesEncoder, this builds against the package beside it.
replace github.com/LukasSelin/zarr => ../
