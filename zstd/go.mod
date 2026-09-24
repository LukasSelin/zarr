module github.com/LukasSelin/zarr/zstd

go 1.25

toolchain go1.25.13

require (
	github.com/LukasSelin/zarr v0.3.0
	github.com/klauspost/compress v1.20.0
)

require go.uber.org/goleak v1.3.0

// The require above is what a consumer gets: Go ignores this replace in a
// module that is not the main one. It is here so that the tests in this
// repository run against the core beside them, and scripts/published.sh
// checks this module against the core it requires, with the replace dropped.
replace github.com/LukasSelin/zarr => ../
