module github.com/LukasSelin/zarr/azblob

go 1.26.0

toolchain go1.26.9

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.23.2
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.2
	github.com/LukasSelin/zarr v0.3.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/internal v0.1.0 // indirect
	github.com/apache/arrow-go/v18 v18.7.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.28 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260813180055-c1d0aacb2297 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

// The require above is what a consumer gets: Go ignores this replace in a
// module that is not the main one. It is here so that the tests in this
// repository run against the core beside them, and scripts/published.sh
// checks this module against the core it requires, with the replace dropped.
replace github.com/LukasSelin/zarr => ../
