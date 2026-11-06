module github.com/LukasSelin/zarr/pycompare

go 1.25

toolchain go1.25.13

require (
	github.com/LukasSelin/zarr v0.4.0
	github.com/LukasSelin/zarr/s3 v0.2.0
	github.com/LukasSelin/zarr/zstd v0.0.0-00010101000000-000000000000
	github.com/aws/aws-sdk-go-v2 v1.47.8
	github.com/aws/aws-sdk-go-v2/config v1.33.16
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.3
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.22 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.16 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.12.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.39.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.44.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.9 // indirect
	github.com/aws/smithy-go v1.28.8 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
)

// pycompare is a command, not a package anyone imports, so these replaces
// always hold: it times the core and the modules beside it as they are in
// this repository, not as they were tagged.
replace (
	github.com/LukasSelin/zarr => ../
	github.com/LukasSelin/zarr/s3 => ../s3
	github.com/LukasSelin/zarr/zstd => ../zstd
)
