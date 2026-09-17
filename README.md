# zarr

Zarr version 3 for Go, standard library only.

A Zarr store is N-dimensional arrays cut into chunks, each chunk encoded
and kept under its own key: `height/zarr.json` says what the array is, and
`height/c/3/7` holds one chunk of it. The same store can be a directory,
an object store or memory, and it can be read from Python (`zarr`,
`xarray`), JavaScript, Rust and Julia.

It began inside [terra](https://github.com/LukasSelin/terra), whose
`cmd/zarr` writes worlds with it, and keeps its history from there.

```go
s := zarr.NewDirStore("world.zarr")
root, _ := zarr.CreateGroup(ctx, s, "", map[string]any{"seed": seed})
h, _ := root.CreateArray(ctx, "height", zarr.ArrayOptions{
	Shape:          []int{512, 1024},
	ChunkShape:     []int{64, 64},
	DataType:       zarr.Float32,
	Codecs:         []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.GzipCodec{Level: 5}},
	DimensionNames: []string{"y", "x"},
})
err := zarr.Write(ctx, h, nil, nil, heights)         // the whole array
row, err := zarr.Read[float32](ctx, h, []int{100, 0}, []int{1, 1024})
```

## What it has

| Part of the specification | Here |
|---|---|
| Arrays, groups, attributes, dimension names | yes |
| `bool`, `int8`-`int64`, `uint8`-`uint64`, `float32`, `float64` | yes |
| `float16`, `complex*`, `r*` raw types | no |
| Regular chunk grid | yes |
| `default` and `v2` chunk key encodings, `/` or `.` | yes |
| `bytes` codec, either endian | yes |
| `gzip`, `crc32c` codecs | yes |
| `zstd` | in [`zstd`](zstd), a module of its own |
| `blosc` | not in the standard library; register one with `RegisterCodec` |
| `transpose` codec | no |
| `sharding_indexed` codec, index at either end | yes: `ArrayOptions.ShardShape`, or a `ShardingCodec` of your own |
| Extensions with `must_understand: false` | ignored, as the specification allows |
| Storage transformers | refused |

Chunks that hold nothing but the fill value are deleted rather than
written, as zarr-python does; set `Array.WriteEmptyChunks` to keep them.

## Growing an array

`Append` writes after the end of an array along one axis and grows it;
`Resize` sets its shape outright:

```go
fwi, _ := root.CreateArray(ctx, "fwi", zarr.ArrayOptions{
	Shape:      []int{0, 250, 620}, // time, y, x
	ChunkShape: []int{32, 250, 620},
	DataType:   zarr.Float32,
	FillValue:  math.NaN(),
})
day, err := zarr.Append(ctx, fwi, 0, grid) // grid is 250 by 620 long
```

`Append` writes the chunks before the metadata, so one that fails part way
leaves the shape as it was. Growing writes nothing but the metadata: no
chunk holds anything past the end of its array, `WriteChunk` included, so
what is past the old end reads as fill. Shrinking deletes the chunks past
the new end and fills the ones it cuts through before writing the metadata.
A handle on the array keeps its shape until `Refresh`, and nothing stops two
writers growing one array at once: one of them would lose.

## Stores in S3

`github.com/LukasSelin/zarr/s3` is a module of its own, so that the core
needs nothing past the standard library. Its `Store` is a `RangeGetter`: a
sharded array is read a shard's index and the chunks it needs at a time,
and a shard's index at its end is one request. A region read has as many
requests in flight as `Array.Concurrency`, so the 30 to 50 ms of a GET is
waited out sixteen keys at a time by default rather than one.

```go
cfg, _ := config.LoadDefaultConfig(ctx)
s := s3.New(awss3.NewFromConfig(cfg), "my-bucket", "fwi.zarr")
```

S3 answers a read of a key that is not there with 403 rather than 404 unless
the reader may `s3:ListBucket`, and a chunk never written is then an error
rather than fill: grant it along with `s3:GetObject`. Both modules build
with Go 1.23; the S3 module holds `aws-sdk-go-v2/service/s3` at v1.96.2, the
last before it asked for Go 1.24, and a program that requires a later one
gets that.

## zstd

zstd is not in the standard library, so its codec is a module of its own,
[`github.com/LukasSelin/zarr/zstd`](zstd), with the pure Go compressor of
[klauspost/compress](https://github.com/klauspost/compress). Importing it
registers the codec, so arrays that use zstd open:

```go
import zstd "github.com/LukasSelin/zarr/zstd"

Codecs: []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zstd.Codec{Level: 3}},
```

A zstd chunk is held to what a gzip chunk is. A codec of your own can be
too: one that is a `LimitedBytesDecoder` is told the most a chunk may
decode to, and one that is a `BoundedBytesEncoder` says the most it
encodes to, for the codecs after it.

## Shards

One object a chunk is a great many files at scale: a 16 000 by 8 000 map in
chunks of 64 is 31 250 of them for every field. A sharded array keeps many
chunks in one object, with an index of where each is:

```go
h, _ := root.CreateArray(ctx, "height", zarr.ArrayOptions{
	Shape:      []int{8000, 16000},
	ChunkShape: []int{64, 64},   // what is read and written
	ShardShape: []int{1024, 1024}, // what is stored: 256 chunks each
	DataType:   zarr.Float32,
	Codecs:     []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.GzipCodec{Level: 5}},
})
```

`Read` from a store that is a `RangeGetter` - `DirStore` and `MemoryStore`
both are - reads a shard's index and then only the chunks it needs. `Write`
writes whole shards, as zarr-python does: a region that covers part of a
shard reads the rest of it and writes it all back.

## Chunks at once

`Read`, `Write`, `Resize` and `Append` work on up to `Array.Concurrency`
stored objects at once, sixteen by default. A read of 508 chunks over a
store whose round trip is 40 ms takes 508 of them one at a time - twenty
seconds of nothing but waiting - and sixteen at a time it is about one.
Decoding a chunk is microseconds, so the number to pick is how many
requests the store will bear, not how many cores there are.

```go
h.Concurrency = 64 // an object store far away
h.Concurrency = 1  // one at a time, as this package once did
```

Each object in flight is held in memory, and a gzip writer besides when
writing, so an array of large shards read from a store that cannot do
ranges wants a smaller number. The order chunks are fetched in is not
defined, and a `Write` that fails part way has written some of the stored
objects it covers and not others.

## Stores that are not trusted

Metadata, chunks and shards that are malformed are an error, never a
panic, and a store cannot make a read allocate more than its metadata
implies, times `Array.Concurrency` - which is a Go field, so nothing a
store holds can raise it. An array does not open if its shape counts more elements than an
int, or if a chunk, a shard or a shard's index would be more than 2 GiB. A
gzip chunk may inflate to no more than its elements take, through the
codecs before it. A shard's index must put every chunk inside the shard,
clear of the index and of every other chunk. A region read is made whole
in memory, so look at the shape of an array from a stranger before reading
all of it.

The fuzz tests hold this: metadata, each codec, a shard and its index, and
an array opened and read from a store of fuzzed keys, none of which may
panic or allocate past a bound. `go test` runs their seeds and
`testdata/fuzz`; to fuzz one:

```sh
go test -run '^$' -fuzz '^FuzzOpenAndRead$' -fuzztime 5m .
```

`bench_test.go` has Write and Read of a 512 by 1024 float64 array in chunks
of 64 at gzip 5, with and without shards of 16 chunks, a 3 by 3 region from
shards in a directory, and ReadChunk and WriteChunk.

## Against zarr-python

`TestZarrPython` has zarr-python write every case in
`testdata/interop/cases.json` for this package to read, and reads back with
zarr-python every case this package writes: every data type, both endians,
gzip and crc32c alone and together, NaN and infinite fills, both
separators, a scalar, a nested group, dimension names, a `uint64`
attribute, and shards with the index at the end, at the start, and with a
codec after the shard. It is skipped unless `ZARR_PYTHON` names a Python with `zarr`
and `numpy`:

```sh
python -m venv .venv && .venv/bin/pip install zarr numpy
ZARR_PYTHON=.venv/bin/python go test -run ZarrPython .
```

Last run against zarr-python 3.4.0, numpy 2.5.3.
