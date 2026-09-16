# zarr

Zarr version 3 for Go, standard library only.

A Zarr store is N-dimensional arrays cut into chunks, each chunk encoded
and kept under its own key: `height/zarr.json` says what the array is, and
`height/c/3/7` holds one chunk of it. The same store can be a directory,
an object store or memory, and it can be read from Python (`zarr`,
`xarray`), JavaScript, Rust and Julia.

This module lives inside [terra](https://github.com/LukasSelin/terra) for
now, as a module of its own that terra does not import. It is meant to
leave for its own repository as it stands.

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
| `zstd`, `blosc` | not in the standard library; register one with `RegisterCodec` |
| `transpose` codec | no |
| `sharding_indexed` codec, index at either end | yes: `ArrayOptions.ShardShape`, or a `ShardingCodec` of your own |
| Extensions with `must_understand: false` | ignored, as the specification allows |
| Storage transformers | refused |

Chunks that hold nothing but the fill value are deleted rather than
written, as zarr-python does; set `Array.WriteEmptyChunks` to keep them.

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

## Stores that are not trusted

Metadata, chunks and shards that are malformed are an error, never a
panic, and a store cannot make a read allocate more than its metadata
implies. An array does not open if its shape counts more elements than an
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
