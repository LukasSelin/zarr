# zarr

Zarr version 3 for Go, standard library only.

A Zarr store is N-dimensional arrays cut into chunks, each chunk encoded
and kept under its own key: `height/zarr.json` says what the array is, and
`height/c/3/7` holds one chunk of it. The same store can be a directory,
an object store or memory, and it can be read from Python (`zarr`,
`xarray`), JavaScript, Rust and Julia.

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
| `numcodecs.shuffle`, which the specification does not have | yes: `ShuffleCodec`, as zarr-python's `zarr.codecs.numcodecs.Shuffle` |
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

## Walking and deleting

`Store.List` calls a function with every key under a prefix, so a hierarchy
can be walked without holding all of it:

```go
var bytes int
err := s.List(ctx, "fwi/", func(key string) error {
	b, err := s.Get(ctx, key)
	bytes += len(b)
	return err
})
```

A prefix is a string and not a path: `"height"` is also the keys of
`"heightmap"`, and `""` is everything the store holds. Pass `path + "/"` for
everything under a node.

One level at a time is `zarr.ListDir`, which uses the store's own `ListDir`
where there is one - a bucket rolls a level up with a delimiter, a directory
reads one directory - and derives it from `List` where there is not, reading
every key under the prefix to do it. `Group.Children` is the nodes directly
in a group, with what each one is:

```go
for _, c := range children { // [{fwi array} {isi array} {sub group}]
	fmt.Println(c.Name, c.Type)
}
```

It costs a listing and a read of the metadata of each name, not a walk of
every chunk under the group.

`Delete` removes a node and everything under it:

```go
err := zarr.Delete(ctx, s, "fwi/isi") // or isi.Delete(ctx), root.Delete(ctx)
```

The metadata goes first, so a delete that fails part way leaves keys that
nothing opens rather than an array whose missing chunks read as the fill
value; doing it again clears what was left. It never reads the metadata, so
a node this package cannot open goes too. The root, `""`, is the whole
store. A `DirStore` keeps the directories a node was in, empty; nothing
reads them, and `List` does not yield them.

`Store` gained `List` in v0.3.0. A store of your own needs it, and may add
`ListDir`; `storetest.Run` checks either against the contract.

## Stores in S3

`github.com/LukasSelin/zarr/s3` is a module of its own, so that the core
needs nothing past the standard library. Its `Store` is a `RangeGetter`: a
sharded array is read a shard's index and the chunks it needs at a time,
and a shard's index at its end is one request. It is a `DirLister` too,
listing one level of a hierarchy with the delimiter S3 rolls a level up by.
A region read has as many requests in flight as `Array.Concurrency`, so the
30 to 50 ms of a GET is waited out sixteen keys at a time by default rather
than one.

```go
cfg, _ := config.LoadDefaultConfig(ctx)
s := s3.New(awss3.NewFromConfig(cfg), "my-bucket", "fwi.zarr")
```

S3 answers a read of a key that is not there with 403 rather than 404 unless
the reader may `s3:ListBucket`, and a chunk never written is then an error
rather than fill; `List` and `ListDir` do not work at all without it. Grant
it along with `s3:GetObject`. The core builds with Go 1.23; the S3 module
needs 1.24. It held `aws-sdk-go-v2/service/s3` at v1.96.2, the last before
the SDK asked for 1.24, until govulncheck found GO-2026-5764 in the
eventstream protocol under it: the fix is service/s3 v1.97.3, which asks
for 1.24, so that is what the module builds with now.

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

## Shuffle

A compressor sees a float field as elements that differ in every byte. The
shuffle codec lays the bytes out by their place in an element instead - every
first byte, then every second - so that the high bytes of a smooth field,
which hardly change, become long runs that deflate well:

```go
Codecs: []zarr.Codec{
	zarr.BytesCodec{Endian: zarr.Little},
	zarr.ShuffleCodec{ElementSize: zarr.Float32.Size()},
	zarr.GzipCodec{Level: 5},
},
```

`ElementSize` has to be given because a bytes-to-bytes codec is handed
nothing but bytes: it cannot be told the data type. It is
`numcodecs.shuffle`, which the version 3 core specification does not have and
zarr-python writes as `zarr.codecs.numcodecs.Shuffle`.

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

Regions that do not overlap may be written from as many goroutines as you
like, through one handle on the array or several, even where they share a
chunk or a shard: a `Write` that covers part of a stored object reads it,
patches it and writes it back under a lock of that object's own. The lock
is in the process, so two processes - or two machines - writing regions
that share a stored object can still lose one of them; split such work
along `ChunkShape`, or `ShardShape` if the array is sharded.

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
shards in a directory, ReadChunk and WriteChunk, and a chunk shuffled and
unshuffled.

## Against zarr-python

`TestZarrPython` has zarr-python write every case in
`testdata/interop/cases.json` for this package to read, and reads back with
zarr-python every case this package writes: every data type, both endians,
gzip, crc32c and `numcodecs.shuffle` alone and together, NaN and infinite
fills, both separators, a scalar, a nested group, dimension names, a
`uint64` attribute, and shards with the index at the end, at the start, and
with a codec after the shard. It is skipped unless `ZARR_PYTHON` names a Python with `zarr`
and `numpy`:

```sh
python -m venv .venv && .venv/bin/pip install zarr numpy
ZARR_PYTHON=.venv/bin/python go test -run ZarrPython .
```

Last run against zarr-python 3.4.0, numcodecs 0.17.0, numpy 2.5.3.

## Checks

`make check` runs what CI runs, over all three modules - the root, `s3` and
`zstd` - and `make tools` installs the two that are not in the toolchain:

| Check | What it is for |
|---|---|
| `make fmt-check` | gofmt, in every module |
| `make lint` | golangci-lint, configured by `.golangci.yml` at the root |
| `make test` | `go test -race`, every module |
| `make vuln` | govulncheck, against the modules and the standard library |
| `make fuzz` | each fuzz target for `FUZZTIME`, 30s by default |
| `make tidy` | `go mod tidy` in each module |

Every package's `TestMain` verifies with
[goleak](https://github.com/uber-go/goleak) that no test left a goroutine
running: `Read`, `Write` and `Resize` start a worker per stored object and
must gather every one of them back, whether the work finished, a chunk
failed to decode, or the caller's context was cancelled. With `-race`
beside it, goleak says a goroutine outlived its call and the detector says
what it touched while it did.

govulncheck reports the standard library as well as the modules, so a Go
toolchain behind on its patch releases shows up as a finding of its own.
Each `go.mod` asks for `go1.25.13` by its `toolchain` line and CI pins the
same, so a build here and a build on a laptop are the same build; the
`toolchain` line is ignored in a module that is not the main one, so it
asks nothing of anyone who imports this. CI runs the scan weekly as well as
on every push, because a vulnerability is usually published long after the
code that has it was written.

The linters beyond the default set are chosen for what breaks rather than
for taste: `bodyclose`, `contextcheck`, `errorlint`, `gosec`, `makezero`,
`nilerr` and `noctx`, with gocritic held to its `diagnostic` tag.
`.golangci.yml` says why each exclusion is there.
