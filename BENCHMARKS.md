# Benchmarks

Results of the suite in `bench_meta_test.go`, `bench_raster_test.go` and
`zstd/bench_test.go`: what metadata and rasters cost, and what is worth
making faster.

## How they were run

```bash
go test -run '^$' -bench 'Meta|Raster|Codec' -count 3 -timeout 3h . > core.txt
(cd zstd && go test -run '^$' -bench . -count 3 .) > zstd.txt
```

The machine was a cloud VM: 4 vCPUs of an Intel Xeon at 2.10 GHz, Linux
amd64, Go 1.25.13. Each figure is the median of 3 runs. The store is memory
unless the row says `dir`. `conc=1` is one goroutine, which is the cost per
core. `default` is `Array.Concurrency` left at 0, which here means 4 cores.
A VM's numbers wander by about 10%, so read them as ratios rather than as
absolutes.

The raster is 2048 by 2048. `dem` is a smooth float32 field with a metre
of noise in its low bits, and `speckle` is radar-like backscatter. Neither
compresses much without shuffle, which is typical of real float rasters.

## Rasters

### Whole raster, float32, chunks of 512

| codec | read, 1 core | read, 4 | write, 1 core | write, 4 | ratio |
|---|---:|---:|---:|---:|---:|
| none | 2074 MB/s | 4008 MB/s | 2368 MB/s | 3813 MB/s | 1.00 |
| crc32c | 1885 | 3161 | 1581 | 2755 | 1.00 |
| gzip 1 | 108 | 360 | 156 | 415 | 1.16 |
| gzip 5 | 105 | 361 | 42 | 164 | 1.16 |
| gzip 9 | 109 | 379 | 44 | 156 | 1.16 |
| shuffle + gzip 1 | 257 | 785 | 181 | 590 | 1.72 |
| **shuffle + gzip 5** | **293** | **986** | 58 | 211 | **1.80** |
| zstd 1 (`zstd` module) | – | 1871 | – | 1624 | 1.00 |
| zstd 3 (`zstd` module) | – | 2062 | – | 1996 | 1.00 |

The rows `none` and `crc32c` are from after the bytes codec copied rather
than decoded element by element (item 5 below), the median of 5 runs. The
same runs measured the old code at 813 and 797 MB/s for a read on one core,
lower than the 1041 and 971 above, so the VM was about a fifth slower that
day and the gain is larger than the rows show against each other.

zstd did not compress this raster at all (a ratio of 1.00). It skips blocks
it finds incompressible, so its speed here is close to that of `none` and
does not predict its speed on data it does compress. zstd behind shuffle
needs a tag of the core with `ShuffleCodec`, which the `zstd` module does
not require yet.

The data type (gzip 5, one core) makes no difference to MB/s: reads run at
100–107 MB/s and writes at 39–46 MB/s for float32, float64, uint16 and
int16. uint8 reads faster (256 MB/s) only because it compresses 7.4 times.
The chunk size from 128 to 1024 changes nothing either, within 5%, and
neither does sharding (1024 shards of 256 chunks).

### Where a read goes

The profile of an uncompressed read, one core:

| | |
|---|---:|
| `memmove` + `memclr` (copies: store → decode → `out`, and new buffers cleared) | 90% |
| of which the bytes codec (`BytesCodec.DecodeArray`: allocate, clear, copy) | 28% |

A gzip 5 read spends 62% of its time in `compress/flate`, and the bytes
codec and the copies take about 15% more.

A whole read allocates about 4 times the raster: 65 MB for a 16 MB float32
raster.

### Windows (float32, gzip 5)

| window | chunk 512 | chunk 256 | chunk 512, no codec |
|---|---:|---:|---:|
| one pixel | 9.1 ms | 2.3 ms | 0.9 ms |
| 3×3 across a chunk corner | 12.2 ms | 3.0 ms | 1.5 ms |
| 256² aligned | 9.2 ms | 2.4 ms | 0.9 ms |
| 256² across four chunks | 12.6 ms | 3.1 ms | 1.4 ms |
| a row of 2048 | 11.1 ms | 5.4 ms | 1.5 ms |
| 1024² aligned | 12.6 ms | 11.9 ms | 2.1 ms |

Every window decodes whole chunks: a pixel costs a whole chunk, and a 3×3
window across a corner costs four.

### Writing tile by tile (float32, gzip 5, one tile at a time)

| layout | tile = chunk or larger | tile = half a chunk |
|---|---:|---:|
| chunks of 512 | 11.0 Mcells/s (512 tiles) | 3.2 Mcells/s (256 tiles) |
| shards of 1024, chunks of 256 | 3.3 Mcells/s (512 tiles) | 0.9 Mcells/s (256 tiles) |

A tile smaller than its stored object reads that object, decodes it, patches
it, re-encodes all of it and writes it back. In a shard that is every chunk
of the shard, each time.

## Metadata

| operation | memory | dir | round trips |
|---|---:|---:|---:|
| `OpenArray`, no attributes | 26 µs | 32 µs | 1 get |
| `OpenArray`, 50 attributes | 160 µs | 170 µs | 1 get |
| `OpenArray`, sharded, no attributes | 55 µs | 63 µs | 1 get |
| `OpenGroup`, no attributes / 50 | 2.9 / 130 µs | 7.4 / 140 µs | 1 get |
| `CreateArray`, no attributes | 18 µs | 0.2 ms | 1 get, 1 set |
| `SetAttributes`, on 50 | 64 µs | 140 µs | 1 set |
| `Attribute("crs_wkt")` | 3.4 µs | | none |
| open something that is not there | 1.4 µs | | 4 gets |
| `Children`, 10 / 100 / 1000 | 0.1 / 0.9 / 10 ms | 0.2 / 1.6 / 16 ms | 1 list + 1 get each |
| a group and its 10 / 100 arrays opened | 0.6 / 5.0 ms | 0.7 / 6.6 ms | 21 / 201 gets |

`readMetadata` unmarshals each `zarr.json` three times: into a map of fields,
into its version and node type, and into the metadata itself. That is why
attributes cost so much.

## What to make faster

Ranked by what they would save for rasters and a catalogue read from an
object store.

1. **Read the children of a group concurrently.** `Children` gets each
   child's `zarr.json` one after another. From an object store at about
   25 ms a round trip, 100 children take 2.5 s and 1000 take 25 s. With 16
   in flight, as `Array.Concurrency` does for chunks, that becomes about
   0.2 s and 1.6 s.
2. **Do not read a child's metadata twice.** Opening a group and its
   arrays gets every `zarr.json` twice (2N+1 gets), once in `Children` and
   once in `OpenArray`. Opening the child from the bytes `Children` already
   read halves the round trips.
3. **Align tile writes with the stored objects.** Writing tiles of half a
   chunk is 3.4 times slower than writing whole chunks, and 12 times slower
   into shards. The engine writing into Zarr should hand over whole chunks,
   or whole shards when the array is sharded. In the library, a shard patched
   by chunk could keep the encoded bytes of the chunks it does not touch
   rather than decode and re-encode all of them.
4. **Prefer shuffle for float rasters.** shuffle + gzip 5 reads 2.8 times
   faster than gzip 5 and stores 1.8 times smaller, against 1.16. gzip 1
   writes 3.7 times faster than gzip 5, with the same ratio. gzip 5 and 9
   buy nothing on float data. This is a choice of default and needs no
   code.
5. **Decode the bytes codec with a copy.** Done: bytes in the machine's
   order are copied straight into or out of the element slice, and bytes
   in the other order are reversed a word at a time. Element-by-element
   decoding was 55% of an uncompressed read and capped it at about 1 GB/s
   per core; the bytes codec now runs at about 3 GB/s in the machine's
   order and 2.3 GB/s in the other, and an uncompressed read at 2 GB/s.
6. **Copy less on a read.** With item 5 done, the store's copy, the
   decoded chunk and the copy into `out` are 90% of an uncompressed read
   and 4 times the raster in allocations. A chunk that covers its part of `out` exactly could be
   decoded straight into it.
7. **Cache decoded chunks for windowed reads.** A pixel costs a whole
   chunk (9 ms at 512, gzip), and a focal window across a chunk corner
   costs four. An LRU of decoded chunks in the adapter, like the block cache
   of strata's `cog`, would cut that to one decode per chunk. Chunks of 256
   make small windows 4 times cheaper and cost nothing on whole reads.
8. **Parse `zarr.json` once.** Unmarshalling it once rather than three
   times would make `OpenArray` about 3 times cheaper. That matters only
   when opening hundreds of arrays, after items 1 and 2.

Smaller things:

- A chunk that was never written is filled and then copied. It could be
  filled straight into `out`; sparse reads run at 2 GB/s.
- `CRC32CCodec.EncodeBytes` copies the whole chunk to append 4 bytes. It
  now copies it once, into a buffer with room for the checksum: 1 MB
  allocated for a 1 MB chunk rather than 2.3 MB. It cannot append in place,
  as the bytes it is given may be the caller's.
- Opening something that is not there costs 4 gets, because it looks for
  version 2 metadata too. An adapter that checks whether an array exists
  pays all 4 over the network.
