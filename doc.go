// Package zarr reads and writes Zarr version 3 stores: N-dimensional arrays
// cut into chunks, each chunk encoded and kept under its own key in a
// key-value store - a directory, an object store, or memory.
//
// It depends on nothing but the standard library, so that it can be carried
// into any program without bringing anything else with it.
//
// Of the core specification
// (https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html) it has:
//
//   - arrays and groups, with attributes and dimension names;
//   - the data types bool, int8 to int64, uint8 to uint64, float32 and float64;
//   - the regular chunk grid;
//   - the default and v2 chunk key encodings;
//   - the codecs bytes, gzip, crc32c and sharding_indexed.
//
// Past the core it has numcodecs.shuffle, which the specification does not
// have and zarr-python writes as zarr.codecs.numcodecs.Shuffle: ShuffleCodec
// lays bytes out by their place in an element for the compressor after it.
//
// Other codecs can be registered with RegisterCodec. zstd, which the
// standard library does not have, is in the module
// github.com/LukasSelin/zarr/zstd. Not yet here: transpose, float16,
// complex and raw data types, storage transformers.
//
// Arrays are read and written with the generic functions Read, Write,
// ReadChunk and WriteChunk, whose element type must be the array's data
// type. Elements are in C order: the last dimension varies fastest.
//
// An Array may be read from several goroutines at once, and written from
// several so long as no two write the same chunk - or, in a sharded array,
// the same shard. SetAttributes may not run
// beside anything else on the same node.
//
// What a store holds is not trusted. Metadata, chunks and shards that are
// malformed are an error, never a panic, and a store cannot make the package
// allocate more than the metadata implies: an array whose shape counts more
// elements than an int, or whose chunk - or shard, or shard index - is more
// than 2 GiB, does not open; a compressed chunk may not inflate past the
// bytes its elements take; and a shard's index must put every chunk inside
// the shard, and no two over each other.
package zarr
