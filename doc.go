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
//   - the codecs bytes, gzip and crc32c.
//
// Other codecs can be registered with RegisterCodec - zstd, for one, which
// the standard library does not have. Not yet here: sharding, transpose,
// float16, complex and raw data types, storage transformers.
//
// Arrays are read and written with the generic functions Read, Write,
// ReadChunk and WriteChunk, whose element type must be the array's data
// type. Elements are in C order: the last dimension varies fastest.
//
// An Array may be read from several goroutines at once, and written from
// several so long as no two write the same chunk. SetAttributes may not run
// beside anything else on the same node.
package zarr
