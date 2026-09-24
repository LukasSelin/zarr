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
// Zarr version 2 is not read: opening a node that has a .zarray, .zgroup or
// .zattrs and no zarr.json is an error wrapping ErrZarrV2.
//
// Arrays are read and written with the generic functions Read, Write,
// ReadChunk and WriteChunk, whose element type must be the array's data
// type. Elements are in C order: the last dimension varies fastest.
//
// The keys of a store are walked with Store.List, a prefix at a time, and one
// level of them with ListDir, which a store that can do it cheaply - a
// directory, a bucket - answers in one request. Group.Children is the nodes
// directly in a group. Delete removes a node and everything under it, its
// metadata first, so that a delete that fails part way leaves keys that
// nothing opens rather than an array whose missing chunks read as the fill
// value.
//
// Read, Write, Resize and Append work on up to Array.Concurrency stored
// objects at once - sixteen by default - so that the round trip of an object
// store is waited out many keys at a time rather than one. A Store's methods
// must therefore bear being called from several goroutines at once, and each
// object in flight is held in memory; set Concurrency to 1 for one at a time.
//
// An Array may be read and written from several goroutines at once, through
// one handle or several, so long as no two write the same elements. Regions
// that share a chunk - or, in a sharded array, a shard - are safe: a Write
// reads what it covers only part of, patches it and writes it back, and it
// does so under a lock of that stored object's own. The lock is in this
// process and nowhere else: two processes, or two machines, writing regions
// that share a stored object may lose one of them, so split such work along
// the stored objects (Array.ChunkShape, or ShardShape if sharded).
// SetAttributes, Resize and Append may not run beside anything else on the
// same node.
//
// What a store holds is not trusted. Metadata, chunks and shards that are
// malformed are an error, never a panic, and a store cannot make the package
// allocate more than the metadata implies, times Array.Concurrency: an array
// whose shape counts more
// elements than an int, or whose chunk - or shard, or shard index - is more
// than 2 GiB, does not open; a compressed chunk may not inflate past the
// bytes its elements take; and a shard's index must put every chunk inside
// the shard, and no two over each other.
package zarr
