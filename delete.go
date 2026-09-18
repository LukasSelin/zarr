package zarr

import (
	"context"
	"errors"
)

// Delete removes the node at path - the root being "" - and everything under
// it: its metadata first, then the rest, so that a Delete that fails part way
// leaves keys that nothing opens rather than an array whose chunks are gone
// and which reads as the fill value. Doing it again clears what was left. It
// does not read the metadata, so a node this package cannot open goes too. A
// path holding no node is not an error, and a node kept under one goes with
// it: deleting a group deletes its arrays.
//
// A DirStore keeps the directories, so what was an array is an empty tree of
// them afterwards; nothing reads them, and List does not yield them.
func Delete(ctx context.Context, s Store, path string) error {
	if err := checkPath(path); err != nil {
		return err
	}
	if err := s.Delete(ctx, metadataKey(path)); err != nil {
		return err
	}
	prefix := path
	if prefix != "" {
		prefix += "/"
	}
	return deletePrefix(ctx, s, prefix)
}

// errListFull stops a listing that has as many keys as one round deletes.
var errListFull = errors.New("zarr: enough keys")

// deletePrefix deletes every key under prefix, a batch at a time and listing
// again for the next: a store need not answer a List whose keys are being
// deleted under it, and a batch is small enough to hold and large enough that
// the listing is not the cost. The keys of the last round are gone, so the
// next listing begins where it left off.
func deletePrefix(ctx context.Context, s Store, prefix string) error {
	const batch = 1000
	for {
		keys := make([]string, 0, batch)
		err := s.List(ctx, prefix, func(key string) error {
			if keys = append(keys, key); len(keys) == batch {
				return errListFull
			}
			return nil
		})
		if err != nil && !errors.Is(err, errListFull) {
			return err
		}
		for _, key := range keys {
			if err := s.Delete(ctx, key); err != nil {
				return err
			}
		}
		if len(keys) < batch {
			return nil
		}
	}
}

// Delete removes the array: its metadata, and then its chunks or shards,
// whatever key encoding they are under. See the package-level Delete for the
// order it does it in. The handle is no use afterwards - a Write through it
// writes chunks under a path that has no metadata.
func (a *Array) Delete(ctx context.Context) error { return Delete(ctx, a.store, a.path) }

// Delete removes the group and every node under it. See the package-level
// Delete for the order it does it in. The root group, at "", is the whole
// store.
func (g *Group) Delete(ctx context.Context) error { return Delete(ctx, g.store, g.path) }
