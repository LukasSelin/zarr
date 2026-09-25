package zarr

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"strings"
)

// Group is a group in a store: a node that holds other nodes.
type Group struct {
	store Store
	path  string
	meta  GroupMetadata
}

// CreateGroup creates a group at path, "" being the root, which must not
// hold a node already. It does not create the groups above it.
func CreateGroup(ctx context.Context, s Store, path string, attrs map[string]any) (*Group, error) {
	if err := mustBeNew(ctx, s, path); err != nil {
		return nil, err
	}
	m := GroupMetadata{ZarrFormat: 3, NodeType: "group"}
	var err error
	if m.Attributes, err = withAttributes(nil, attrs); err != nil {
		return nil, err
	}
	if err := writeMetadata(ctx, s, path, m); err != nil {
		return nil, err
	}
	return &Group{store: s, path: path, meta: m}, nil
}

// OpenGroup opens the group at path.
func OpenGroup(ctx context.Context, s Store, path string) (*Group, error) {
	g := &Group{store: s, path: path}
	if err := readMetadata(ctx, s, path, "group", groupKeys, &g.meta); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Group) Path() string            { return g.path }
func (g *Group) Store() Store            { return g.store }
func (g *Group) Metadata() GroupMetadata { return g.meta }
func (g *Group) Attribute(name string, v any) (bool, error) {
	return attribute(g.meta.Attributes, name, v)
}

// SetAttributes lays attrs over the group's attributes and writes its
// metadata again.
func (g *Group) SetAttributes(ctx context.Context, attrs map[string]any) error {
	merged, err := withAttributes(g.meta.Attributes, attrs)
	if err != nil {
		return err
	}
	m := g.meta
	m.Attributes = merged
	if err := writeMetadata(ctx, g.store, g.path, m); err != nil {
		return err
	}
	g.meta = m
	return nil
}

// CreateArray creates an array called name in the group.
func (g *Group) CreateArray(ctx context.Context, name string, o ArrayOptions) (*Array, error) {
	return CreateArray(ctx, g.store, join(g.path, name), o)
}

// OpenArray opens the array called name in the group.
func (g *Group) OpenArray(ctx context.Context, name string) (*Array, error) {
	return OpenArray(ctx, g.store, join(g.path, name))
}

// CreateGroup creates a group called name in the group.
func (g *Group) CreateGroup(ctx context.Context, name string, attrs map[string]any) (*Group, error) {
	return CreateGroup(ctx, g.store, join(g.path, name), attrs)
}

// OpenGroup opens the group called name in the group.
func (g *Group) OpenGroup(ctx context.Context, name string) (*Group, error) {
	return OpenGroup(ctx, g.store, join(g.path, name))
}

// Child is a node directly in a group.
type Child struct {
	Name string
	// Type is what the metadata of the node says it is, "array" or "group",
	// and "" for metadata this package cannot read - which Delete removes
	// all the same.
	Type string
}

// Children is every node directly in the group, by name, sorted. It lists the
// one level under the group and reads the metadata of each name it finds, so
// it costs a listing and a read for each child rather than a walk of every
// key under the group; a name with no metadata under it - the chunks of an
// array, or a directory a delete left empty - is not a child. The reads go
// as many at once as the chunks of an array that says nothing about its
// concurrency, so that over a network a thousand children take the time of
// some sixty round trips rather than of a thousand.
func (g *Group) Children(ctx context.Context) ([]Child, error) {
	found, err := g.children(ctx)
	if err != nil || len(found) == 0 {
		return nil, err
	}
	children := make([]Child, len(found))
	for i, c := range found {
		children[i] = c.Child
	}
	return children, nil
}

// OpenArrays opens every array directly in the group, sorted by name, from
// the metadata Children reads: a listing and a read for each child, rather
// than the read again that OpenArray of each child would make. Groups, and
// children whose metadata this package cannot read, are left out; an array
// that does not open is an error, as it is from OpenArray.
func (g *Group) OpenArrays(ctx context.Context) ([]*Array, error) {
	found, err := g.children(ctx)
	if err != nil {
		return nil, err
	}
	var named []child
	for _, c := range found {
		if c.Type == "array" {
			named = append(named, c)
		}
	}
	if len(named) == 0 {
		return nil, nil
	}
	// Parsing is most of what is left, so it goes as many at once too.
	arrays := make([]*Array, len(named))
	err = eachSpan(ctx, min(defaultConcurrency, len(named)), []int{0}, []int{len(named) - 1},
		func(_ context.Context, n int, _ []int) error {
			path := join(g.path, named[n].Name)
			var m ArrayMetadata
			if err := parseMetadata(named[n].meta, path, "array", arrayKeys, &m); err != nil {
				return err
			}
			a, err := newArray(g.store, path, m)
			arrays[n] = a
			return err
		})
	if err != nil {
		return nil, err
	}
	return arrays, nil
}

// child is a Child and the metadata it was found by.
type child struct {
	Child
	meta []byte
}

// children is Children, with the metadata of each child.
func (g *Group) children(ctx context.Context) ([]child, error) {
	prefix := g.path
	if prefix != "" {
		prefix += "/"
	}
	var names []string
	err := ListDir(ctx, g.store, prefix, func(name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A node always has a zarr.json under it, so a child is always a
		// name with keys under it; the group's own zarr.json is not one.
		if n, ok := strings.CutSuffix(name, "/"); ok && checkPath(join(g.path, n)) == nil {
			names = append(names, n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names) // a store lists in no particular order
	// Each read goes into the slot of its name, so the order is kept and no
	// lock is needed; a slot left without metadata is a name with none.
	found := make([]child, len(names))
	err = eachSpan(ctx, min(defaultConcurrency, len(names)), []int{0}, []int{len(names) - 1},
		func(ctx context.Context, n int, _ []int) error {
			b, err := g.store.Get(ctx, metadataKey(join(g.path, names[n])))
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if b == nil {
				b = []byte{}
			}
			var head struct {
				NodeType string `json:"node_type"`
			}
			// Metadata that does not parse has no type, and is a child
			// anyway: finding it is how it is deleted.
			_ = json.Unmarshal(b, &head)
			found[n] = child{Child{Name: names[n], Type: head.NodeType}, b}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(found, func(c child) bool { return c.meta == nil }), nil
}
