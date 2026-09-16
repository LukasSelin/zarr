package zarr

import "context"

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
