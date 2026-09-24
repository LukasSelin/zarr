package zarr

import (
	"context"
	"fmt"
	"slices"
)

// fillPastEnd is data, the stored object at sidx, with what of it is past the
// end of the array set to the fill value: in data itself if owned, and in a
// copy otherwise.
func fillPastEnd[T Element](a *Array, sidx []int, data []T, owned bool) []T {
	origin := times(sidx, a.grid)
	for k := range a.grid {
		in := max(a.meta.Shape[k]-origin[k], 0)
		if in >= a.grid[k] {
			continue
		}
		if !owned {
			data, owned = slices.Clone(data), true
		}
		at, n := make([]int, len(a.grid)), slices.Clone(a.grid)
		at[k], n[k] = in, a.grid[k]-in
		copyBlock(data, a.grid, at, filled(product(n), a.fill.(T)), n, make([]int, len(n)), n)
	}
	return data
}

// withShape is a copy of the array with shape in place of its own, its
// metadata not yet written.
func (a *Array) withShape(shape []int) (*Array, error) {
	if len(shape) != len(a.meta.Shape) {
		return nil, fmt.Errorf("zarr: shape %v for an array of %d dimensions", shape, len(a.meta.Shape))
	}
	m := a.meta
	m.Shape = slices.Clone(shape)
	b, err := newArray(a.store, a.path, m)
	if err != nil {
		return nil, err
	}
	b.WriteEmptyChunks, b.Concurrency = a.WriteEmptyChunks, a.Concurrency
	return b, nil
}

// numStored is how many stored objects - chunks, or shards - the array is cut
// into along each dimension.
func (a *Array) numStored() []int {
	n := make([]int, len(a.grid))
	for k := range n {
		n[k] = (a.meta.Shape[k] + a.grid[k] - 1) / a.grid[k]
	}
	return n
}

// Resize sets the shape of the array and writes its metadata again.
//
// Growing writes nothing but the metadata: what is past the old end reads
// as the fill value. Shrinking first deletes the chunks, or shards, wholly
// past the new end, and writes the ones the new end cuts through with fill
// past it, and only then writes the metadata. A Resize that fails part way
// may have filled some of what was being cut off, in no particular order;
// the shape is still the old one.
//
// Another handle on the array keeps the shape it had until Refresh. Nothing
// stops two writers resizing or appending to one array at once, and one of
// them would lose.
func (a *Array) Resize(ctx context.Context, shape []int) error {
	b, err := a.withShape(shape)
	if err != nil {
		return err
	}
	if err := resizeStored(ctx, a, b); err != nil {
		return err
	}
	if err := writeMetadata(ctx, a.store, a.path, b.meta); err != nil {
		return err
	}
	*a = *b
	return nil
}

// resizeStored deletes or refills the stored objects of a, at its old shape,
// that b, the same array at a new shape, cuts off.
func resizeStored(ctx context.Context, a, b *Array) error {
	shrinks := false
	for k := range a.meta.Shape {
		shrinks = shrinks || b.meta.Shape[k] < a.meta.Shape[k]
	}
	n := a.numStored()
	if !shrinks || product(n) == 0 {
		return nil
	}
	// Each stored object is deleted, refilled or left alone on its own, so
	// they go in as many goroutines as the array allows.
	lo, hi := make([]int, len(n)), minusOne(n)
	return eachSpan(ctx, a.limit(spanLen(lo, hi)), lo, hi, func(ctx context.Context, _ int, sidx []int) error {
		origin := times(sidx, a.grid)
		gone, cut := false, false
		for k := range origin {
			gone = gone || origin[k] >= b.meta.Shape[k]
			cut = cut || origin[k]+a.grid[k] > b.meta.Shape[k] && b.meta.Shape[k] < a.meta.Shape[k]
		}
		switch {
		case gone:
			return a.store.Delete(ctx, a.storedKey(sidx))
		case cut:
			return refill(ctx, a, b, sidx)
		}
		return nil
	})
}

// refill reads the stored object at sidx and writes it back, which fills what
// of it is past the end of b.
func refill(ctx context.Context, a, b *Array, sidx []int) error {
	switch a.meta.DataType {
	case Bool:
		return refillAs[bool](ctx, a, b, sidx)
	case Int8:
		return refillAs[int8](ctx, a, b, sidx)
	case Int16:
		return refillAs[int16](ctx, a, b, sidx)
	case Int32:
		return refillAs[int32](ctx, a, b, sidx)
	case Int64:
		return refillAs[int64](ctx, a, b, sidx)
	case Uint8:
		return refillAs[uint8](ctx, a, b, sidx)
	case Uint16:
		return refillAs[uint16](ctx, a, b, sidx)
	case Uint32:
		return refillAs[uint32](ctx, a, b, sidx)
	case Uint64:
		return refillAs[uint64](ctx, a, b, sidx)
	case Float32:
		return refillAs[float32](ctx, a, b, sidx)
	case Float64:
		return refillAs[float64](ctx, a, b, sidx)
	}
	return fmt.Errorf("%w: data type %q", ErrUnsupported, a.meta.DataType)
}

func refillAs[T Element](ctx context.Context, a, b *Array, sidx []int) error {
	return a.exclusive(ctx, sidx, func() error {
		buf, err := readStored[T](ctx, a, sidx)
		if err != nil {
			return err
		}
		return writeStored(ctx, b, sidx, buf, true)
	})
}

// Append writes data after the end of the array along axis, and grows the
// array to hold it. data is in C order, of the array's shape but for axis,
// along which it is as long as it needs to be. It returns where along axis
// data begins.
//
// The chunks are written before the metadata, so an Append that fails part
// way leaves the shape as it was and nothing of data can be read; doing it
// again writes over what it left. An Append is not idempotent: to write a
// time step that may already be there, compare its index with Shape and
// Write it if it is.
//
// Another handle on the array keeps the shape it had until Refresh, and an
// Append through it would write over what this one appended.
func Append[T Element](ctx context.Context, a *Array, axis int, data []T) (int, error) {
	if err := checkType[T](a); err != nil {
		return 0, err
	}
	shape := a.Shape()
	if axis < 0 || axis >= len(shape) {
		return 0, fmt.Errorf("zarr: append along axis %d of an array of %d dimensions", axis, len(shape))
	}
	across := 1
	for k, v := range shape {
		if k != axis {
			across *= v
		}
	}
	if across == 0 {
		return 0, fmt.Errorf("zarr: append along axis %d of an array of shape %v, which holds nothing across it", axis, shape)
	}
	if len(data)%across != 0 {
		return 0, fmt.Errorf("zarr: %d elements to append are not a whole number of slices of %d", len(data), across)
	}
	at, n := shape[axis], len(data)/across
	if n == 0 {
		return at, nil
	}
	if at > int(^uint(0)>>1)-n {
		return 0, fmt.Errorf("zarr: appending %d to %d along axis %d is more than an int can count", n, at, axis)
	}
	shape[axis] = at + n
	b, err := a.withShape(shape)
	if err != nil {
		return 0, err
	}
	start, region := make([]int, len(shape)), slices.Clone(shape)
	start[axis], region[axis] = at, n
	if err := Write(ctx, b, start, region, data); err != nil {
		return 0, err
	}
	if err := writeMetadata(ctx, a.store, a.path, b.meta); err != nil {
		return 0, err
	}
	*a = *b
	return at, nil
}

// Refresh reads the array's metadata again: its shape, attributes and all,
// as another handle on the array may have written them. WriteEmptyChunks and
// Concurrency are kept.
func (a *Array) Refresh(ctx context.Context) error {
	b, err := OpenArray(ctx, a.store, a.path)
	if err != nil {
		return err
	}
	b.WriteEmptyChunks, b.Concurrency = a.WriteEmptyChunks, a.Concurrency
	*a = *b
	return nil
}
