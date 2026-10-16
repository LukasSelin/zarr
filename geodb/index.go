package geodb

import "math"

// hilbert is the distance along a Hilbert curve of order 16 of the point
// (x, y), each in [0, 65535].
func hilbert(x, y uint32) uint64 {
	const n = 1 << 16
	var d uint64
	for s := uint32(n / 2); s > 0; s /= 2 {
		var rx, ry uint32
		if x&s != 0 {
			rx = 1
		}
		if y&s != 0 {
			ry = 1
		}
		d += uint64(s) * uint64(s) * uint64((3*rx)^ry)
		if ry == 0 {
			if rx == 1 {
				x, y = n-1-x, n-1-y
			}
			x, y = y, x
		}
	}
	return d
}

// hilbertKeys are the Hilbert distances of the centres of boxes, over the
// box that holds them all.
func hilbertKeys(boxes []BBox) []uint64 {
	all := emptyBBox()
	for _, b := range boxes {
		all = all.union(b)
	}
	w, h := all.MaxX-all.MinX, all.MaxY-all.MinY
	scale := func(v, lo, span float64) uint32 {
		if span <= 0 {
			return 0
		}
		return uint32(math.Min(65535, math.Max(0, (v-lo)/span*65535)))
	}
	keys := make([]uint64, len(boxes))
	for i, b := range boxes {
		c := b.center()
		keys[i] = hilbert(scale(c.X, all.MinX, w), scale(c.Y, all.MinY, h))
	}
	return keys
}

// rtree is a packed R-tree over boxes in the order they are given, which is
// best when they are sorted along a Hilbert curve: each node holds the
// next nodeSize boxes of the level under it.
type rtree struct {
	levels [][]BBox // levels[0] is the items; the last is the root
}

const nodeSize = 16

func newRTree(items []BBox) *rtree {
	t := &rtree{levels: [][]BBox{items}}
	for level := items; len(level) > 1; {
		up := make([]BBox, 0, (len(level)+nodeSize-1)/nodeSize)
		for i := 0; i < len(level); i += nodeSize {
			b := emptyBBox()
			for _, c := range level[i:min(i+nodeSize, len(level))] {
				b = b.union(c)
			}
			up = append(up, b)
		}
		t.levels = append(t.levels, up)
		level = up
	}
	return t
}

// search calls fn with the index of every item that intersects q, in the
// order of the items.
func (t *rtree) search(q BBox, fn func(i int)) {
	if len(t.levels[0]) == 0 {
		return
	}
	var walk func(level, node int)
	walk = func(level, node int) {
		if !t.levels[level][node].Intersects(q) {
			return
		}
		if level == 0 {
			fn(node)
			return
		}
		below := len(t.levels[level-1])
		for c := node * nodeSize; c < min((node+1)*nodeSize, below); c++ {
			walk(level-1, c)
		}
	}
	walk(len(t.levels)-1, 0)
}
