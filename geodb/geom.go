package geodb

import (
	"errors"
	"fmt"
	"math"
)

// Point is a position: longitude and latitude for parcels and weather, the
// tile's own coordinates for a tile.
type Point struct{ X, Y float64 }

// BBox is an axis-aligned box, closed on every side.
type BBox struct{ MinX, MinY, MaxX, MaxY float64 }

// emptyBBox is a box that holds nothing and grows to whatever is added to it.
func emptyBBox() BBox {
	return BBox{math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)}
}

// Valid is whether b is finite and its minimum is not past its maximum.
func (b BBox) Valid() bool {
	return finite(b.MinX) && finite(b.MinY) && finite(b.MaxX) && finite(b.MaxY) &&
		b.MinX <= b.MaxX && b.MinY <= b.MaxY
}

// Intersects is whether b and o share a point.
func (b BBox) Intersects(o BBox) bool {
	return b.MinX <= o.MaxX && o.MinX <= b.MaxX && b.MinY <= o.MaxY && o.MinY <= b.MaxY
}

// Contains is whether p is in b.
func (b BBox) Contains(p Point) bool {
	return p.X >= b.MinX && p.X <= b.MaxX && p.Y >= b.MinY && p.Y <= b.MaxY
}

func (b BBox) extend(p Point) BBox {
	return BBox{min(b.MinX, p.X), min(b.MinY, p.Y), max(b.MaxX, p.X), max(b.MaxY, p.Y)}
}

func (b BBox) union(o BBox) BBox {
	return BBox{min(b.MinX, o.MinX), min(b.MinY, o.MinY), max(b.MaxX, o.MaxX), max(b.MaxY, o.MaxY)}
}

func (b BBox) center() Point { return Point{(b.MinX + b.MaxX) / 2, (b.MinY + b.MaxY) / 2} }

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// Grid is a raster that is north up: Width by Height pixels, Dx by Dy each,
// in the coordinates of CRS. Column 0 is at the west and row 0 at the north:
// pixel (col, row) covers X0+col*Dx to X0+(col+1)*Dx, and Y0-(row+1)*Dy to
// Y0-row*Dy.
type Grid struct {
	CRS    string  `json:"crs"`
	X0     float64 `json:"x0"` // the west edge of column 0
	Y0     float64 `json:"y0"` // the north edge of row 0
	Dx     float64 `json:"dx"`
	Dy     float64 `json:"dy"`
	Width  int     `json:"width"`
	Height int     `json:"height"`
}

func (g Grid) validate() error {
	switch {
	case g.CRS == "":
		return errors.New("geodb: a grid needs a CRS")
	case !(g.Dx > 0) || !(g.Dy > 0) || !finite(g.Dx) || !finite(g.Dy):
		return fmt.Errorf("geodb: pixel size %g by %g: both must be positive", g.Dx, g.Dy)
	case !finite(g.X0) || !finite(g.Y0):
		return errors.New("geodb: the origin of a grid must be finite")
	case g.Width <= 0 || g.Height <= 0:
		return fmt.Errorf("geodb: a grid of %d by %d pixels", g.Width, g.Height)
	case int64(g.Width)*int64(g.Height) > math.MaxInt32:
		return fmt.Errorf("geodb: a grid of %d by %d pixels is too many", g.Width, g.Height)
	}
	if _, err := projectionTo(g.CRS); err != nil {
		return err
	}
	return nil
}

// Bounds is the box the grid covers.
func (g Grid) Bounds() BBox {
	return BBox{g.X0, g.Y0 - float64(g.Height)*g.Dy, g.X0 + float64(g.Width)*g.Dx, g.Y0}
}

// Pixel is the column and row of the pixel p is in, and whether there is
// one.
func (g Grid) Pixel(p Point) (col, row int, ok bool) {
	c := math.Floor((p.X - g.X0) / g.Dx)
	r := math.Floor((g.Y0 - p.Y) / g.Dy)
	if !(c >= 0 && c < float64(g.Width) && r >= 0 && r < float64(g.Height)) {
		return 0, 0, false
	}
	return int(c), int(r), true
}

// Center is the centre of pixel (col, row).
func (g Grid) Center(col, row int) Point {
	return Point{g.X0 + (float64(col)+0.5)*g.Dx, g.Y0 - (float64(row)+0.5)*g.Dy}
}

// Window is a block of pixels: columns Col to Col+Width, rows Row to
// Row+Height.
type Window struct {
	Col, Row, Width, Height int
}

// Window is the pixels of g whose centres are in b, and whether there are
// any.
func (g Grid) Window(b BBox) (Window, bool) {
	if !b.Valid() || !b.Intersects(g.Bounds()) {
		return Window{}, false
	}
	c0 := int(math.Max(0, math.Ceil((b.MinX-g.X0)/g.Dx-0.5)))
	c1 := int(math.Min(float64(g.Width-1), math.Floor((b.MaxX-g.X0)/g.Dx-0.5)))
	r0 := int(math.Max(0, math.Ceil((g.Y0-b.MaxY)/g.Dy-0.5)))
	r1 := int(math.Min(float64(g.Height-1), math.Floor((g.Y0-b.MinY)/g.Dy-0.5)))
	if c0 > c1 || r0 > r1 {
		return Window{}, false
	}
	return Window{Col: c0, Row: r0, Width: c1 - c0 + 1, Height: r1 - r0 + 1}, true
}
