package geozarr

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// CRS is the coordinate reference system that map coordinates are in. Set
// at least one field. EPSG is the code, and WKT2 and PROJJSON are the full
// definition, which a reader that has no EPSG database can still use.
type CRS struct {
	EPSG     int
	WKT2     string
	PROJJSON json.RawMessage
}

// IsZero reports whether the CRS has none of its fields set.
func (c CRS) IsZero() bool { return c.EPSG == 0 && c.WKT2 == "" && len(c.PROJJSON) == 0 }

// Code is the CRS as "EPSG:4326", or "" when it has no EPSG code.
func (c CRS) Code() string {
	if c.EPSG == 0 {
		return ""
	}
	return "EPSG:" + strconv.Itoa(c.EPSG)
}

// attributes are the CRS under the proj: convention and as GDAL's _CRS.
func (c CRS) attributes(attrs map[string]any) {
	if c.IsZero() {
		return
	}
	gdal := map[string]any{}
	if c.EPSG != 0 {
		attrs["proj:code"] = c.Code()
		gdal["url"] = "http://www.opengis.net/def/crs/EPSG/0/" + strconv.Itoa(c.EPSG)
	}
	if c.WKT2 != "" {
		attrs["proj:wkt2"] = c.WKT2
		gdal["wkt"] = c.WKT2
	}
	if len(c.PROJJSON) != 0 {
		attrs["proj:projjson"] = c.PROJJSON
		gdal["projjson"] = c.PROJJSON
	}
	attrs["_CRS"] = gdal
}

// crsFrom reads a CRS back from the attributes attributes wrote, or from
// GDAL's _CRS alone.
func crsFrom(attr func(string, any) (bool, error)) (CRS, error) {
	var c CRS
	var code string
	if ok, err := attr("proj:code", &code); err != nil {
		return c, err
	} else if ok {
		n, err := parseEPSG(code)
		if err != nil {
			return c, err
		}
		c.EPSG = n
	}
	if _, err := attr("proj:wkt2", &c.WKT2); err != nil {
		return c, err
	}
	if _, err := attr("proj:projjson", &c.PROJJSON); err != nil {
		return c, err
	}
	if !c.IsZero() {
		return c, nil
	}
	var gdal struct {
		URL      string          `json:"url"`
		WKT      string          `json:"wkt"`
		PROJJSON json.RawMessage `json:"projjson"`
	}
	if ok, err := attr("_CRS", &gdal); err != nil || !ok {
		return c, err
	}
	c.WKT2, c.PROJJSON = gdal.WKT, gdal.PROJJSON
	if i := strings.LastIndex(gdal.URL, "/EPSG/0/"); i >= 0 {
		n, err := strconv.Atoi(gdal.URL[i+len("/EPSG/0/"):])
		if err != nil || n <= 0 {
			return c, fmt.Errorf("geozarr: _CRS url %q is not an EPSG code", gdal.URL)
		}
		c.EPSG = n
	}
	return c, nil
}

func parseEPSG(code string) (int, error) {
	rest, ok := strings.CutPrefix(strings.ToUpper(code), "EPSG:")
	if !ok {
		return 0, fmt.Errorf("geozarr: proj:code %q is not EPSG:<n>", code)
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("geozarr: proj:code %q is not EPSG:<n>", code)
	}
	return n, nil
}

// Transform takes a pixel's column and row to map coordinates, as the affine
// transform of rasterio and of the spatial: convention does:
//
//	x = A*col + B*row + C
//	y = D*col + E*row + F
//
// (col, row) is a pixel's top left corner, (0.5, 0.5) the first pixel's
// centre. A north-up raster has B and D zero and E negative.
type Transform struct{ A, B, C, D, E, F float64 }

// NorthUp is the transform of a raster with its top left corner at (x, y)
// and pixels dx wide and dy tall, dy positive.
func NorthUp(x, y, dx, dy float64) Transform {
	return Transform{A: dx, C: x, E: -dy, F: y}
}

// FromGDAL is a transform from GDAL's GeoTransform order:
// x0, dx, row rotation, y0, column rotation, dy.
func FromGDAL(g [6]float64) Transform {
	return Transform{A: g[1], B: g[2], C: g[0], D: g[4], E: g[5], F: g[3]}
}

// GDAL is the transform in GDAL's GeoTransform order.
func (t Transform) GDAL() [6]float64 { return [6]float64{t.C, t.A, t.B, t.F, t.D, t.E} }

// Apply is the map coordinates of (col, row).
func (t Transform) Apply(col, row float64) (x, y float64) {
	return t.A*col + t.B*row + t.C, t.D*col + t.E*row + t.F
}

// Invert is the pixel position of (x, y), fractional: the pixel it is in is
// the floor of each.
func (t Transform) Invert(x, y float64) (col, row float64) {
	det := t.A*t.E - t.B*t.D
	x, y = x-t.C, y-t.F
	return (t.E*x - t.B*y) / det, (t.A*y - t.D*x) / det
}

// rotated reports whether the transform's axes are not the raster's.
func (t Transform) rotated() bool { return t.B != 0 || t.D != 0 }

func (t Transform) valid() error {
	det := t.A*t.E - t.B*t.D
	for _, v := range []float64{t.A, t.B, t.C, t.D, t.E, t.F} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return errors.New("geozarr: transform is not finite")
		}
	}
	if det == 0 {
		return errors.New("geozarr: transform is singular")
	}
	return nil
}

// shift is the transform of the window whose top left pixel is (col, row).
func (t Transform) shift(col, row int) Transform {
	t.C, t.F = t.Apply(float64(col), float64(row))
	return t
}

// BBox is a box in map coordinates.
type BBox struct{ MinX, MinY, MaxX, MaxY float64 }

// Intersects reports whether b and o share any point, edges included.
func (b BBox) Intersects(o BBox) bool {
	return b.MinX <= o.MaxX && o.MinX <= b.MaxX && b.MinY <= o.MaxY && o.MinY <= b.MaxY
}

// Contains reports whether (x, y) is in b, edges included.
func (b BBox) Contains(x, y float64) bool {
	return b.MinX <= x && x <= b.MaxX && b.MinY <= y && y <= b.MaxY
}

func (b BBox) valid() error {
	for _, v := range []float64{b.MinX, b.MinY, b.MaxX, b.MaxY} {
		if math.IsNaN(v) {
			return errors.New("geozarr: bounding box has NaN in it")
		}
	}
	if b.MinX > b.MaxX || b.MinY > b.MaxY {
		return fmt.Errorf("geozarr: bounding box %v has its minimum past its maximum", b)
	}
	return nil
}

// Window is a rectangle of pixels: Height rows from Row and Width columns
// from Col.
type Window struct{ Row, Col, Height, Width int }

// Empty reports whether the window holds no pixels.
func (w Window) Empty() bool { return w.Height <= 0 || w.Width <= 0 }

// bounds is the bounding box of the pixels [col0, col1) by [row0, row1).
func (t Transform) bounds(col0, row0, col1, row1 float64) BBox {
	b := BBox{math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)}
	for _, p := range [4][2]float64{{col0, row0}, {col1, row0}, {col0, row1}, {col1, row1}} {
		x, y := t.Apply(p[0], p[1])
		b.MinX, b.MaxX = math.Min(b.MinX, x), math.Max(b.MaxX, x)
		b.MinY, b.MaxY = math.Min(b.MinY, y), math.Max(b.MaxY, y)
	}
	return b
}

// window is the smallest window of a height by width raster that holds every
// pixel b touches, clipped to the raster; empty if b misses it.
func (t Transform) window(b BBox, height, width int) Window {
	c0, r0 := math.Inf(1), math.Inf(1)
	c1, r1 := math.Inf(-1), math.Inf(-1)
	for _, p := range [4][2]float64{{b.MinX, b.MinY}, {b.MaxX, b.MinY}, {b.MinX, b.MaxY}, {b.MaxX, b.MaxY}} {
		c, r := t.Invert(p[0], p[1])
		c0, c1 = math.Min(c0, c), math.Max(c1, c)
		r0, r1 = math.Min(r0, r), math.Max(r1, r)
	}
	clip := func(v float64, n int) int { return int(math.Max(0, math.Min(float64(n), v))) }
	// A box edge that falls exactly on a pixel edge does not take in the
	// pixel past it, unless the box is that thin.
	col0, row0 := clip(math.Floor(c0), width), clip(math.Floor(r0), height)
	col1, row1 := clip(math.Ceil(c1), width), clip(math.Ceil(r1), height)
	if col1 == col0 && c0 >= 0 && c0 < float64(width) {
		col1 = col0 + 1
	}
	if row1 == row0 && r0 >= 0 && r0 < float64(height) {
		row1 = row0 + 1
	}
	return Window{Row: row0, Col: col0, Height: row1 - row0, Width: col1 - col0}
}
