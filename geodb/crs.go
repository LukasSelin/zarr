package geodb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
)

// Projection takes a longitude and latitude in degrees on WGS84 to the
// coordinates of a CRS.
type Projection func(lon, lat float64) (x, y float64)

var projections sync.Map // CRS name -> Projection

// RegisterProjection makes crs known, so that a tile can be on a grid in it.
// EPSG:4326 and the WGS84 UTM zones are known already; registering one of
// them replaces it.
func RegisterProjection(crs string, p Projection) {
	projections.Store(strings.ToUpper(crs), p)
}

// projectionTo is the projection from longitude and latitude to crs.
func projectionTo(crs string) (Projection, error) {
	name := strings.ToUpper(crs)
	if p, ok := projections.Load(name); ok {
		return p.(Projection), nil
	}
	if name == "EPSG:4326" || name == "OGC:CRS84" {
		return func(lon, lat float64) (float64, float64) { return lon, lat }, nil
	}
	if code, ok := strings.CutPrefix(name, "EPSG:"); ok {
		if n, err := strconv.Atoi(code); err == nil {
			switch {
			case n >= 32601 && n <= 32660:
				return utm(n-32600, false), nil
			case n >= 32701 && n <= 32760:
				return utm(n-32700, true), nil
			}
		}
	}
	return nil, fmt.Errorf("geodb: no projection to %q is known: register one with RegisterProjection", crs)
}

// WGS84, and the series of Krüger for the transverse Mercator projection to
// the third order in n, which is good to well under a millimetre within a
// UTM zone.
const (
	wgs84A = 6378137.0
	wgs84F = 1 / 298.257223563
	utmK0  = 0.9996
)

var (
	tmN     = wgs84F / (2 - wgs84F)
	tmA     = wgs84A / (1 + tmN) * (1 + tmN*tmN/4 + tmN*tmN*tmN*tmN/64)
	tmAlpha = [3]float64{
		tmN/2 - 2*tmN*tmN/3 + 5*tmN*tmN*tmN/16,
		13*tmN*tmN/48 - 3*tmN*tmN*tmN/5,
		61 * tmN * tmN * tmN / 240,
	}
	tmE = 2 * math.Sqrt(tmN) / (1 + tmN)
)

// utm is the projection to a UTM zone on WGS84, in metres.
func utm(zone int, south bool) Projection {
	lon0 := float64(zone*6-183) * math.Pi / 180
	n0 := 0.0
	if south {
		n0 = 10_000_000
	}
	return func(lon, lat float64) (float64, float64) {
		phi := lat * math.Pi / 180
		dl := lon*math.Pi/180 - lon0
		s := math.Sin(phi)
		t := math.Sinh(math.Atanh(s) - tmE*math.Atanh(tmE*s))
		xi := math.Atan2(t, math.Cos(dl))
		eta := math.Atanh(math.Sin(dl) / math.Sqrt(1+t*t))
		e, n := eta, xi
		for j, a := range tmAlpha {
			k := float64(2 * (j + 1))
			e += a * math.Cos(k*xi) * math.Sinh(k*eta)
			n += a * math.Sin(k*xi) * math.Cosh(k*eta)
		}
		return 500_000 + utmK0*tmA*e, n0 + utmK0*tmA*n
	}
}
