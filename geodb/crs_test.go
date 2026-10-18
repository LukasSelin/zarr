package geodb

import (
	"math"
	"testing"
)

// TestUTMIsWhatPROJGives against points projected by pyproj 3 (PROJ 9).
func TestUTMIsWhatPROJGives(t *testing.T) {
	for _, c := range []struct {
		lon, lat float64
		crs      string
		x, y     float64
	}{
		{18.0686, 59.3293, "EPSG:32634", 333230.0612, 6580391.3234}, // Stockholm
		{13.0, 55.6, "EPSG:32633", 373988.3716, 6163377.1164},
		{15.0, 0.0, "EPSG:32633", 500000.0000, 0.0000},
		{18.0, 0.0, "EPSG:32633", 833978.5569, 0.0000},
		{-70.6, -33.45, "EPSG:32719", 351290.3713, 6297679.5101},
		{24.9, 60.2, "EPSG:32635", 383587.7703, 6675536.6454},
		{11.2, 63.0, "EPSG:32632", 611419.5293, 6987495.4996},
		{4.9, 52.37, "epsg:32631", 629351.9095, 5803890.8038},
	} {
		p, err := projectionTo(c.crs)
		if err != nil {
			t.Fatal(err)
		}
		x, y := p(c.lon, c.lat)
		if math.Abs(x-c.x) > 1e-3 || math.Abs(y-c.y) > 1e-3 {
			t.Errorf("(%g, %g) in %s = (%.4f, %.4f), want (%.4f, %.4f)", c.lon, c.lat, c.crs, x, y, c.x, c.y)
		}
	}
}

func TestAProjectionThatIsNotKnownIsAnError(t *testing.T) {
	for _, crs := range []string{"EPSG:3035", "EPSG:32600", "EPSG:32661", "ESRI:54009", ""} {
		if _, err := projectionTo(crs); err == nil {
			t.Errorf("%q is known", crs)
		}
	}
	RegisterProjection("EPSG:3035", func(lon, lat float64) (float64, float64) { return lon * 2, lat * 2 })
	t.Cleanup(func() { projections.Delete("EPSG:3035") })
	p, err := projectionTo("epsg:3035")
	if err != nil {
		t.Fatal(err)
	}
	if x, y := p(1, 2); x != 2 || y != 4 {
		t.Errorf("the registered projection gives (%g, %g)", x, y)
	}
}
