package geodb

import (
	"errors"
	"fmt"
	"math"
	"regexp"

	"github.com/LukasSelin/zarr"
)

// Variable is a product kept in a tile's scenes, NDVI for instance. Its
// values are stored as int16: a value v is round((v-Offset)/Scale), and
// NaN, which is a pixel the cloud mask threw out, is the fill value.
type Variable struct {
	Name   string  `json:"name"`
	Scale  float64 `json:"scale"`  // 1e-4 holds NDVI to four places
	Offset float64 `json:"offset"` // added after scaling
	// Composite is how a pixel's valid values in a period are reduced to
	// one: "median" (the default), "max" or "mean". Max suits NDVI, which
	// cloud and haze only ever push down.
	Composite string `json:"composite,omitempty"`
}

// fill16 is what a pixel with no valid value is stored as.
const fill16 = math.MinInt16

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

func validName(kind, name string) error {
	if !namePattern.MatchString(name) {
		return fmt.Errorf("geodb: %s name %q: letters, digits, _ . and -, starting with a letter or digit", kind, name)
	}
	return nil
}

func (v *Variable) validate() error {
	if err := validName("variable", v.Name); err != nil {
		return err
	}
	switch v.Name {
	case "time", "parcel", "npix", "span_offset", "spans":
		return fmt.Errorf("geodb: %q is a name geodb keeps for itself", v.Name)
	}
	if !(v.Scale > 0) || !finite(v.Scale) || !finite(v.Offset) {
		return fmt.Errorf("geodb: variable %s: the scale must be positive and the offset finite", v.Name)
	}
	switch v.Composite {
	case "":
		v.Composite = "median"
	case "median", "max", "mean":
	default:
		return fmt.Errorf("geodb: variable %s: no composite %q: median, max or mean", v.Name, v.Composite)
	}
	return nil
}

// encode is v as stored: NaN and infinities are the fill, and values past
// what int16 holds are held to its ends.
func (v Variable) encode(f float32) int16 {
	if !finite(float64(f)) {
		return fill16
	}
	r := math.Round((float64(f) - v.Offset) / v.Scale)
	return int16(math.Max(-math.MaxInt16, math.Min(math.MaxInt16, r)))
}

func (v Variable) decode(i int16) float32 {
	if i == fill16 {
		return float32(math.NaN())
	}
	return float32(float64(i)*v.Scale + v.Offset)
}

// cfAttributes are what xarray decodes an int16 array of v by.
func (v Variable) cfAttributes() map[string]any {
	return map[string]any{
		"scale_factor": v.Scale,
		"add_offset":   v.Offset,
		"_FillValue":   fill16,
	}
}

// codecs is shuffle and gzip, which in this repository's benchmarks does
// more than any other codec of the core for rasters of numbers.
func codecs(t zarr.DataType) []zarr.Codec {
	return []zarr.Codec{
		zarr.BytesCodec{Endian: zarr.Little},
		zarr.ShuffleCodec{ElementSize: t.Size()},
		zarr.GzipCodec{Level: 5},
	}
}

// ErrNotFound is what asking for a parcel, tile, weather source, variable
// or scene that is not there wraps.
var ErrNotFound = errors.New("geodb: not found")

// isNotFound is whether err is a node or key that is not there.
func isNotFound(err error) bool { return errors.Is(err, zarr.ErrNotFound) }

// joinPath joins the parts of a path in a store.
func joinPath(parts ...string) string {
	out := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		if out != "" {
			out += "/"
		}
		out += p
	}
	return out
}
