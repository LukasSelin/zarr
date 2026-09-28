package zarr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// DataType is the type of an array's elements, as named in its metadata.
type DataType string

const (
	Bool    DataType = "bool"
	Int8    DataType = "int8"
	Int16   DataType = "int16"
	Int32   DataType = "int32"
	Int64   DataType = "int64"
	Uint8   DataType = "uint8"
	Uint16  DataType = "uint16"
	Uint32  DataType = "uint32"
	Uint64  DataType = "uint64"
	Float32 DataType = "float32"
	Float64 DataType = "float64"
)

// Element is the Go types an array's elements can be read and written as.
type Element interface {
	bool | int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// Size is how many bytes an element of the type takes, or 0 for a type this
// package does not know.
func (d DataType) Size() int {
	switch d {
	case Bool, Int8, Uint8:
		return 1
	case Int16, Uint16:
		return 2
	case Int32, Uint32, Float32:
		return 4
	case Int64, Uint64, Float64:
		return 8
	}
	return 0
}

func (d DataType) float() bool { return d == Float32 || d == Float64 }

func (d DataType) signed() bool {
	return d == Int8 || d == Int16 || d == Int32 || d == Int64
}

// DataTypeOf is the data type of an array whose elements are T.
func DataTypeOf[T Element]() DataType { return dataTypeOf[T]() }

func dataTypeOf[T Element]() DataType {
	var z T
	switch any(z).(type) {
	case bool:
		return Bool
	case int8:
		return Int8
	case int16:
		return Int16
	case int32:
		return Int32
	case int64:
		return Int64
	case uint8:
		return Uint8
	case uint16:
		return Uint16
	case uint32:
		return Uint32
	case uint64:
		return Uint64
	case float32:
		return Float32
	}
	return Float64
}

// makeSlice is n zero elements of the type, as a []T in an any.
func makeSlice(d DataType, n int) any {
	switch d {
	case Bool:
		return make([]bool, n)
	case Int8:
		return make([]int8, n)
	case Int16:
		return make([]int16, n)
	case Int32:
		return make([]int32, n)
	case Int64:
		return make([]int64, n)
	case Uint8:
		return make([]uint8, n)
	case Uint16:
		return make([]uint16, n)
	case Uint32:
		return make([]uint32, n)
	case Uint64:
		return make([]uint64, n)
	case Float32:
		return make([]float32, n)
	case Float64:
		return make([]float64, n)
	}
	return nil
}

// bytesOf is the memory of s, a []T of a data type other than bool, as bytes,
// and the size of an element. The bytes are s's own, in the machine's order:
// a write to one is a write to the other. A bool is left out because a byte
// other than 0 or 1 written to one is not a valid bool.
func bytesOf(s any) ([]byte, int) {
	switch s := s.(type) {
	case []int8:
		return rawBytes(s)
	case []int16:
		return rawBytes(s)
	case []int32:
		return rawBytes(s)
	case []int64:
		return rawBytes(s)
	case []uint8:
		return s, 1
	case []uint16:
		return rawBytes(s)
	case []uint32:
		return rawBytes(s)
	case []uint64:
		return rawBytes(s)
	case []float32:
		return rawBytes(s)
	case []float64:
		return rawBytes(s)
	}
	return nil, 0
}

func rawBytes[T int8 | int16 | int32 | int64 | uint16 | uint32 | uint64 | float32 | float64](s []T) ([]byte, int) {
	size := int(unsafe.Sizeof(*new(T)))
	// Every bit pattern is a valid T, and a T is aligned at least as a byte
	// is, so its memory may be read and written as bytes.
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(s))), len(s)*size), size
}

func goType(d DataType) reflect.Type { return reflect.TypeOf(makeSlice(d, 0)).Elem() }

// fillFrom is v as an element of the type: nil is zero, and a Go number of
// another type is taken if the type holds it exactly.
func fillFrom(d DataType, v any) (any, error) {
	out := reflect.New(goType(d)).Elem()
	if v == nil {
		return out.Interface(), nil
	}
	in := reflect.ValueOf(v)
	bad := fmt.Errorf("zarr: fill value %v (%T) is not a %s", v, v, d)
	switch in.Kind() {
	case reflect.Bool:
		if d != Bool {
			return nil, bad
		}
		out.SetBool(in.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		x := in.Int()
		switch {
		case d.signed() && !out.OverflowInt(x):
			out.SetInt(x)
		case d.float():
			out.SetFloat(float64(x))
		case d != Bool && !d.signed() && x >= 0 && !out.OverflowUint(uint64(x)):
			out.SetUint(uint64(x))
		default:
			return nil, bad
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		x := in.Uint()
		switch {
		case d.signed() && x <= math.MaxInt64 && !out.OverflowInt(int64(x)):
			out.SetInt(int64(x))
		case d.float():
			out.SetFloat(float64(x))
		case d != Bool && !d.signed() && !out.OverflowUint(x):
			out.SetUint(x)
		default:
			return nil, bad
		}
	case reflect.Float32, reflect.Float64:
		f := in.Float()
		switch {
		case d.float():
			out.SetFloat(f)
		case f != math.Trunc(f):
			return nil, bad
		case d.signed() && f >= math.MinInt64 && f < math.MaxInt64 && !out.OverflowInt(int64(f)):
			out.SetInt(int64(f))
		case d != Bool && !d.signed() && f >= 0 && f < math.MaxUint64 && !out.OverflowUint(uint64(f)):
			out.SetUint(uint64(f))
		default:
			return nil, bad
		}
	default:
		return nil, bad
	}
	return out.Interface(), nil
}

// parseFill reads a fill_value from metadata. Floats may be a number,
// "NaN", "Infinity", "-Infinity", or the bits of one as "0x" and hex digits.
func parseFill(d DataType, raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, fmt.Errorf("zarr: an array must have a fill_value")
	}
	bad := func(err error) error { return fmt.Errorf("zarr: fill_value %s is not a %s: %w", raw, d, err) }
	bits := d.Size() * 8
	out := reflect.New(goType(d)).Elem()
	switch {
	case d == Bool:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return nil, bad(err)
		}
		out.SetBool(b)
	case d.float():
		var f float64
		if raw[0] == '"' {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, bad(err)
			}
			switch {
			case s == "NaN":
				f = math.NaN()
			case s == "Infinity":
				f = math.Inf(1)
			case s == "-Infinity":
				f = math.Inf(-1)
			case strings.HasPrefix(s, "0x") && len(s) == 2+bits/4:
				u, err := strconv.ParseUint(s[2:], 16, bits)
				if err != nil {
					return nil, bad(err)
				}
				if d == Float32 {
					return math.Float32frombits(uint32(u)), nil
				}
				return math.Float64frombits(u), nil
			default:
				return nil, bad(fmt.Errorf("unknown value"))
			}
		} else {
			var err error
			if f, err = strconv.ParseFloat(string(raw), bits); err != nil {
				return nil, bad(err)
			}
		}
		out.SetFloat(f)
	case d.signed():
		x, err := strconv.ParseInt(string(raw), 10, bits)
		if err != nil {
			return nil, bad(err)
		}
		out.SetInt(x)
	default:
		x, err := strconv.ParseUint(string(raw), 10, bits)
		if err != nil {
			return nil, bad(err)
		}
		out.SetUint(x)
	}
	return out.Interface(), nil
}

// formatFill writes a fill value as metadata keeps it.
func formatFill(v any) json.RawMessage {
	var s string
	switch x := v.(type) {
	case bool:
		s = strconv.FormatBool(x)
	case int8, int16, int32, int64:
		s = strconv.FormatInt(reflect.ValueOf(x).Int(), 10)
	case uint8, uint16, uint32, uint64:
		s = strconv.FormatUint(reflect.ValueOf(x).Uint(), 10)
	case float32:
		s = formatFloat(float64(x), 32)
	case float64:
		s = formatFloat(x, 64)
	}
	return json.RawMessage(s)
}

func formatFloat(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return `"NaN"`
	case math.IsInf(f, 1):
		return `"Infinity"`
	case math.IsInf(f, -1):
		return `"-Infinity"`
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}
