package zarr

import "bytes"

// A scanner of JSON that reads what it can in one pass and leaves the rest
// to encoding/json: it validates as encoding/json does, but decodes nothing,
// finding where each member and element begins and ends.

// plainKey says whether the string between the quotes of a JSON string is
// what it says: printable ASCII with no escapes.
func plainKey(s []byte) bool {
	for _, c := range s {
		if c < 0x20 || c >= 0x80 || c == '\\' {
			return false
		}
	}
	return true
}

// maxDepth is how deep the scanner follows JSON before it leaves it to
// encoding/json, whose limit is deeper.
const maxDepth = 1000

// members reads the JSON object at b[i], calling fn with the key of each
// member, between its quotes and as written, and where its value begins.
// fn reads the value and is where it ends, or -1 to stop. members is where
// the object ends, or -1 if it is not valid, or fn stopped it. depth is how
// deep the object is.
func members(b []byte, i, depth int, fn func(key []byte, i int) int) int {
	if i >= len(b) || b[i] != '{' || depth > maxDepth {
		return -1
	}
	if i = skipSpace(b, i+1); i < len(b) && b[i] == '}' {
		return i + 1
	}
	for {
		end := skipString(b, i)
		if end < 0 {
			return -1
		}
		key := b[i+1 : end-1]
		if i = skipSpace(b, end); i >= len(b) || b[i] != ':' {
			return -1
		}
		if i = fn(key, skipSpace(b, i+1)); i < 0 {
			return -1
		}
		if i = skipSpace(b, i); i >= len(b) {
			return -1
		}
		switch b[i] {
		case '}':
			return i + 1
		case ',':
			i = skipSpace(b, i+1)
		default:
			return -1
		}
	}
}

// items reads the JSON array at b[i] as members reads an object, calling
// fn with where each element begins.
func items(b []byte, i, depth int, fn func(i int) int) int {
	if i >= len(b) || b[i] != '[' || depth > maxDepth {
		return -1
	}
	if i = skipSpace(b, i+1); i < len(b) && b[i] == ']' {
		return i + 1
	}
	for {
		if i = fn(i); i < 0 {
			return -1
		}
		if i = skipSpace(b, i); i >= len(b) {
			return -1
		}
		switch b[i] {
		case ']':
			return i + 1
		case ',':
			i = skipSpace(b, i+1)
		default:
			return -1
		}
	}
}

func skipSpace(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// skipValue is where the JSON value at b[i] ends, or -1 if it is not valid.
// depth is how deep the value is.
func skipValue(b []byte, i, depth int) int {
	if i >= len(b) {
		return -1
	}
	switch c := b[i]; {
	case c == '"':
		return skipString(b, i)
	case c == '{':
		return members(b, i, depth, func(_ []byte, i int) int { return skipValue(b, i, depth+1) })
	case c == '[':
		return items(b, i, depth, func(i int) int { return skipValue(b, i, depth+1) })
	case c == '-' || '0' <= c && c <= '9':
		return skipNumber(b, i)
	}
	for _, lit := range [...]string{"true", "false", "null"} {
		if bytes.HasPrefix(b[i:], []byte(lit)) {
			return i + len(lit)
		}
	}
	return -1
}

// skipString is where the JSON string at b[i] ends, or -1 if it is not
// valid.
func skipString(b []byte, i int) int {
	if i >= len(b) || b[i] != '"' {
		return -1
	}
	for i++; i < len(b); i++ {
		switch c := b[i]; {
		case c == '"':
			return i + 1
		case c < 0x20:
			return -1
		case c == '\\':
			if i++; i >= len(b) {
				return -1
			}
			switch b[i] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				if i+4 >= len(b) {
					return -1
				}
				for _, h := range b[i+1 : i+5] {
					if !('0' <= h && h <= '9' || 'a' <= h && h <= 'f' || 'A' <= h && h <= 'F') {
						return -1
					}
				}
				i += 4
			default:
				return -1
			}
		}
	}
	return -1
}

// skipNumber is where the JSON number at b[i] ends, or -1 if it is not
// valid.
func skipNumber(b []byte, i int) int {
	digits := func(i int) int {
		start := i
		for i < len(b) && '0' <= b[i] && b[i] <= '9' {
			i++
		}
		if i == start {
			return -1
		}
		return i
	}
	if b[i] == '-' {
		i++
	}
	if i < len(b) && b[i] == '0' {
		i++
	} else if i = digits(i); i < 0 {
		return -1
	}
	if i < len(b) && b[i] == '.' {
		if i = digits(i + 1); i < 0 {
			return -1
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		if i++; i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i = digits(i); i < 0 {
			return -1
		}
	}
	return i
}

// plainString is the JSON string raw, if it is one and plain.
func plainString(raw []byte) (string, bool) {
	if len(raw) < 2 || raw[0] != '"' || !plainKey(raw[1:len(raw)-1]) {
		return "", false
	}
	return string(raw[1 : len(raw)-1]), true
}
