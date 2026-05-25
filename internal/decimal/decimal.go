// Package decimal provides fast fixed-point arithmetic with 10^8 precision.
// All values are int64 scaled by 1e8, zero heap allocation per operation.
package decimal

import (
	"math/big"
	"strconv"
	"strings"
)

const (
	Precision = 8
	Scale     = 1_0000_0000 // 10^8
)

// D is a fixed-point decimal number: actual value = raw / 10^8.
type D int64

// New creates a D from an unscaled int64 (raw = val * 10^8).
func New(raw int64) D { return D(raw) }

// FromString parses a decimal string, returning (0, false) on failure.
func FromString(s string) (D, bool) {
	if s == "" {
		return 0, false
	}
	neg := false
	rest := s
	if rest[0] == '-' {
		neg = true
		rest = rest[1:]
	} else if rest[0] == '+' {
		rest = rest[1:]
	}

	var intP, fracP int64
	dot := false
	fracLen := 0

	for _, c := range rest {
		if c == '.' {
			if dot {
				return 0, false
			}
			dot = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		d := int64(c - '0')
		if !dot {
			intP = intP*10 + d
		} else if fracLen < Precision {
			fracP = fracP*10 + d
			fracLen++
		}
	}
	for fracLen < Precision {
		fracP *= 10
		fracLen++
	}

	v := intP*Scale + fracP
	if neg {
		v = -v
	}
	return D(v), true
}

// MustFromString panics on failure.
func MustFromString(s string) D {
	d, ok := FromString(s)
	if !ok {
		panic("decimal: parse " + s)
	}
	return d
}

const int64Max = 1<<63 - 1

// Add returns a+b. Panics on overflow.
func (a D) Add(b D) D {
	sum := int64(a) + int64(b)
	if (sum^int64(a)) >= 0 || (sum^int64(b)) >= 0 {
		return D(sum)
	}
	if int64(a) > 0 {
		return D(int64Max)
	}
	return D(-int64Max - 1)
}

// Sub returns a-b.
func (a D) Sub(b D) D { return D(int64(a) - int64(b)) }

// Mul returns a*b / Scale. Uses big.Int for safety with large values.
func (a D) Mul(b D) D {
	va, vb := int64(a), int64(b)
	// Check if direct int64 multiplication could overflow.
	// Safe range: |va| <= 1<<31 && |vb| <= 1<<31
	if va > 1<<31 || va < -(1<<31) || vb > 1<<31 || vb < -(1<<31) {
		prod := new(big.Int).Mul(big.NewInt(va), big.NewInt(vb))
		prod.Div(prod, big.NewInt(Scale))
		return D(prod.Int64())
	}
	return D((va * vb) / Scale)
}

// Quo returns a*Scale / b. Panics on divide by zero.
func (a D) Quo(b D) D {
	if b == 0 {
		panic("decimal: division by zero")
	}
	return D((int64(a) * Scale) / int64(b))
}

// Cmp returns -1, 0, or 1.
func (a D) Cmp(b D) int {
	va, vb := int64(a), int64(b)
	if va < vb {
		return -1
	}
	if va > vb {
		return 1
	}
	return 0
}

// Sign returns -1, 0, or 1.
func (a D) Sign() int {
	v := int64(a)
	if v < 0 {
		return -1
	}
	if v > 0 {
		return 1
	}
	return 0
}

// String formats with 8 decimal places.
func (a D) String() string {
	v := int64(a)
	if v == 0 {
		return "0.00000000"
	}
	neg := false
	if v < 0 {
		neg = true
		v = -v
	}
	intP := v / Scale
	fracP := v % Scale
	s := strconv.FormatInt(intP, 10) + "." + pad(fracP)
	if neg {
		s = "-" + s
	}
	return s
}

// Trimmed formats without trailing zeros.
func (a D) Trimmed() string {
	s := a.String()
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		for len(s) > i+1 && s[len(s)-1] == '0' {
			s = s[:len(s)-1]
		}
		if s[len(s)-1] == '.' {
			s = s[:len(s)-1]
		}
	}
	return s
}

// Int64 returns the raw unscaled value.
func (a D) Int64() int64 { return int64(a) }

func pad(v int64) string {
	s := strconv.FormatInt(v, 10)
	for len(s) < Precision {
		s = "0" + s
	}
	return s
}
