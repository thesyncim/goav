package spelling

import (
	"math"
	"reflect"
	"strconv"
	"time"
)

var durationType = reflect.TypeOf(time.Duration(0))

// SetScalar parses text into a scalar reflect.Value (time.Duration, string,
// bool, signed/unsigned integers with SI suffixes, or finite float64). It
// returns ("", true) when dst was set, (want, false) when text failed
// validation for a supported scalar type, or ("", false) when dst's type is
// not handled here.
func SetScalar(dst reflect.Value, text string) (want string, ok bool) {
	if dst.Type() == durationType {
		d, err := time.ParseDuration(text)
		if err != nil {
			return "a duration like 2s or 20ms", false
		}
		dst.SetInt(int64(d))
		return "", true
	}
	switch dst.Kind() {
	case reflect.String:
		dst.SetString(text)
		return "", true
	case reflect.Bool:
		b, valid := Bool(text)
		if !valid {
			return "true or false", false
		}
		dst.SetBool(b)
		return "", true
	case reflect.Int, reflect.Int64:
		n, valid := Quantity(text)
		if !valid {
			return "a whole number (k and M multiply by 1000)", false
		}
		dst.SetInt(int64(n))
		return "", true
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		n, valid := Quantity(text)
		if !valid || n < 0 || dst.OverflowUint(uint64(n)) {
			return "a non-negative whole number", false
		}
		dst.SetUint(uint64(n))
		return "", true
	case reflect.Float64:
		n, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return "a number", false
		}
		dst.SetFloat(n)
		return "", true
	}
	return "", false
}
