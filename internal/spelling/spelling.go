// Package spelling reads the text spellings of values that pipeline programs
// and control calls share — 2M, true/on, and did-you-mean hints — so a value
// means the same thing wherever an operator types it.
package spelling

import (
	"math"
	"strconv"
	"strings"
)

// Quantity reads a whole number with an optional SI suffix: 96k, 2M, 1G.
func Quantity(s string) (float64, bool) {
	mult := 1.0
	switch {
	case strings.HasSuffix(s, "k") || strings.HasSuffix(s, "K"):
		mult, s = 1e3, s[:len(s)-1]
	case strings.HasSuffix(s, "M"):
		mult, s = 1e6, s[:len(s)-1]
	case strings.HasSuffix(s, "G"):
		mult, s = 1e9, s[:len(s)-1]
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
		return 0, false
	}
	n *= mult
	if n != math.Trunc(n) || math.Abs(n) > math.MaxInt64 {
		return 0, false
	}
	return n, true
}

// Bool reads true, yes, on, 1 and false, no, off, 0.
func Bool(s string) (value, ok bool) {
	switch strings.ToLower(s) {
	case "true", "yes", "1", "on":
		return true, true
	case "false", "no", "0", "off":
		return false, true
	}
	return false, false
}

// Nearest returns the choice within two edits of name, ignoring case, or "".
func Nearest(name string, choices []string) string {
	best, bestDist := "", 3
	for _, choice := range choices {
		if d := distance(strings.ToLower(name), strings.ToLower(choice)); d < bestDist {
			best, bestDist = choice, d
		}
	}
	return best
}

func distance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
