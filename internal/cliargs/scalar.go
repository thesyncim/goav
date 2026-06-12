package cliargs

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// FPS is a reduced positive frame-rate fraction.
type FPS struct {
	Num int
	Den int
}

func (f FPS) String() string {
	if f.Den == 1 {
		return strconv.Itoa(f.Num)
	}
	return fmt.Sprintf("%d/%d", f.Num, f.Den)
}

// ParseFPS accepts integer, decimal, or fraction frame-rate values.
func ParseFPS(value string) (FPS, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return FPS{}, fmt.Errorf("fps is required")
	}
	if left, right, ok := strings.Cut(value, "/"); ok {
		num, err := parsePositiveInt("fps numerator", left)
		if err != nil {
			return FPS{}, err
		}
		den, err := parsePositiveInt("fps denominator", right)
		if err != nil {
			return FPS{}, err
		}
		return reduceFPS(FPS{Num: num, Den: den}), nil
	}
	if strings.Contains(value, ".") {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || parsed <= 0 || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
			return FPS{}, fmt.Errorf("fps must be positive")
		}
		return reduceFPS(FPS{Num: int(math.Round(parsed * 1000)), Den: 1000}), nil
	}
	num, err := parsePositiveInt("fps", value)
	if err != nil {
		return FPS{}, err
	}
	return FPS{Num: num, Den: 1}, nil
}

// ParseRate accepts decimal bit rates with optional SI suffixes.
func ParseRate(value string) (int, error) {
	raw := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(value, "_", "")))
	if raw == "" {
		return 0, fmt.Errorf("rate is required")
	}
	multiplier := float64(1)
	for _, suffix := range []struct {
		text string
		mult float64
	}{
		{text: "gbps", mult: 1_000_000_000},
		{text: "mbps", mult: 1_000_000},
		{text: "kbps", mult: 1_000},
		{text: "bps", mult: 1},
		{text: "g", mult: 1_000_000_000},
		{text: "m", mult: 1_000_000},
		{text: "k", mult: 1_000},
	} {
		if strings.HasSuffix(raw, suffix.text) {
			raw = strings.TrimSuffix(raw, suffix.text)
			multiplier = suffix.mult
			break
		}
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil || parsed <= 0 || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return 0, fmt.Errorf("rate must be positive")
	}
	rate := parsed * multiplier
	if rate > float64(math.MaxInt) {
		return 0, fmt.Errorf("rate overflows int")
	}
	return int(math.Round(rate)), nil
}

func parsePositiveInt(name string, value string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return parsed, nil
}

func reduceFPS(fps FPS) FPS {
	divisor := gcd(fps.Num, fps.Den)
	if divisor <= 1 {
		return fps
	}
	return FPS{Num: fps.Num / divisor, Den: fps.Den / divisor}
}

func gcd(a int, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}
