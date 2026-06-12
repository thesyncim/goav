package cliargs

import "testing"

func TestParseRate(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{value: "1200k", want: 1_200_000},
		{value: "1.5M", want: 1_500_000},
		{value: "2mbps", want: 2_000_000},
		{value: "300_000", want: 300_000},
		{value: "1g", want: 1_000_000_000},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got, err := ParseRate(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("ParseRate(%q) = %d, want %d", tc.value, got, tc.want)
			}
		})
	}
}

func TestParseRateRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "nope"} {
		t.Run(value, func(t *testing.T) {
			if got, err := ParseRate(value); err == nil || got != 0 {
				t.Fatalf("ParseRate(%q) = %d, %v; want error", value, got, err)
			}
		})
	}
}

func TestParseFPS(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  FPS
		label string
	}{
		{value: "30", want: FPS{Num: 30, Den: 1}, label: "30"},
		{value: "29.97", want: FPS{Num: 2997, Den: 100}, label: "2997/100"},
		{value: "60000/2002", want: FPS{Num: 30000, Den: 1001}, label: "30000/1001"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got, err := ParseFPS(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want || got.String() != tc.label {
				t.Fatalf("ParseFPS(%q) = %+v (%s), want %+v (%s)", tc.value, got, got.String(), tc.want, tc.label)
			}
		})
	}
}

func TestParseFPSRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "30/0", "nope"} {
		t.Run(value, func(t *testing.T) {
			if got, err := ParseFPS(value); err == nil || got != (FPS{}) {
				t.Fatalf("ParseFPS(%q) = %+v, %v; want error", value, got, err)
			}
		})
	}
}
