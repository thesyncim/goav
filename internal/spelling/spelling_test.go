package spelling

import "testing"

func TestQuantity(t *testing.T) {
	for in, want := range map[string]float64{"96k": 96_000, "2M": 2_000_000, "1.5M": 1_500_000, "1G": 1e9, "-3": -3, "800": 800} {
		if got, ok := Quantity(in); !ok || got != want {
			t.Errorf("Quantity(%q) = %v, %v; want %v", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "1.5", "2x", "Inf", "NaN", "1e30", "M"} {
		if got, ok := Quantity(in); ok {
			t.Errorf("Quantity(%q) = %v, want a refusal", in, got)
		}
	}
}

func TestBool(t *testing.T) {
	for in, want := range map[string]bool{"true": true, "ON": true, "1": true, "no": false, "off": false} {
		if got, ok := Bool(in); !ok || got != want {
			t.Errorf("Bool(%q) = %v, %v", in, got, ok)
		}
	}
	if _, ok := Bool("maybe"); ok {
		t.Error("Bool(maybe) accepted")
	}
}

func TestNearest(t *testing.T) {
	choices := []string{"decode", "encode", "resize"}
	for in, want := range map[string]string{"decod": "decode", "RESIZ": "resize", "transcode": ""} {
		if got := Nearest(in, choices); got != want {
			t.Errorf("Nearest(%q) = %q, want %q", in, got, want)
		}
	}
}
