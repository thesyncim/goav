package h264

import (
	"encoding/hex"
	"errors"
	"testing"
)

// TestParseSPS reads parameter sets x264 wrote: 1080p High (cropped from
// 1088 rows) and interlaced 576i High 4:2:2.
func TestParseSPS(t *testing.T) {
	for _, tc := range []struct {
		hex           string
		profile, lvl  byte
		width, height int
	}{
		{"67640028acd940780227e5c044000003000400000300c83c60c658", 100, 40, 1920, 1080},
		{"677a001ebcd940b424d8088000000300800000190f8a14cb", 122, 30, 720, 576},
	} {
		nal, _ := hex.DecodeString(tc.hex)
		sps, err := ParseSPS(nal)
		if err != nil || sps != (SPS{Profile: tc.profile, Level: tc.lvl, Width: tc.width, Height: tc.height}) {
			t.Errorf("ParseSPS(%s) = %+v, %v; want %dx%d profile %d level %d", tc.hex, sps, err, tc.width, tc.height, tc.profile, tc.lvl)
		}
		// Cut before the size fields, the set is refused, never misread.
		for n := 1; n < 9; n++ {
			if _, err := ParseSPS(nal[:n]); !errors.Is(err, ErrInvalid) {
				t.Errorf("ParseSPS of %d of %d bytes: %v, want ErrInvalid", n, len(nal), err)
			}
		}
	}
	if _, err := ParseSPS([]byte{0x68, 1, 2, 3}); !errors.Is(err, ErrInvalid) {
		t.Errorf("a PPS: %v", err)
	}
}

func TestRBSP(t *testing.T) {
	in := []byte{1, 0, 0, 3, 1, 0, 0, 3, 0, 0, 3}
	if got := RBSP(in); string(got) != string([]byte{1, 0, 0, 1, 0, 0, 0, 0}) {
		t.Fatalf("RBSP = %v", got)
	}
	plain := []byte{1, 2, 0, 0, 1}
	if got := RBSP(plain); &got[0] != &plain[0] {
		t.Fatal("RBSP copied a payload without emulation prevention")
	}
}
