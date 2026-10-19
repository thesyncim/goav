package hevc

import (
	"bytes"
	"encoding/hex"
	"slices"
	"testing"
)

// Parameter sets VideoToolbox wrote for a 64x48 HEVC Main stream.
var (
	testVPS = unhex("40010c01ffff016000000300b0000003000003001e0bc090")
	testSPS = unhex("420101016000000300b0000003000003001ea0142041c18c4e202fb916452ffcb9fc4fe880")
	testPPS = unhex("4401c072f05324")
)

func unhex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func annexB(nals ...[]byte) []byte {
	var out []byte
	for _, nal := range nals {
		out = append(append(out, 0, 0, 0, 1), nal...)
	}
	return out
}

func TestParseSPS(t *testing.T) {
	sps, err := ParseSPS(testSPS)
	if err != nil {
		t.Fatal(err)
	}
	want := SPS{TemporalLayers: 1, TemporalIDNested: 1, ChromaFormat: 1, LumaDepth: 8, ChromaDepth: 8, Width: 64, Height: 48}
	copy(want.ProfileTierLevel[:], unhex("0160000000b000000000001e"))
	if sps != want {
		t.Fatalf("SPS %+v, want %+v", sps, want)
	}
	for n := range 16 {
		if _, err := ParseSPS(testSPS[:n]); err == nil {
			t.Errorf("an SPS cut to %d bytes parsed", n)
		}
	}
	if _, err := ParseSPS(testPPS); err == nil {
		t.Error("a PPS parsed as an SPS")
	}
}

// TestConfigFromAnnexB pins the record built from in-band parameter sets:
// the SPS's profile, tier, level, chroma format, and depths, 4-byte lengths,
// and complete VPS, SPS, and PPS arrays that read back as they went in.
func TestConfigFromAnnexB(t *testing.T) {
	record, err := ConfigFromAnnexB(annexB(testVPS, testSPS, testPPS, []byte{0x26, 0x01, 0xaf}))
	if err != nil {
		t.Fatal(err)
	}
	header := unhex("01" + "0160000000b000000000001e" + "f000" + "fc" + "fd" + "f8" + "f8" + "0000" + "0f" + "03")
	if !bytes.Equal(record[:23], header) {
		t.Fatalf("header % x, want % x", record[:23], header)
	}
	config, err := ParseConfig(record)
	if err != nil || config.LengthSize != 4 || !slices.EqualFunc(config.ParameterSets, [][]byte{testVPS, testSPS, testPPS}, bytes.Equal) {
		t.Fatalf("ParseConfig = %+v, %v", config, err)
	}
	if _, err := ConfigFromAnnexB(annexB(testSPS, testPPS)); err == nil {
		t.Error("a record without a VPS was built")
	}
}

// TestCompleteConfig rewrites a record with 2-byte lengths and incomplete
// arrays for an hvc1 entry.
func TestCompleteConfig(t *testing.T) {
	record, _ := ConfigFromAnnexB(annexB(testVPS, testSPS, testPPS))
	record[21] = record[21]&^3 | 1
	for _, at := range []int{23, 23 + 5 + len(testVPS), 23 + 10 + len(testVPS) + len(testSPS)} {
		record[at] &^= 0x80
	}
	complete, err := CompleteConfig(record)
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := ConfigFromAnnexB(annexB(testVPS, testSPS, testPPS)); !bytes.Equal(complete, want) {
		t.Fatalf("complete % x, want % x", complete, want)
	}
	if _, err := CompleteConfig(record[:30]); err == nil {
		t.Error("a truncated record was completed")
	}
}

func FuzzConfig(f *testing.F) {
	f.Add(annexB(testVPS, testSPS, testPPS))
	record, _ := ConfigFromAnnexB(annexB(testVPS, testSPS, testPPS))
	f.Add(record)
	f.Fuzz(func(t *testing.T, data []byte) {
		if built, err := ConfigFromAnnexB(data); err == nil {
			if _, err := ParseConfig(built); err != nil {
				t.Fatalf("built a record ParseConfig refuses: %v", err)
			}
		}
		if complete, err := CompleteConfig(data); err == nil {
			if config, err := ParseConfig(complete); err != nil || config.LengthSize != 4 {
				t.Fatalf("completed a record ParseConfig refuses: %v", err)
			}
		}
		ParseSPS(data)
	})
}
