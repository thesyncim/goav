package h264

import (
	"bytes"
	"errors"
	"testing"
)

// testRecord is an avcC with 4-byte lengths, one 3-byte SPS, one 2-byte PPS.
var testRecord = []byte{1, 0x64, 0, 0x1f, 0xff, 0xe1, 0, 3, 0x67, 0xaa, 0xbb, 1, 0, 2, 0x68, 0xcc}

func TestParseConfigAndParameterSets(t *testing.T) {
	config, err := ParseConfig(testRecord)
	if err != nil {
		t.Fatal(err)
	}
	if config.LengthSize != 4 || len(config.SPS) != 1 || len(config.PPS) != 1 {
		t.Fatalf("config = %+v", config)
	}
	got := config.AppendParameterSets(nil)
	want := []byte{0, 0, 0, 1, 0x67, 0xaa, 0xbb, 0, 0, 0, 1, 0x68, 0xcc}
	if !bytes.Equal(got, want) {
		t.Fatalf("parameter sets = % x, want % x", got, want)
	}
	for _, bad := range [][]byte{nil, {0, 1, 2, 3, 4, 5, 6}, testRecord[:9], {1, 0, 0, 0, 0xfe, 0xe0, 0}} {
		if _, err := ParseConfig(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ParseConfig(% x) err = %v, want ErrInvalid", bad, err)
		}
	}
}

func TestAppendAnnexBConvertsLengthPrefixes(t *testing.T) {
	for _, tt := range []struct {
		lengthSize int
		sample     []byte
	}{
		{4, []byte{0, 0, 0, 2, 0x65, 1, 0, 0, 0, 1, 0x41}},
		{2, []byte{0, 2, 0x65, 1, 0, 1, 0x41}},
		{1, []byte{2, 0x65, 1, 1, 0x41}},
	} {
		got, err := AppendAnnexB(nil, tt.sample, tt.lengthSize)
		if err != nil {
			t.Fatalf("length %d: %v", tt.lengthSize, err)
		}
		want := []byte{0, 0, 0, 1, 0x65, 1, 0, 0, 0, 1, 0x41}
		if !bytes.Equal(got, want) {
			t.Fatalf("length %d: got % x, want % x", tt.lengthSize, got, want)
		}
		if !ContainsNAL(got, NALIDR) || ContainsNAL(got, NALSPS) || !HasStartCode(got) {
			t.Fatalf("length %d: NAL scan wrong on % x", tt.lengthSize, got)
		}
	}
	for _, bad := range [][]byte{{0, 0, 0, 9, 1}, {0, 0, 0, 0}, {0, 0}} {
		if _, err := AppendAnnexB(nil, bad, 4); !errors.Is(err, ErrInvalid) {
			t.Fatalf("AppendAnnexB(% x) err = %v, want ErrInvalid", bad, err)
		}
	}
}
