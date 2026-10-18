package h264

import (
	"bytes"
	"testing"
)

var (
	sps = []byte{0x67, 0x42, 0xc0, 0x1e, 0xda}
	pps = []byte{0x68, 0xce, 0x3c, 0x80}
	idr = []byte{0x65, 0x88, 0x84}
)

func annexB(nals ...[]byte) []byte {
	var out []byte
	for i, nal := range nals {
		if i == 0 {
			out = append(out, 0, 0, 0, 1)
		} else {
			out = append(out, 0, 0, 1)
		}
		out = append(out, nal...)
	}
	return out
}

func TestAppendAVCAndConfigFromAnnexB(t *testing.T) {
	unit := annexB(sps, pps, idr)
	avc, err := AppendAVC(nil, unit)
	if err != nil {
		t.Fatal(err)
	}
	back, err := AppendAnnexB(nil, avc, 4)
	if err != nil || !bytes.Equal(back, annexB4(sps, pps, idr)) {
		t.Fatalf("round trip = % x, %v", back, err)
	}
	record, err := ConfigFromAnnexB(unit)
	if err != nil {
		t.Fatal(err)
	}
	config, err := ParseConfig(record)
	if err != nil || config.LengthSize != 4 || !bytes.Equal(config.SPS[0], sps) || !bytes.Equal(config.PPS[0], pps) {
		t.Fatalf("config = %+v, %v", config, err)
	}
	if _, err := ConfigFromAnnexB(annexB(idr)); err == nil {
		t.Fatal("a unit without parameter sets made a config")
	}
	if _, err := AppendAVC(nil, []byte{1, 2, 3}); err == nil {
		t.Fatal("bytes without a start code converted")
	}
}

func annexB4(nals ...[]byte) []byte {
	var out []byte
	for _, nal := range nals {
		out = append(out, 0, 0, 0, 1)
		out = append(out, nal...)
	}
	return out
}
