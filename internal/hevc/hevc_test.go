package hevc

import (
	"bytes"
	"errors"
	"testing"
)

// The parameter sets of ffmpeg's libx265 output for a 64x48 4:2:0 8-bit
// stream, and the hvcC header ffmpeg wrote for them.
var (
	vps = []byte{0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0x90, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x1e, 0x95, 0x98, 0x09}
	sps = []byte{0x42, 0x01, 0x01, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0x90, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x1e, 0xa0, 0x20, 0x83, 0x16, 0x59, 0x59, 0xae, 0x4c, 0xaf, 0x01, 0x68, 0x08, 0x00, 0x00, 0x03, 0x00, 0x08, 0x00, 0x00, 0x03, 0x00, 0x50, 0x40}
	pps = []byte{0x44, 0x01, 0xc1, 0x73, 0xd0, 0x89}
	// ffmpegHeader is the record's first 23 bytes.
	ffmpegHeader = []byte{0x01, 0x01, 0x60, 0x00, 0x00, 0x00, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1e, 0xf0, 0x00, 0xfc, 0xfd, 0xf8, 0xf8, 0x00, 0x00, 0x0f, 0x04}
)

func annexB(nals ...[]byte) []byte {
	var out []byte
	for _, nal := range nals {
		out = append(append(out, 0, 0, 0, 1), nal...)
	}
	return out
}

func TestConfigFromAnnexBMatchesFFmpeg(t *testing.T) {
	record, err := ConfigFromAnnexB(annexB(vps, sps, pps))
	if err != nil {
		t.Fatal(err)
	}
	// ffmpeg's record has a fourth array (an SEI); the header is the same
	// but for the array count.
	if !bytes.Equal(record[:22], ffmpegHeader[:22]) || record[22] != 3 {
		t.Fatalf("header % x\nwant   % x", record[:23], ffmpegHeader)
	}
	config, err := ParseConfig(record)
	if err != nil || config.LengthSize != 4 || len(config.ParameterSets) != 3 ||
		!bytes.Equal(config.ParameterSets[0], vps) || !bytes.Equal(config.ParameterSets[1], sps) || !bytes.Equal(config.ParameterSets[2], pps) {
		t.Fatalf("ParseConfig = %+v, %v", config, err)
	}
}

func TestConfigRefusesWhatIsMissingOrMalformed(t *testing.T) {
	for name, data := range map[string][]byte{
		"no pps":      annexB(vps, sps),
		"no vps":      annexB(sps, pps),
		"short sps":   annexB(vps, sps[:8], pps),
		"not annex b": vps,
	} {
		if _, err := ConfigFromAnnexB(data); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
	record, _ := ConfigFromAnnexB(annexB(vps, sps, pps))
	for name, data := range map[string][]byte{
		"short":         record[:20],
		"version 2":     append([]byte{2}, record[1:]...),
		"truncated nal": record[:len(record)-1],
		"length size 3": append(append(bytes.Clone(record[:21]), 0xfe), record[22:]...),
	} {
		if _, err := ParseConfig(data); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}
