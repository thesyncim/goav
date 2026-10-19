package aac

import (
	"bytes"
	"testing"
)

func TestParseConfig(t *testing.T) {
	for _, tc := range []struct {
		config         []byte
		rate, channels int
		ok             bool
	}{
		{[]byte{0x11, 0x90}, 48000, 2, true},                   // AAC-LC 48 kHz stereo
		{[]byte{0x12, 0x08}, 44100, 1, true},                   // AAC-LC 44.1 kHz mono
		{[]byte{0x13, 0x88}, 22050, 1, true},                   // index 7
		{[]byte{0x17, 0x80, 0x5d, 0xc0, 0x10}, 48000, 2, true}, // an explicit 24-bit rate
	} {
		rate, channels, ok := ParseConfig(tc.config)
		if rate != tc.rate || channels != tc.channels || ok != tc.ok {
			t.Errorf("ParseConfig(% x) = %d %d %v, want %d %d %v", tc.config, rate, channels, ok, tc.rate, tc.channels, tc.ok)
		}
	}
	for _, bad := range [][]byte{nil, {0x11}, {0x16, 0x80}, {0xf8, 0x00}} {
		if _, _, ok := ParseConfig(bad); ok {
			t.Errorf("ParseConfig(% x) ok", bad)
		}
	}
}

func TestADTS(t *testing.T) {
	// AAC-LC, 48 kHz (index 3), stereo, no CRC.
	frame := []byte{0xff, 0xf1, 0x4c, 0x80, 0x01, 0x3f, 0xfc, 0xde, 0xad}
	header, config, ok := ADTS(frame)
	if !ok || header != 7 || !bytes.Equal(config, []byte{0x11, 0x90}) {
		t.Fatalf("ADTS = %d % x %v", header, config, ok)
	}
	if _, _, ok := ADTS([]byte{0x11, 0x90, 0, 0, 0, 0, 0}); ok {
		t.Fatal("raw AAC read as ADTS")
	}
}
