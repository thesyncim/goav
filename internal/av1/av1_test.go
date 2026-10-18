package av1

import (
	"bytes"
	"errors"
	"testing"
)

var (
	sequenceHeaderOBU    = []byte{0x0a, 0x06, 0x19, 0x5d, 0xc3, 0xc3, 0xda, 0x44}
	temporalDelimiterOBU = []byte{0x12, 0x00}
)

func TestConfigFromFrames(t *testing.T) {
	config, err := ConfigFromFrames([][]byte{sequenceHeaderOBU})
	if err != nil {
		t.Fatal(err)
	}
	if want := append([]byte{0x81, 0x05, 0x10, 0x00}, sequenceHeaderOBU...); !bytes.Equal(config, want) {
		t.Fatalf("av1C = % x, want % x", config, want)
	}
	if _, err := ConfigFromFrames([][]byte{temporalDelimiterOBU}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("no sequence header: err = %v", err)
	}
}

func TestStripTemporalDelimiters(t *testing.T) {
	unit := append(append([]byte(nil), temporalDelimiterOBU...), sequenceHeaderOBU...)
	got, err := StripTemporalDelimiters(unit)
	if err != nil || !bytes.Equal(got, sequenceHeaderOBU) {
		t.Fatalf("stripped = % x, %v", got, err)
	}
	if got, _ := StripTemporalDelimiters(sequenceHeaderOBU); &got[0] != &sequenceHeaderOBU[0] {
		t.Fatal("a unit without delimiters was copied")
	}
}
