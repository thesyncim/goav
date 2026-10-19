package bitio

import (
	"errors"
	"testing"
)

func TestReader(t *testing.T) {
	short := errors.New("short")
	r := Reader{Data: []byte{0b1010_0001, 0xff}, Err: short}
	if v, _ := r.Bits(3); v != 0b101 {
		t.Fatalf("Bits(3) = %b", v)
	}
	if b, _ := r.Bool(); b {
		t.Fatal("Bool = true")
	}
	if v, _ := r.Bits(12); v != 0b0001_1111_1111 {
		t.Fatalf("Bits(12) = %b", v)
	}
	if _, err := r.Bits(2); !errors.Is(err, short) {
		t.Fatalf("past the end: %v", err)
	}
	uvlc := Reader{Data: []byte{0b0010_1000}, Err: short} // 2 zeros, a 1, then 2 bits
	if err := uvlc.SkipUVLC(); err != nil || uvlc.bit != 5 {
		t.Fatalf("SkipUVLC: %v at bit %d", err, uvlc.bit)
	}
	for code, want := range map[byte]uint64{0b1000_0000: 0, 0b0100_0000: 1, 0b0110_0000: 2, 0b0010_1000: 4} {
		r := Reader{Data: []byte{code}, Err: short}
		if v, err := r.UVLC(); err != nil || v != want {
			t.Errorf("UVLC(%08b) = %d, %v; want %d", code, v, err, want)
		}
	}
}

func TestExpGolomb(t *testing.T) {
	short := errors.New("short")
	// ue: 1 -> 0, 010 -> 1, 011 -> 2, 00100 -> 3; se of codes 1 and 2 is 1 and -1.
	r := Reader{Data: []byte{0b1010_0110, 0b0100_0100, 0b1100_0000}, Err: short}
	for _, want := range []uint64{0, 1, 2, 3} {
		if v, err := r.UVLC(); err != nil || v != want {
			t.Fatalf("UVLC = %d, %v; want %d", v, err, want)
		}
	}
	for _, want := range []int64{1, -1} {
		if v, err := r.SE(); err != nil || v != want {
			t.Fatalf("SE = %d, %v; want %d", v, err, want)
		}
	}
}
