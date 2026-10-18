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
}
