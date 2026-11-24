package h264

import (
	"errors"
	"testing"
)

func TestAppendAnnexBRejectsHighBitLength(t *testing.T) {
	// 4-byte length 0x80000001 must return ErrInvalid without panicking.
	src := []byte{0x80, 0x00, 0x00, 0x01, 0x65}
	if _, err := AppendAnnexB(nil, src, 4); !errors.Is(err, ErrInvalid) {
		t.Fatalf("AppendAnnexB high-bit length err = %v, want ErrInvalid", err)
	}
}
