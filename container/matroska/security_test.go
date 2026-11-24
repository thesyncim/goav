package matroska

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReadSizedPayloadRejectsOversizedAndTruncated(t *testing.T) {
	if _, err := readSizedPayload(bytes.NewReader(nil), maxPayloadReadSize+1); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("oversized payload err = %v, want ErrInvalidData", err)
	}
	// Truncated stream claiming 10 MiB must hit ErrUnexpectedEOF without allocating 10 MiB.
	if _, err := readSizedPayload(bytes.NewReader([]byte{1, 2, 3}), 10<<20); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated large payload err = %v, want ErrUnexpectedEOF", err)
	}
}
