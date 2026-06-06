package ebml

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidVINT       = errors.New("ebml: invalid variable-size integer")
	ErrVINTOverflow      = errors.New("ebml: variable-size integer overflow")
	ErrInvalidElementID  = errors.New("ebml: invalid element id")
	ErrInvalidSize       = errors.New("ebml: invalid element size")
	ErrUnknownSize       = errors.New("ebml: unknown element size")
	ErrElementTooLarge   = errors.New("ebml: element too large")
	ErrNonSeekableWriter = errors.New("ebml: writer is not seekable")
)

type ElementError struct {
	ID     ID
	Offset int64
	Err    error
}

func (e *ElementError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.ID == 0 {
		return fmt.Sprintf("ebml: offset %d: %v", e.Offset, e.Err)
	}
	return fmt.Sprintf("ebml: element 0x%x at offset %d: %v", uint64(e.ID), e.Offset, e.Err)
}

func (e *ElementError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
