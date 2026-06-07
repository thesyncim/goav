package ebml

import (
	"io"
)

type ReaderOptions struct {
	MaxElementSize uint64
}

type Reader struct {
	r           io.Reader
	offset      int64
	options     ReaderOptions
	scratch     [MaxSizeWidth]byte
	skipScratch [4096]byte
}

type Header struct {
	ID         ID
	Size       Size
	Offset     int64
	DataOffset int64
	HeaderSize int
}

func NewReader(r io.Reader, options ReaderOptions) *Reader {
	return &Reader{r: r, options: options}
}

func (r *Reader) Reset(input io.Reader, options ReaderOptions) {
	r.r = input
	r.offset = 0
	r.options = options
}

// ResetAt resets the input and sets the logical offset reported by future reads.
func (r *Reader) ResetAt(input io.Reader, options ReaderOptions, offset int64) {
	r.r = input
	r.offset = offset
	r.options = options
}

func (r *Reader) Offset() int64 {
	if r == nil {
		return 0
	}
	return r.offset
}

func (r *Reader) Remaining() (uint64, bool) {
	if r == nil {
		return 0, false
	}
	limited, ok := r.r.(*io.LimitedReader)
	if !ok || limited.N < 0 {
		return 0, false
	}
	return uint64(limited.N), true
}

func (r *Reader) Read(p []byte) (int, error) {
	if r == nil || r.r == nil {
		return 0, io.ErrUnexpectedEOF
	}
	n, err := r.r.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *Reader) ReadHeader() (Header, error) {
	offset := r.offset
	id, idWidth, err := ReadID(r, &r.scratch)
	if err != nil {
		return Header{}, err
	}
	size, err := ReadSize(r, &r.scratch)
	if err != nil {
		return Header{}, &ElementError{ID: id, Offset: offset, Err: err}
	}
	if !size.Unknown && r.options.MaxElementSize > 0 && size.Value > r.options.MaxElementSize {
		return Header{}, &ElementError{ID: id, Offset: offset, Err: ErrElementTooLarge}
	}
	return Header{
		ID:         id,
		Size:       size,
		Offset:     offset,
		DataOffset: r.offset,
		HeaderSize: idWidth + size.Width,
	}, nil
}

func (r *Reader) ReadFull(dst []byte) error {
	_, err := io.ReadFull(r, dst)
	return err
}

func (r *Reader) Skip(size uint64) error {
	if size > uint64(^uint(0)>>1) {
		return ErrElementTooLarge
	}
	remaining := size
	for remaining > 0 {
		n := len(r.skipScratch)
		if remaining < uint64(n) {
			n = int(remaining)
		}
		if err := r.ReadFull(r.skipScratch[:n]); err != nil {
			return err
		}
		remaining -= uint64(n)
	}
	return nil
}

func (r *Reader) Limited(size uint64) *io.LimitedReader {
	return &io.LimitedReader{R: r, N: int64(size)}
}

func (h Header) KnownSize() (uint64, error) {
	if h.Size.Unknown {
		return 0, &ElementError{ID: h.ID, Offset: h.Offset, Err: ErrUnknownSize}
	}
	return h.Size.Value, nil
}
