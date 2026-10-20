package goav

import (
	"bytes"
	"io"

	"github.com/thesyncim/goav/format"
)

// sniffSize bounds how much of an input probing reads: enough for every
// signature the probers match (the WebM DocType, three MPEG-TS sync bytes)
// while a held input stays at its first bytes. A plain stream is read until
// sniffMin bytes arrive, so a live input starts without waiting for more.
const (
	sniffSize = 512
	sniffMin  = 64
)

// sniffInput reads the first bytes of input for content probing without
// consuming them: a seekable reader reads in place (ReadAt) or seeks back, a
// declared ReaderAt reads from the start, and a plain stream is re-prefixed
// with what was read. The name, URI, and MIME type stay hints; the bytes
// decide. A read error is left for the demuxer to meet where it reads.
func sniffInput(input format.Input) (format.Input, []byte, error) {
	header := make([]byte, sniffSize)
	switch reader := input.Reader.(type) {
	case nil:
		if input.ReaderAt == nil {
			return input, nil, nil
		}
		n, _ := input.ReaderAt.ReadAt(header, 0)
		return input, header[:n], nil
	case io.Seeker:
		start, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			break
		}
		if at, ok := reader.(io.ReaderAt); ok {
			n, _ := at.ReadAt(header, start)
			return input, header[:n], nil
		}
		n, _ := io.ReadFull(input.Reader, header)
		if _, err := reader.Seek(start, io.SeekStart); err != nil {
			return input, nil, err
		}
		return input, header[:n], nil
	}
	n, _ := io.ReadAtLeast(input.Reader, header, sniffMin)
	header = header[:n]
	input.Reader = io.MultiReader(bytes.NewReader(header), input.Reader)
	return input, header, nil
}
