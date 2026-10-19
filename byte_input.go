package goav

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/provider"
)

// openByteInput opens a byte provider's reader and threads its random-access
// and liveness capabilities into the demuxer's input.
func openByteInput(ctx context.Context, src provider.ByteSource, input format.Input) (format.Input, io.ReadCloser, error) {
	reader, err := src.OpenBytes(ctx)
	if err != nil {
		return input, nil, fmt.Errorf("goav: open %s: %w", firstNonEmpty(input.Name, input.URI, "input"), err)
	}
	if reader == nil {
		return input, nil, errNilReader
	}
	input.Reader = reader
	if at, ok := reader.(io.ReaderAt); ok {
		input.ReaderAt = at
	}
	if sized, ok := reader.(interface{ Size() int64 }); ok {
		input.Size = sized.Size()
	}
	if live, ok := reader.(interface{ Live() bool }); ok && live.Live() {
		// A stream that turns out live when it opens (an Icecast mount)
		// paces and reconnects like one declared live.
		input.Realtime = true
	}
	return input, reader, nil
}

// onceCloser closes its reader once, however many owners close it.
type onceCloser struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func closeOnce(reader io.ReadCloser) io.ReadCloser {
	if once, ok := reader.(*onceCloser); ok {
		return once
	}
	return &onceCloser{ReadCloser: reader}
}

func (c *onceCloser) Close() error {
	c.once.Do(func() { c.err = c.ReadCloser.Close() })
	return c.err
}

// byteInputDemuxer owns a byte provider's reader under its demuxer: Close
// closes both, and a context that ends while the demuxer opens or reads
// closes the reader, which unblocks a Read pending on a network.
type byteInputDemuxer struct {
	format.Demuxer
	reader io.Closer
	// armedDone is the read context the reader is armed against; one
	// context.AfterFunc hook is kept per distinct read context, so steady
	// reads register nothing.
	armedDone <-chan struct{}
	stop      func() bool
}

type byteInputSeekDemuxer struct {
	*byteInputDemuxer
	seeker format.Seeker
}

func (d byteInputSeekDemuxer) Seek(ctx context.Context, position time.Duration) error {
	return d.seeker.Seek(ctx, position)
}

func (d byteInputSeekDemuxer) CanSeek() bool { return canSeek(d.seeker) }

func newByteInputDemuxer(demuxer format.Demuxer, reader io.Closer) format.Demuxer {
	d := &byteInputDemuxer{Demuxer: demuxer, reader: reader}
	if seeker, ok := demuxer.(format.Seeker); ok {
		return byteInputSeekDemuxer{byteInputDemuxer: d, seeker: seeker}
	}
	return d
}

func (d *byteInputDemuxer) Open(ctx context.Context, input format.Input, options format.OpenOptions) error {
	stop := context.AfterFunc(ctx, func() { _ = d.reader.Close() })
	defer stop()
	return d.Demuxer.Open(ctx, input, options)
}

func (d *byteInputDemuxer) ReadInto(ctx context.Context, result *format.ReadResult) error {
	if done := ctx.Done(); done != nil && done != d.armedDone {
		if d.stop != nil {
			d.stop()
		}
		d.armedDone = done
		d.stop = context.AfterFunc(ctx, func() { _ = d.reader.Close() })
	}
	return d.Demuxer.ReadInto(ctx, result)
}

func (d *byteInputDemuxer) Close() error {
	if d.stop != nil {
		d.stop()
	}
	err := d.Demuxer.Close()
	if closeErr := d.reader.Close(); err == nil {
		err = closeErr
	}
	return err
}
