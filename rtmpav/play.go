package rtmpav

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// playInput is the input an rtmp URL binds to: the FLV a play receives.
type playInput struct {
	target target
	config config
}

var _ provider.ByteSource = (*playInput)(nil)

func (*playInput) SourceShape() shape.Spec {
	return shape.Spec{Format: av.FormatFLV, Realtime: true}
}

func (*playInput) Name() string   { return "rtmp" }
func (*playInput) Detail() string { return "rtmp play" }

// OpenBytes connects and plays.
func (in *playInput) OpenBytes(ctx context.Context) (io.ReadCloser, error) {
	s, err := dial(ctx, in.target, in.config, false)
	if err != nil {
		return nil, err
	}
	return newTagReader(s.next, s.close), nil
}

// next returns the play's next media or data message, or the error that
// ends it.
func (s *session) next() (message, error) {
	if len(s.early) > 0 {
		msg := s.early[0]
		s.early = s.early[1:]
		return msg, nil
	}
	for {
		msg, err := s.read(idleTimeout)
		if err != nil {
			return message{}, readError(s.target, err)
		}
		switch msg.typ {
		case typeAudio, typeVideo, typeDataAMF0, typeDataAMF3, typeAggregate:
			return msg, nil
		case typeUserControl:
			if len(msg.body) >= 2 && binary.BigEndian.Uint16(msg.body) == eventStreamEOF {
				return message{}, fmt.Errorf("%w: %s", ErrStreamEnded, s.target.name)
			}
		case typeCommandAMF0, typeCommandAMF3:
			cmd, err := decodeCommand(msg)
			if err != nil || cmd.name != "onStatus" {
				continue
			}
			info := cmd.info()
			switch code, _ := info.String("code"); code {
			case "NetStream.Play.Stop", "NetStream.Play.UnpublishNotify", "NetStream.Play.Complete":
				return message{}, fmt.Errorf("%w: %s", ErrStreamEnded, s.target.name)
			}
			if level, _ := info.String("level"); level == "error" {
				return message{}, fmt.Errorf("rtmpav: %s: %w", s.target.name, s.refusal("play", info))
			}
		}
	}
}

// readError names why a stream's connection stopped delivering.
func readError(t target, err error) error {
	switch {
	case isTimeout(err):
		return fmt.Errorf("rtmpav: %s: no media for %s", t.name, idleTimeout)
	case errors.Is(err, io.EOF):
		return fmt.Errorf("%w: %s closed the connection", ErrStreamEnded, t.name)
	default:
		return fmt.Errorf("rtmpav: %s: %w", t.name, err)
	}
}

// flvHeader starts the FLV a play becomes: version 1, audio and video,
// and the first PreviousTagSize.
var flvHeader = []byte{'F', 'L', 'V', 1, 0x05, 0, 0, 0, 9, 0, 0, 0, 0}

// tagReader turns a stream of RTMP media and data messages into FLV, for
// the runtime to demux: each message is a tag, an aggregate message the
// tags it carries.
type tagReader struct {
	next      func() (message, error)
	close     func() error
	buf       []byte
	off       int
	started   bool
	closeOnce sync.Once
	closeErr  error
}

func newTagReader(next func() (message, error), close func() error) *tagReader {
	return &tagReader{next: next, close: close}
}

// Live reports the stream is live: it reads forward, paced as it arrives.
func (r *tagReader) Live() bool { return true }

func (r *tagReader) Read(p []byte) (int, error) {
	for r.off == len(r.buf) {
		r.buf, r.off = r.buf[:0], 0
		if !r.started {
			r.started = true
			r.buf = append(r.buf, flvHeader...)
			break
		}
		msg, err := r.next()
		if err != nil {
			return 0, err
		}
		r.buf = appendTags(r.buf, msg)
	}
	n := copy(p, r.buf[r.off:])
	r.off += n
	return n, nil
}

func (r *tagReader) Close() error {
	r.closeOnce.Do(func() { r.closeErr = r.close() })
	return r.closeErr
}

// appendTags appends the FLV tags a message carries.
func appendTags(dst []byte, msg message) []byte {
	switch msg.typ {
	case typeAudio, typeVideo, typeDataAMF0:
		return appendTag(dst, msg.typ, msg.timestamp, msg.body)
	case typeDataAMF3:
		if len(msg.body) > 0 { // an AMF0 body behind the AMF3 marker byte
			return appendTag(dst, typeDataAMF0, msg.timestamp, msg.body[1:])
		}
	case typeAggregate:
		// FLV tags, their timestamps relative to the first's, which the
		// message's timestamp places.
		body := msg.body
		var base uint32
		for first := true; len(body) >= 11; first = false {
			size := int(body[1])<<16 | int(body[2])<<8 | int(body[3])
			if 11+size > len(body) {
				break
			}
			stamp := uint32(body[7])<<24 | uint32(body[4])<<16 | uint32(body[5])<<8 | uint32(body[6])
			if first {
				base = stamp
			}
			if typ := body[0] & 0x1f; typ == typeAudio || typ == typeVideo || typ == typeDataAMF0 {
				dst = appendTag(dst, typ, msg.timestamp+stamp-base, body[11:11+size])
			}
			body = body[min(11+size+4, len(body)):]
		}
	}
	return dst
}

// appendTag appends one FLV tag and its PreviousTagSize.
func appendTag(dst []byte, typ byte, timestamp uint32, body []byte) []byte {
	if len(body) > 1<<24-1 {
		return dst
	}
	size := len(body)
	dst = append(dst, typ, byte(size>>16), byte(size>>8), byte(size),
		byte(timestamp>>16), byte(timestamp>>8), byte(timestamp), byte(timestamp>>24), 0, 0, 0)
	dst = append(dst, body...)
	return binary.BigEndian.AppendUint32(dst, uint32(11+size))
}
