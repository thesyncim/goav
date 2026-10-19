package rtmpav

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
)

// Message types (RTMP 1.0 sections 5.4, 6.2, 7.1).
const (
	typeSetChunkSize     = 1
	typeAbort            = 2
	typeAcknowledgement  = 3
	typeUserControl      = 4
	typeWindowAckSize    = 5
	typeSetPeerBandwidth = 6
	typeAudio            = 8
	typeVideo            = 9
	typeDataAMF3         = 15
	typeCommandAMF3      = 17
	typeDataAMF0         = 18
	typeCommandAMF0      = 20
	typeAggregate        = 22
)

// Chunk stream ids goav sends on: protocol control, commands, and media.
const (
	csControl = 2
	csCommand = 3
	csAudio   = 4
	csData    = 5
	csVideo   = 6
)

const (
	// defaultChunkSize is every chunk stream's size until a Set Chunk Size.
	defaultChunkSize = 128
	// maxChunkSize bounds a peer's Set Chunk Size: no message exceeds 16 MiB.
	maxChunkSize = 1<<24 - 1
	// maxAssembling bounds the bytes held across messages still arriving in
	// chunks, which a peer interleaves across up to 65599 chunk streams.
	maxAssembling = 32 << 20
	// extendedMark in a 24-bit timestamp field announces a 32-bit extended
	// timestamp after the message header.
	extendedMark = 0xffffff
)

// message is one RTMP message.
type message struct {
	chunkStream uint32
	typ         byte
	streamID    uint32
	timestamp   uint32
	body        []byte
}

// chunkReader reassembles messages from the chunk stream (section 5.3):
// every basic header form, every message header type, extended
// timestamps, and Abort.
type chunkReader struct {
	r         *bufio.Reader
	size      int
	streams   map[uint32]*readStream
	buffered  int
	scratch   [11]byte
	countFrom *countingReader
}

// readStream is the state of one chunk stream: the last message header
// and the message being assembled.
type readStream struct {
	timestamp, delta uint32
	length           int
	typ              byte
	streamID         uint32
	extended         bool
	started          bool
	body             []byte
}

// countingReader counts the bytes read from the connection, for
// acknowledgements.
type countingReader struct {
	r io.Reader
	n uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += uint64(n)
	return n, err
}

func newChunkReader(r io.Reader) *chunkReader {
	counter := &countingReader{r: r}
	return &chunkReader{
		r:         bufio.NewReaderSize(counter, 64<<10),
		size:      defaultChunkSize,
		streams:   map[uint32]*readStream{},
		countFrom: counter,
	}
}

// bytesRead is how many bytes the connection delivered.
func (c *chunkReader) bytesRead() uint64 { return c.countFrom.n }

// setChunkSize applies the peer's Set Chunk Size.
func (c *chunkReader) setChunkSize(size uint32) error {
	if size == 0 || size > maxChunkSize {
		return fmt.Errorf("%w: chunk size %d", errProtocol, size)
	}
	c.size = int(size)
	return nil
}

// abort drops the partial message of a chunk stream.
func (c *chunkReader) abort(id uint32) {
	if s := c.streams[id]; s != nil {
		c.buffered -= len(s.body)
		s.body = s.body[:0]
	}
}

// read returns the next whole message. Its body is valid until the next
// read of its chunk stream.
func (c *chunkReader) read() (message, error) {
	for {
		msg, done, err := c.readChunk()
		if err != nil || done {
			return msg, err
		}
	}
}

// readChunk reads one chunk; done reports that it completed a message.
func (c *chunkReader) readChunk() (message, bool, error) {
	first, err := c.r.ReadByte()
	if err != nil {
		return message{}, false, err
	}
	format := first >> 6
	id := uint32(first & 0x3f)
	switch id {
	case 0: // two bytes: 64-319
		b, err := c.r.ReadByte()
		if err != nil {
			return message{}, false, unexpected(err)
		}
		id = 64 + uint32(b)
	case 1: // three bytes: 64-65599
		if _, err := io.ReadFull(c.r, c.scratch[:2]); err != nil {
			return message{}, false, unexpected(err)
		}
		id = 64 + uint32(c.scratch[0]) + uint32(c.scratch[1])<<8
	}
	s := c.streams[id]
	if s == nil {
		if format != 0 {
			return message{}, false, fmt.Errorf("%w: chunk stream %d starts with a type %d header", errProtocol, id, format)
		}
		s = &readStream{}
		c.streams[id] = s
	}
	header := c.scratch[:[]int{11, 7, 3, 0}[format]]
	if _, err := io.ReadFull(c.r, header); err != nil {
		return message{}, false, unexpected(err)
	}
	fresh := len(s.body) == 0 // this chunk starts a message
	if format == 3 && !s.started {
		return message{}, false, fmt.Errorf("%w: chunk stream %d continues no message", errProtocol, id)
	}
	var stamp uint32
	if format < 3 {
		stamp = uint32(header[0])<<16 | uint32(header[1])<<8 | uint32(header[2])
		s.extended = stamp == extendedMark
	}
	if format < 2 {
		s.length = int(header[3])<<16 | int(header[4])<<8 | int(header[5])
		s.typ = header[6]
	}
	if format == 0 {
		s.streamID = binary.LittleEndian.Uint32(header[7:])
	}
	if s.extended {
		ext, err := c.extendedTimestamp()
		if err != nil {
			return message{}, false, err
		}
		if format < 3 {
			stamp = ext
		}
	}
	if fresh {
		switch format {
		case 0:
			// A type 3 message after it repeats its timestamp as a delta.
			s.timestamp, s.delta = stamp, stamp
		case 1, 2:
			s.timestamp += stamp
			s.delta = stamp
		case 3:
			s.timestamp += s.delta
		}
	} else if format != 3 {
		return message{}, false, fmt.Errorf("%w: chunk stream %d interrupts a message with a type %d header", errProtocol, id, format)
	}
	s.started = true
	n := min(s.length-len(s.body), c.size)
	if c.buffered+n > maxAssembling {
		return message{}, false, fmt.Errorf("%w: more than %d MiB of messages arriving at once", errProtocol, maxAssembling>>20)
	}
	start := len(s.body)
	s.body = slices.Grow(s.body, n)[:start+n]
	if _, err := io.ReadFull(c.r, s.body[start:]); err != nil {
		return message{}, false, unexpected(err)
	}
	if len(s.body) < s.length {
		c.buffered += n
		return message{}, false, nil
	}
	c.buffered -= start
	msg := message{chunkStream: id, typ: s.typ, streamID: s.streamID, timestamp: s.timestamp, body: s.body}
	s.body = s.body[:0:0]
	return msg, true, nil
}

// extendedTimestamp reads a chunk's 32-bit extended timestamp, which a
// type 3 chunk repeats after a header that had one, as FFmpeg, librtmp,
// and MediaMTX write and read it.
func (c *chunkReader) extendedTimestamp() (uint32, error) {
	if _, err := io.ReadFull(c.r, c.scratch[:4]); err != nil {
		return 0, unexpected(err)
	}
	return binary.BigEndian.Uint32(c.scratch[:4]), nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// chunkWriter splits messages into chunks: a type 0 header for a chunk
// stream's first message or a new message stream, type 1 after, type 3
// for continuations, each repeating an extended timestamp.
type chunkWriter struct {
	w       *bufio.Writer
	size    int
	streams map[uint32]*writeStream
	header  []byte
}

type writeStream struct {
	timestamp uint32
	streamID  uint32
}

func newChunkWriter(w io.Writer) *chunkWriter {
	return &chunkWriter{w: bufio.NewWriterSize(w, 64<<10), size: defaultChunkSize, streams: map[uint32]*writeStream{}}
}

// write chunks one message and flushes it.
func (c *chunkWriter) write(msg message) error {
	if len(msg.body) > maxChunkSize {
		return fmt.Errorf("%w: a %d-byte message exceeds RTMP's 16 MiB", errProtocol, len(msg.body))
	}
	s := c.streams[msg.chunkStream]
	format := byte(1)
	stamp := msg.timestamp
	if s == nil || s.streamID != msg.streamID || msg.timestamp < s.timestamp {
		format = 0
		if s == nil {
			s = &writeStream{}
			c.streams[msg.chunkStream] = s
		}
	} else {
		stamp = msg.timestamp - s.timestamp
	}
	s.timestamp, s.streamID = msg.timestamp, msg.streamID
	extended := stamp >= extendedMark
	field := min(stamp, extendedMark)
	h := c.basicHeader(c.header[:0], format, msg.chunkStream)
	h = append(h, byte(field>>16), byte(field>>8), byte(field))
	h = append(h, byte(len(msg.body)>>16), byte(len(msg.body)>>8), byte(len(msg.body)), msg.typ)
	if format == 0 {
		h = binary.LittleEndian.AppendUint32(h, msg.streamID)
	}
	if extended {
		h = binary.BigEndian.AppendUint32(h, stamp)
	}
	body := msg.body
	for {
		if _, err := c.w.Write(h); err != nil {
			return err
		}
		n := min(len(body), c.size)
		if _, err := c.w.Write(body[:n]); err != nil {
			return err
		}
		if body = body[n:]; len(body) == 0 {
			break
		}
		h = c.basicHeader(h[:0], 3, msg.chunkStream)
		if extended {
			h = binary.BigEndian.AppendUint32(h, stamp)
		}
	}
	c.header = h[:0]
	return c.w.Flush()
}

// basicHeader appends a chunk basic header in the shortest form the chunk
// stream id allows.
func (c *chunkWriter) basicHeader(dst []byte, format byte, id uint32) []byte {
	switch {
	case id < 64:
		return append(dst, format<<6|byte(id))
	case id < 320:
		return append(dst, format<<6, byte(id-64))
	default:
		return append(dst, format<<6|1, byte(id-64), byte((id-64)>>8))
	}
}
