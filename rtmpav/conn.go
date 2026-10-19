package rtmpav

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/thesyncim/goav/internal/amf0"
)

// User control events (section 6.2).
const (
	eventStreamBegin     = 0
	eventStreamEOF       = 1
	eventSetBufferLength = 3
	eventPingRequest     = 6
	eventPingResponse    = 7
)

// The window and chunk size goav announces.
const (
	ackWindow = 2_500_000
	chunkSize = 4096
)

// conn is a handshaken RTMP connection: messages in and out, with the
// protocol control messages (chunk size, acknowledgements, pings) handled
// beneath them. Reads happen on one goroutine; writes may come from any.
type conn struct {
	nc net.Conn
	rd *chunkReader

	// The read side's acknowledgement state.
	window, acked uint64

	mu sync.Mutex
	wr *chunkWriter
	// writeTimeout, when set, bounds each write.
	writeTimeout time.Duration
}

func newConn(nc net.Conn) *conn {
	return &conn{nc: nc, rd: newChunkReader(nc), wr: newChunkWriter(nc)}
}

// write sends one message.
func (c *conn) write(msg message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeTimeout > 0 {
		_ = c.nc.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	return c.wr.write(msg)
}

// control sends a protocol control message.
func (c *conn) control(typ byte, body ...byte) error {
	return c.write(message{chunkStream: csControl, typ: typ, body: body})
}

// setChunkSize announces, then uses, a larger outgoing chunk size.
func (c *conn) setChunkSize(size uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.wr.write(message{chunkStream: csControl, typ: typeSetChunkSize, body: binary.BigEndian.AppendUint32(nil, size)}); err != nil {
		return err
	}
	c.wr.size = int(size)
	return nil
}

// announce sends the window acknowledgement size and chunk size this side
// uses, and for a server the peer bandwidth it allows.
func (c *conn) announce(server bool) error {
	if err := c.control(typeWindowAckSize, binary.BigEndian.AppendUint32(nil, ackWindow)...); err != nil {
		return err
	}
	if server {
		if err := c.control(typeSetPeerBandwidth, append(binary.BigEndian.AppendUint32(nil, ackWindow), 2)...); err != nil {
			return err
		}
	}
	return c.setChunkSize(chunkSize)
}

// command sends an AMF0 command message.
func (c *conn) command(chunkStream, streamID uint32, values ...any) error {
	body, err := amf0.Append(nil, values...)
	if err != nil {
		return err
	}
	return c.write(message{chunkStream: chunkStream, typ: typeCommandAMF0, streamID: streamID, body: body})
}

// userControl sends a user control event for a stream.
func (c *conn) userControl(event uint16, data ...uint32) error {
	body := binary.BigEndian.AppendUint16(nil, event)
	for _, d := range data {
		body = binary.BigEndian.AppendUint32(body, d)
	}
	return c.control(typeUserControl, body...)
}

// read returns the next message above protocol control: a command, data,
// audio, video, aggregate, or user control event. It applies the peer's
// chunk size and window, answers pings, and acknowledges what it reads.
// idle bounds the wait for it.
func (c *conn) read(idle time.Duration) (message, error) {
	for {
		if idle > 0 {
			_ = c.nc.SetReadDeadline(time.Now().Add(idle))
		}
		msg, err := c.rd.read()
		if err != nil {
			return message{}, err
		}
		if err := c.acknowledge(); err != nil {
			return message{}, err
		}
		switch msg.typ {
		case typeSetChunkSize:
			if len(msg.body) < 4 {
				return message{}, fmt.Errorf("%w: short Set Chunk Size", errProtocol)
			}
			if err := c.rd.setChunkSize(binary.BigEndian.Uint32(msg.body) & 0x7fffffff); err != nil {
				return message{}, err
			}
		case typeAbort:
			if len(msg.body) >= 4 {
				c.rd.abort(binary.BigEndian.Uint32(msg.body))
			}
		case typeWindowAckSize:
			if len(msg.body) >= 4 {
				c.window = uint64(binary.BigEndian.Uint32(msg.body))
			}
		case typeAcknowledgement, typeSetPeerBandwidth:
			// goav sends at the pace of its media, not of the peer's window.
		case typeUserControl:
			if len(msg.body) >= 6 && binary.BigEndian.Uint16(msg.body) == eventPingRequest {
				if err := c.userControl(eventPingResponse, binary.BigEndian.Uint32(msg.body[2:])); err != nil {
					return message{}, err
				}
				continue
			}
			return msg, nil
		default:
			return msg, nil
		}
	}
}

// acknowledge sends an Acknowledgement once the peer's window of bytes
// has arrived since the last.
func (c *conn) acknowledge() error {
	read := c.rd.bytesRead()
	if c.window == 0 || read-c.acked < c.window {
		return nil
	}
	c.acked = read
	return c.control(typeAcknowledgement, binary.BigEndian.AppendUint32(nil, uint32(read))...)
}

func (c *conn) Close() error { return c.nc.Close() }

// command is a decoded command message.
type command struct {
	name     string
	tx       float64
	object   amf0.Object
	args     []any
	streamID uint32
}

// decodeCommand reads an AMF0 command, or an AMF3 one in its AMF0 form.
func decodeCommand(msg message) (command, error) {
	body := msg.body
	if msg.typ == typeCommandAMF3 && len(body) > 0 {
		body = body[1:]
	}
	values, err := amf0.Decode(body)
	if err != nil {
		return command{}, fmt.Errorf("%w: command: %w", errProtocol, err)
	}
	cmd := command{streamID: msg.streamID}
	if len(values) == 0 {
		return cmd, fmt.Errorf("%w: empty command", errProtocol)
	}
	cmd.name, _ = values[0].(string)
	if len(values) > 1 {
		cmd.tx, _ = values[1].(float64)
	}
	if len(values) > 2 {
		cmd.object, _ = values[2].(amf0.Object)
	}
	if len(values) > 3 {
		cmd.args = values[3:]
	}
	return cmd, nil
}

// info is the information object of a result or status: the first object
// among its arguments.
func (c command) info() amf0.Object {
	for _, arg := range c.args {
		if obj, ok := arg.(amf0.Object); ok {
			return obj
		}
	}
	return nil
}

// stringArg is the command's nth argument after its object, as a string.
func (c command) stringArg(n int) string {
	if n < len(c.args) {
		s, _ := c.args[n].(string)
		return s
	}
	return ""
}

// isTimeout reports a read deadline that passed.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
