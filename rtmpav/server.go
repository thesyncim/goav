package rtmpav

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/flv/amf0"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// Server is an RTMP ingest: it accepts publishers (OBS, ffmpeg, another
// goav task publishing rtmp://) and hands each to the live input of its
// stream. Only the streams an Input names are taken, so the stream key is
// the publisher's credential.
type Server struct {
	ln     net.Listener
	config config

	mu      sync.Mutex
	streams map[string]*ingest
	conns   map[net.Conn]struct{}
	closed  bool
	done    chan struct{}
	wg      sync.WaitGroup
}

// ingest is one stream a Server takes: the publisher waiting for its
// input, and whether one is publishing.
type ingest struct {
	arrive chan *serverSession
	active bool
}

// Listen accepts RTMP publishers on addr, or rtmps ones when
// WithTLSConfig gives it certificates. Close stops it.
//
// The server answers the signed and the simple handshake, connect with
// the Enhanced RTMP codecs and multitrack goav reads, and createStream
// and publish for the streams an Input names; it refuses a play, and a
// publish to a stream no Input names or another publisher holds, with
// NetStream.Publish.BadName.
func Listen(addr string, options ...Option) (*Server, error) {
	config := newConfig(options)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if config.tls != nil {
		ln = tls.NewListener(ln, config.tls)
	}
	s := &Server{ln: ln, config: config, streams: map[string]*ingest{}, conns: map[net.Conn]struct{}{}, done: make(chan struct{})}
	s.wg.Add(1)
	go s.accept()
	return s, nil
}

// Addr is the address the server listens on.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Input is the live input of the stream app/key a publisher sends to
// rtmp://host/app/key (its query ignored). Each open waits for the next
// publisher and reads it until it stops, which ends the input with
// ErrStreamEnded; goav.Reconnect waits for the one after.
func (s *Server) Input(stream string) provider.Input {
	stream = strings.Trim(stream, "/")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams[stream] == nil {
		s.streams[stream] = &ingest{arrive: make(chan *serverSession, 1)}
	}
	return &ingestInput{server: s, stream: stream}
}

// Close stops accepting, ends every publish, and wakes the inputs waiting
// for one.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	err := s.ln.Close()
	s.wg.Wait()
	return err
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			nc.Close()
			return
		}
		s.conns[nc] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			s.serve(nc)
		}()
	}
}

// forget drops a connection the server no longer tracks.
func (s *Server) forget(nc net.Conn) {
	s.mu.Lock()
	delete(s.conns, nc)
	s.mu.Unlock()
}

// serverSession is a publisher the server accepted.
type serverSession struct {
	*conn
	server *Server
	stream string
	done   chan struct{}
	once   sync.Once
}

// end closes the publish and frees its stream for the next publisher.
func (p *serverSession) end() error {
	p.once.Do(func() {
		p.server.release(p.stream)
		close(p.done)
		p.Close()
	})
	return nil
}

// serve runs one connection up to its publish, which it hands to the
// stream's input, then waits for that publish to end.
func (s *Server) serve(nc net.Conn) {
	defer s.forget(nc)
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(setupTimeout))
	if err := serverHandshake(nc); err != nil {
		return
	}
	c := newConn(nc)
	p, err := s.setup(c)
	if err != nil || p == nil {
		return
	}
	_ = nc.SetDeadline(time.Time{})
	select {
	case <-p.done:
	case <-s.done:
	}
}

// setup answers connect, createStream, and publish, and hands the
// publisher to its stream; it returns nil for a connection it refused.
func (s *Server) setup(c *conn) (*serverSession, error) {
	var app string
	for {
		msg, err := c.read(0)
		if err != nil {
			return nil, err
		}
		if msg.typ != typeCommandAMF0 && msg.typ != typeCommandAMF3 {
			continue
		}
		cmd, err := decodeCommand(msg)
		if err != nil {
			return nil, err
		}
		switch cmd.name {
		case "connect":
			app, _ = cmd.object.String("app")
			if i := strings.IndexByte(app, '?'); i >= 0 {
				app = app[:i]
			}
			app = strings.Trim(app, "/")
			if err := c.announce(true); err != nil {
				return nil, err
			}
			properties := append(amf0.Object{
				{Name: "fmsVer", Value: "FMS/3,5,7,7009"},
				{Name: "capabilities", Value: 31.0},
				{Name: "mode", Value: 1.0},
			}, enhancedCommand(fourCCCanDecode|fourCCCanForward, capsMultitrack|capsModEx)...)
			if err := c.command(csCommand, 0, "_result", cmd.tx, properties, amf0.Object{
				{Name: "level", Value: "status"},
				{Name: "code", Value: "NetConnection.Connect.Success"},
				{Name: "description", Value: "Connection succeeded."},
				{Name: "objectEncoding", Value: 0.0},
			}); err != nil {
				return nil, err
			}
		case "createStream":
			if err := c.command(csCommand, 0, "_result", cmd.tx, nil, 1.0); err != nil {
				return nil, err
			}
		case "releaseStream", "FCPublish":
			if err := c.command(csCommand, 0, "_result", cmd.tx, nil, nil); err != nil {
				return nil, err
			}
		case "play":
			return nil, status(c, cmd.streamID, "error", "NetStream.Play.Failed", "this server takes publishers only")
		case "publish":
			key := cmd.stringArg(0)
			if i := strings.IndexByte(key, '?'); i >= 0 {
				key = key[:i]
			}
			p := &serverSession{conn: c, server: s, stream: strings.Trim(app+"/"+key, "/"), done: make(chan struct{})}
			if !s.take(p) {
				return nil, status(c, cmd.streamID, "error", "NetStream.Publish.BadName", "no input takes this stream, or it is publishing")
			}
			if err := c.userControl(eventStreamBegin, cmd.streamID); err != nil {
				p.end()
				return nil, err
			}
			if err := status(c, cmd.streamID, "status", "NetStream.Publish.Start", "publishing"); err != nil {
				p.end()
				return nil, err
			}
			return p, nil
		}
	}
}

// status sends an onStatus.
func status(c *conn, streamID uint32, level, code, description string) error {
	return c.command(csCommand, streamID, "onStatus", 0.0, nil, amf0.Object{
		{Name: "level", Value: level},
		{Name: "code", Value: code},
		{Name: "description", Value: description},
	})
}

// take hands a publisher to its stream's input, if an Input names the
// stream and no other publisher holds it.
func (s *Server) take(p *serverSession) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.streams[p.stream]
	if in == nil || in.active || s.closed {
		return false
	}
	in.active = true
	in.arrive <- p
	return true
}

// release frees a stream for its next publisher.
func (s *Server) release(stream string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in := s.streams[stream]; in != nil {
		in.active = false
	}
}

// ingestInput is the live input of one stream a Server takes.
type ingestInput struct {
	server *Server
	stream string
}

var _ provider.ByteSource = (*ingestInput)(nil)

func (*ingestInput) SourceShape() shape.Spec {
	return shape.Spec{Format: av.FormatFLV, Realtime: true}
}

func (*ingestInput) Name() string      { return "rtmp" }
func (in *ingestInput) Detail() string { return "rtmp ingest" }

var errServerClosed = errors.New("rtmpav: the server closed")

// OpenBytes waits for the stream's next publisher.
func (in *ingestInput) OpenBytes(ctx context.Context) (io.ReadCloser, error) {
	in.server.mu.Lock()
	stream := in.server.streams[in.stream]
	in.server.mu.Unlock()
	select {
	case p := <-stream.arrive:
		return newTagReader(p.next, p.end), nil
	case <-in.server.done:
		return nil, errServerClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// next returns the publisher's next media or data message, or the error
// that ends the publish, which frees the stream for the next publisher
// before the input reopens.
func (p *serverSession) next() (message, error) {
	msg, err := p.nextMessage()
	if err != nil {
		p.end()
	}
	return msg, err
}

func (p *serverSession) nextMessage() (message, error) {
	for {
		msg, err := p.read(idleTimeout)
		if err != nil {
			if isTimeout(err) {
				return message{}, fmt.Errorf("rtmpav: publisher of %s sent no media for %s", p.redacted(), idleTimeout)
			}
			return message{}, fmt.Errorf("%w: the publisher of %s left", ErrStreamEnded, p.redacted())
		}
		switch msg.typ {
		case typeAudio, typeVideo, typeDataAMF0, typeDataAMF3, typeAggregate:
			return msg, nil
		case typeCommandAMF0, typeCommandAMF3:
			cmd, err := decodeCommand(msg)
			if err != nil {
				continue
			}
			switch cmd.name {
			case "FCUnpublish", "deleteStream", "closeStream":
				return message{}, fmt.Errorf("%w: the publisher of %s stopped", ErrStreamEnded, p.redacted())
			}
		}
	}
}

// redacted names the stream without its key.
func (p *serverSession) redacted() string {
	if i := strings.LastIndexByte(p.stream, '/'); i >= 0 {
		return p.stream[:i+1] + redacted
	}
	return redacted
}
