// Package srtav is the srt scheme: MPEG-TS over SRT (Secure Reliable
// Transport), the protocol contribution encoders, OBS, vMix, and media
// servers use for live video across the internet, through
// github.com/datarhei/gosrt, a pure-Go implementation. Register it under
// its name:
//
//	rt, err := bundle.New(runconfig.Scheme("srt", srtav.SRT()))
//
// It lives in its own module so the root goav module carries no
// transport dependencies.
package srtav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	srt "github.com/datarhei/gosrt"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// SRT is the srt scheme, for goav.URIInput and goav.URI. A URL names the
// peer and the session:
//
//	srt://host:port?streamid=live/cam1&passphrase=…&latency=200
//
// A URL with a host calls it (mode=caller); one without, srt://:port,
// listens for a caller (mode=listener), which either side can name. The
// parameters are streamid, passphrase (10 to 80 characters, encrypting
// the session with AES at pbkeylen 16, 24, or 32 bytes), latency (the
// receive buffer, in milliseconds: 120 by default), and mode. goav redacts
// the passphrase wherever it reports the URL.
//
// An input is live: it reads the transport stream the peer sends, and its
// end, the peer gone, is ErrDisconnected, which goav.Reconnect recovers. A
// listener admits one caller at a time and rejects one whose stream id
// differs from streamid, when set, or whose passphrase differs. A
// destination sends seven TS packets per SRT packet, paced at the media
// time, and drains what the peer has not acknowledged before it closes. A
// caller the peer rejects fails with *RejectedError, which is ErrPassphrase
// when the passphrases differ.
func SRT() provider.Scheme { return srtScheme{} }

type srtScheme struct{}

var (
	_ provider.InputScheme       = srtScheme{}
	_ provider.DestinationScheme = srtScheme{}
)

var (
	// ErrPassphrase reports a session the peer refused for its passphrase:
	// a different one, or one on one side only.
	ErrPassphrase = errors.New("srtav: passphrase rejected")
	// ErrDisconnected reports a session that ended under a running input or
	// destination: the peer closed it or stopped answering.
	ErrDisconnected = errors.New("srtav: disconnected")
)

// RejectedError reports a handshake the peer rejected, with its reason
// code (SRT's REJ_* and REJX_* values: 1010 is a bad secret, 1404 an
// unknown stream id). It is ErrPassphrase for 1010 and 1011.
type RejectedError struct {
	Reason uint32
}

func (e *RejectedError) Error() string {
	if name, ok := rejectionNames[e.Reason]; ok {
		return fmt.Sprintf("srtav: the peer rejected the connection: %s (%d)", name, e.Reason)
	}
	return fmt.Sprintf("srtav: the peer rejected the connection (%d)", e.Reason)
}

func (e *RejectedError) Is(target error) bool {
	return target == ErrPassphrase && (e.Reason == uint32(srt.REJ_BADSECRET) || e.Reason == uint32(srt.REJ_UNSECURE))
}

var rejectionNames = map[uint32]string{
	uint32(srt.REJ_PEER):          "rejected by the peer",
	uint32(srt.REJ_BACKLOG):       "backlog exceeded",
	uint32(srt.REJ_VERSION):       "SRT version too old",
	uint32(srt.REJ_BADSECRET):     "wrong passphrase",
	uint32(srt.REJ_UNSECURE):      "passphrase required or unexpected",
	uint32(srt.REJX_UNAUTHORIZED): "unauthorized",
	uint32(srt.REJX_FORBIDDEN):    "forbidden",
	uint32(srt.REJX_NOTFOUND):     "stream id not found",
	uint32(srt.REJX_CONFLICT):     "stream id in use",
}

// session is a bound srt URL: where to connect and how.
type session struct {
	address  string
	listen   bool
	streamID string
	secret   string
	config   srt.Config
}

func (srtScheme) Input(u *url.URL) (provider.Input, error) {
	s, err := bind(u)
	if err != nil {
		return nil, err
	}
	return input{s}, nil
}

func (srtScheme) Destination(u *url.URL) (provider.Output, error) {
	s, err := bind(u)
	if err != nil {
		return nil, err
	}
	return destination{s}, nil
}

// bind reads and checks an srt URL without connecting.
func bind(u *url.URL) (session, error) {
	if u.Port() == "" {
		return session{}, errors.New("srt URL has no port: srt://host:port, or srt://:port to listen")
	}
	s := session{address: u.Host, listen: u.Hostname() == "", config: srt.DefaultConfig()}
	for key, values := range u.Query() {
		value := values[len(values)-1]
		switch key {
		case "mode":
			switch value {
			case "caller":
				s.listen = false
			case "listener":
				s.listen = true
			default:
				return session{}, fmt.Errorf("srt URL mode %q: want caller or listener", value)
			}
		case "streamid":
			s.streamID = value
		case "passphrase":
			s.secret = value
		case "pbkeylen":
			n, err := strconv.Atoi(value)
			if err != nil || n != 16 && n != 24 && n != 32 {
				return session{}, fmt.Errorf("srt URL pbkeylen %q: want 16, 24, or 32", value)
			}
			s.config.PBKeylen = n
		case "latency":
			ms, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return session{}, fmt.Errorf("srt URL latency %q: want milliseconds", value)
			}
			s.config.Latency = time.Duration(ms) * time.Millisecond
		default:
			return session{}, fmt.Errorf("srt URL has the unknown parameter %q (it takes mode, streamid, passphrase, pbkeylen, and latency)", key)
		}
	}
	if !s.listen && u.Hostname() == "" {
		return session{}, errors.New("srt URL calls no host: srt://host:port, or mode=listener")
	}
	if s.secret != "" && (len(s.secret) < srt.MIN_PASSPHRASE_SIZE || len(s.secret) > srt.MAX_PASSPHRASE_SIZE) {
		return session{}, fmt.Errorf("srt URL passphrase has %d characters: SRT takes %d to %d", len(s.secret), srt.MIN_PASSPHRASE_SIZE, srt.MAX_PASSPHRASE_SIZE)
	}
	if len(s.streamID) > srt.MAX_STREAMID_SIZE {
		return session{}, fmt.Errorf("srt URL streamid is longer than %d bytes", srt.MAX_STREAMID_SIZE)
	}
	if !s.listen {
		s.config.StreamId, s.config.Passphrase = s.streamID, s.secret
	}
	if err := s.config.Validate(); err != nil {
		return session{}, fmt.Errorf("srt URL: %w", err)
	}
	return s, nil
}

func (s session) detail() string {
	if s.listen {
		return "srt listener"
	}
	return "srt caller"
}

// open connects: it calls the peer, or listens until a caller it admits
// arrives. ctx bounds the wait.
func (s session) open(ctx context.Context) (*conn, error) {
	if !s.listen {
		c, err := srt.DialWithContext(ctx, "srt", s.address, s.config)
		if err != nil {
			return nil, dialError(ctx, err)
		}
		return newConn(c, nil), nil
	}
	ln, err := srt.Listen("srt", s.address, s.config)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, ln.Close)
	defer stop()
	for {
		req, err := ln.Accept2()
		if err != nil {
			ln.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, err
		}
		if reason, ok := s.admit(req); !ok {
			req.Reject(reason)
			continue
		}
		c, err := req.Accept()
		if err != nil {
			continue
		}
		return newConn(c, ln), nil
	}
}

// admit decides on a caller: its stream id, when the URL names one, and
// its passphrase must match.
func (s session) admit(req srt.ConnRequest) (srt.RejectionReason, bool) {
	switch {
	case s.streamID != "" && req.StreamId() != s.streamID:
		return srt.REJX_NOTFOUND, false
	case s.secret == "" && req.IsEncrypted(), s.secret != "" && !req.IsEncrypted():
		return srt.REJ_UNSECURE, false
	case s.secret != "" && req.SetPassphrase(s.secret) != nil:
		return srt.REJ_BADSECRET, false
	}
	return 0, true
}

// dialError types a refused call. gosrt reports a rejection only as text,
// "connection rejected: REJECT (<code in base 32>)"; TestPassphraseMismatch pins
// that form, so a gosrt that changes it fails the tests, not the users.
func dialError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if code, ok := strings.CutPrefix(err.Error(), "connection rejected: REJECT ("); ok {
		if reason, err := strconv.ParseUint(strings.TrimSuffix(code, ")"), 32, 32); err == nil {
			return &RejectedError{Reason: uint32(reason)}
		}
	}
	return err
}

type input struct{ session }

func (input) SourceShape() shape.Spec {
	return shape.Spec{Format: av.FormatTS, Realtime: true}
}

func (in input) OpenBytes(ctx context.Context) (io.ReadCloser, error) {
	c, err := in.open(ctx)
	if err != nil {
		return nil, err
	}
	return receive(c), nil
}

type destination struct{ session }

func (destination) Name() string { return "srt" }

func (d destination) Detail() string { return d.detail() }

func (destination) Contract() provider.Contract {
	return provider.Contract{
		ByteStream: true,
		Realtime:   true,
		Protocol:   "srt",
		Formats:    []av.FormatID{av.FormatTS},
		MIMETypes:  []string{"video/mp2t"},
	}
}

func (d destination) Open(ctx context.Context, _ provider.Info) (provider.Writer, error) {
	return d.open(ctx)
}

// tsPayload is the payload of each SRT packet sent: seven 188-byte TS
// packets, the size that fits an Ethernet MTU.
const tsPayload = 7 * 188

// conn is an open session, read as a byte stream or written as one.
type conn struct {
	srt       srt.Conn
	ln        srt.Listener
	lastWrite atomic.Int64 // unix nanoseconds, 0 before the first write
	window    time.Time    // the burst window Write paces in, and its packets
	inWindow  int
	closed    atomic.Bool
	closing   sync.Once
}

func newConn(c srt.Conn, ln srt.Listener) *conn {
	return &conn{srt: c, ln: ln}
}

func (c *conn) Read(p []byte) (int, error) {
	n, err := c.srt.Read(p)
	if err != nil {
		return n, c.ended(err)
	}
	return n, nil
}

// closeMargin is how long past the peer's delivery delay Close keeps a
// session that wrote: the peer delivers on its own clock and receive tick,
// measured here up to 100ms behind the delay it declared.
const closeMargin = 250 * time.Millisecond

// writeRetry bounds how long Write waits out a full send queue: gosrt
// refuses a packet, with io.EOF, both when its 1024-packet queue is full
// and when the session is gone, so a refusal is retried for this long
// before it means the latter.
const writeRetry = 500 * time.Millisecond

// burst paces a large write: at most burstPackets per burstWindow. A
// gosrt receiver delivers every packet due in one 10ms tick into a
// 1024-packet queue and drops the overflow, so an access unit of more than
// a megabyte (a 4K keyframe) sent at once would lose its tail there.
const (
	burstPackets = 512
	burstWindow  = 10 * time.Millisecond
)

func (c *conn) Write(p []byte) (int, error) {
	defer c.lastWrite.Store(time.Now().UnixNano())
	for sent := 0; sent < len(p); {
		c.pace()
		chunk := p[sent:min(len(p), sent+tsPayload)]
		n, err := c.srt.Write(chunk)
		for wait := time.Millisecond; err != nil && wait < writeRetry && !c.closed.Load(); wait *= 2 {
			time.Sleep(wait)
			n, err = c.srt.Write(chunk)
		}
		sent += n
		if err != nil {
			return sent, c.ended(err)
		}
	}
	return len(p), nil
}

// pace counts a packet into the current burst window, waiting for the next
// window when this one is full.
func (c *conn) pace() {
	now := time.Now()
	if now.Sub(c.window) >= burstWindow {
		c.window, c.inWindow = now, 0
	}
	if c.inWindow == burstPackets {
		time.Sleep(c.window.Add(burstWindow).Sub(now))
		c.window, c.inWindow = time.Now(), 0
	}
	c.inWindow++
}

// ended reports a failed read or write: net.ErrClosed after Close, else the
// session's end.
func (c *conn) ended(err error) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	// The cause is text only: gosrt ends a session with io.EOF, which would
	// read as the media's natural end.
	return fmt.Errorf("%w: %v", ErrDisconnected, err)
}

// Close ends the session. After writing, it first lets the peer have the
// tail: it waits for the acknowledgement of everything sent, then for the
// peer's delivery delay (its latency) and closeMargin past the last write,
// since the peer drops what it has not delivered when the session closes.
// The first wait is bounded by the latency and a second.
func (c *conn) Close() error {
	c.closing.Do(func() {
		c.closed.Store(true)
		if last := c.lastWrite.Load(); last != 0 {
			var stats srt.Statistics
			c.srt.Stats(&stats)
			delay := time.Duration(stats.Instantaneous.MsSendTsbPdDelay) * time.Millisecond
			for deadline := time.Now().Add(delay + time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
				if c.srt.Stats(&stats); stats.Instantaneous.PktSendBuf == 0 {
					break
				}
			}
			time.Sleep(time.Until(time.Unix(0, last).Add(delay + closeMargin)))
		}
		c.srt.Close()
		if c.ln != nil {
			c.ln.Close()
		}
	})
	return nil
}
