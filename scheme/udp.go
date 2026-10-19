package scheme

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// UDP is the udp scheme: an MPEG transport stream in UDP datagrams, as
// broadcast encoders, IPTV head-ends, and ffmpeg send it. As an input,
// udp://239.1.1.1:5000 joins that multicast group (on the interface
// ?iface=eth0 names, else the system's choice) and udp://:5000 listens on
// every address; the input is live, and a read that waits 30s for a
// datagram fails, which goav.Reconnect recovers. As a destination,
// udp://host:5000 sends the stream, unicast or multicast, seven TS packets
// (1316 bytes) per datagram, paced at its media time.
func UDP() provider.Scheme { return udpScheme{} }

type udpScheme struct{}

var (
	_ provider.InputScheme       = udpScheme{}
	_ provider.DestinationScheme = udpScheme{}
)

const (
	// tsDatagram is the payload of each datagram sent: seven 188-byte
	// packets, the size that fits an Ethernet MTU.
	tsDatagram = 7 * 188
	// udpIdle bounds a read waiting for a datagram.
	udpIdle = 30 * time.Second
	// udpReadBuffer is the socket receive buffer asked for, so a burst at a
	// broadcast bitrate is not dropped before it is read.
	udpReadBuffer = 4 << 20
)

var errUDPIdle = fmt.Errorf("udp: no datagram for %s: %w", udpIdle, os.ErrDeadlineExceeded)

func (udpScheme) Input(u *url.URL) (provider.Input, error) {
	addr, err := parseUDP(u, true)
	if err != nil {
		return nil, err
	}
	iface := u.Query().Get("iface")
	if ip := net.ParseIP(addr.host); iface != "" && ip != nil && !ip.IsMulticast() {
		return nil, errors.New("udp URL names an iface for an address that is not a multicast group")
	}
	return udpInput{addr: addr, iface: iface}, nil
}

func (udpScheme) Destination(u *url.URL) (provider.Output, error) {
	addr, err := parseUDP(u, false)
	if err != nil {
		return nil, err
	}
	return udpDestination{addr: addr}, nil
}

// udpURL is the host and port of a udp URL, resolved when the run opens it.
type udpURL struct {
	host string
	port int
}

// parseUDP reads host:port from a udp URL. An input may leave the host out
// (listen on every address); a destination may not.
func parseUDP(u *url.URL, input bool) (udpURL, error) {
	for key := range u.Query() {
		if key != "iface" || !input {
			return udpURL{}, fmt.Errorf("udp URL has the unknown parameter %q (an input takes iface)", key)
		}
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return udpURL{}, errors.New("udp URL has no port: udp://host:port")
	}
	if u.Hostname() == "" && !input {
		return udpURL{}, errors.New("udp URL has no host to send to: udp://host:port")
	}
	return udpURL{host: u.Hostname(), port: int(port)}, nil
}

func (a udpURL) resolve(ctx context.Context) (*net.UDPAddr, error) {
	addr := &net.UDPAddr{Port: a.port}
	if a.host == "" {
		return addr, nil
	}
	if addr.IP = net.ParseIP(a.host); addr.IP != nil {
		return addr, nil
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", a.host)
	if err != nil {
		return nil, err
	}
	addr.IP = ips[0]
	return addr, nil
}

type udpInput struct {
	addr  udpURL
	iface string
}

func (udpInput) SourceShape() shape.Spec {
	return shape.Spec{Format: av.FormatTS, Realtime: true}
}

func (in udpInput) OpenBytes(ctx context.Context) (io.ReadCloser, error) {
	addr, err := in.addr.resolve(ctx)
	if err != nil {
		return nil, err
	}
	var conn *net.UDPConn
	if addr.IP.IsMulticast() {
		var ifi *net.Interface
		if in.iface != "" {
			if ifi, err = net.InterfaceByName(in.iface); err != nil {
				return nil, fmt.Errorf("udp: interface %q: %w", in.iface, err)
			}
		}
		conn, err = net.ListenMulticastUDP("udp", ifi, addr)
	} else {
		conn, err = net.ListenUDP("udp", addr)
	}
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(udpReadBuffer) // best effort: the OS may cap it
	return &datagramReader{conn: conn, buf: make([]byte, 64<<10)}, nil
}

// datagramReader reads a datagram socket as a byte stream: each datagram
// whole, in arrival order.
type datagramReader struct {
	conn    *net.UDPConn
	buf     []byte
	pending []byte
}

func (r *datagramReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if err := r.conn.SetReadDeadline(time.Now().Add(udpIdle)); err != nil {
			return 0, err
		}
		n, err := r.conn.Read(r.buf)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return 0, errUDPIdle
		}
		if err != nil {
			return 0, err
		}
		r.pending = r.buf[:n]
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *datagramReader) Close() error { return r.conn.Close() }

// Live reports the input live: datagrams arrive on the sender's clock.
func (*datagramReader) Live() bool { return true }

type udpDestination struct {
	addr udpURL
}

func (udpDestination) Name() string { return "udp" }

func (udpDestination) Contract() provider.Contract {
	return provider.Contract{
		ByteStream: true,
		Realtime:   true,
		Protocol:   "udp",
		Formats:    []av.FormatID{av.FormatTS},
		MIMETypes:  []string{"video/mp2t"},
	}
}

func (d udpDestination) Open(ctx context.Context, _ provider.Info) (provider.Writer, error) {
	addr, err := d.addr.resolve(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	return datagramWriter{conn}, nil
}

// datagramWriter sends a byte stream as datagrams of at most tsDatagram
// bytes, a Write's bytes in as few as fit, none held back.
type datagramWriter struct{ conn *net.UDPConn }

func (w datagramWriter) Write(p []byte) (int, error) {
	for sent := 0; sent < len(p); {
		n, err := w.conn.Write(p[sent:min(len(p), sent+tsDatagram)])
		sent += n
		if err != nil {
			return sent, err
		}
	}
	return len(p), nil
}

func (w datagramWriter) Close() error { return w.conn.Close() }
