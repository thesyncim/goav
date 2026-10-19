package rtpav

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"syscall"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

// DialUDP returns a PacketWriter sending RTP to address (host:port) over UDP
// and RTCP to the port above it (RFC 3550). Feedback is read from both local
// sockets, so a receiver that sends RTCP to the RTP port is heard too.
func DialUDP(address string) (PacketWriter, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	rtpPort, err := strconv.Atoi(port)
	if err != nil || rtpPort <= 0 || rtpPort >= 65535 {
		return nil, &net.AddrError{Err: "invalid RTP port", Addr: address}
	}
	rtpConn, err := net.Dial("udp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, err
	}
	rtcpConn, err := net.Dial("udp", net.JoinHostPort(host, strconv.Itoa(rtpPort+1)))
	if err != nil {
		rtpConn.Close()
		return nil, err
	}
	w := &udpWriter{rtp: rtpConn, rtcp: rtcpConn, feedback: make(chan []rtcp.Packet, 16), closed: make(chan struct{})}
	w.readers.Add(2)
	go w.read(rtpConn)
	go w.read(rtcpConn)
	return w, nil
}

type udpWriter struct {
	rtp, rtcp net.Conn
	// buf is the marshal scratch of WriteRTP, which the one data path calls.
	buf       []byte
	feedback  chan []rtcp.Packet
	closed    chan struct{}
	closeOnce sync.Once
	readers   sync.WaitGroup
}

func (w *udpWriter) WriteRTP(_ context.Context, packet *rtp.Packet) error {
	size := packet.MarshalSize()
	if cap(w.buf) < size {
		w.buf = make([]byte, size)
	}
	n, err := packet.MarshalTo(w.buf[:size])
	if err != nil {
		return err
	}
	_, err = w.rtp.Write(w.buf[:n])
	return unrefused(err)
}

func (w *udpWriter) WriteRTCP(_ context.Context, packets []rtcp.Packet) error {
	data, err := rtcp.Marshal(packets)
	if err != nil {
		return err
	}
	_, err = w.rtcp.Write(data)
	return unrefused(err)
}

// unrefused drops the refusal a connected UDP socket reports after an ICMP
// port unreachable: a receiver that is not listening yet, or restarts, is
// not a failed send.
func unrefused(err error) error {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return nil
	}
	return err
}

func (w *udpWriter) ReadRTCP(ctx context.Context) ([]rtcp.Packet, error) {
	select {
	case packets := <-w.feedback:
		return packets, nil
	case <-w.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *udpWriter) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.closed)
		err = w.rtp.Close()
		if rtcpErr := w.rtcp.Close(); err == nil {
			err = rtcpErr
		}
		w.readers.Wait()
	})
	return err
}

// read delivers the RTCP arriving on conn until it closes; anything else
// (RTP, junk) is skipped.
func (w *udpWriter) read(conn net.Conn) {
	defer w.readers.Done()
	buf := make([]byte, 1500)
	for {
		n, err := conn.Read(buf)
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			// An unreachable receiver (ICMP) fails one read; keep listening.
			continue
		}
		if n < 2 || buf[1] < 192 || buf[1] > 223 {
			continue
		}
		packets, err := rtcp.Unmarshal(buf[:n])
		if err != nil {
			continue
		}
		select {
		case w.feedback <- packets:
		case <-w.closed:
			return
		}
	}
}
