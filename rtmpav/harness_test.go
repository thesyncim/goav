package rtmpav

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/internal/amf0"
	runconfig "github.com/thesyncim/goav/runconfig"
)

const fixtures = "../container/flv/testdata/"

// testRuntime is the bundled runtime with the rtmp and rtmps schemes.
func testRuntime(t testing.TB, options ...Option) *goav.Runtime {
	t.Helper()
	scheme := RTMP(options...)
	rt, err := bundle.New(runconfig.Scheme("rtmp", scheme), runconfig.Scheme("rtmps", scheme))
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

// fixtureTags reads an FLV fixture as the RTMP messages a server sends for
// it.
func fixtureTags(t testing.TB, name string) []message {
	t.Helper()
	data, err := os.ReadFile(fixtures + name + ".flv")
	if err != nil {
		t.Fatal(err)
	}
	var out []message
	for p := data[13:]; len(p) >= 15; {
		size := int(p[1])<<16 | int(p[2])<<8 | int(p[3])
		stamp := uint32(p[7])<<24 | uint32(p[4])<<16 | uint32(p[5])<<8 | uint32(p[6])
		out = append(out, message{typ: p[0], timestamp: stamp, body: p[11 : 11+size]})
		p = p[15+size:]
	}
	return out
}

// harness is a test RTMP server: it serves plays of tags, accepts and
// drains publishes, and can declare Enhanced RTMP capabilities in its
// connect result and drop a play part way.
type harness struct {
	t    testing.TB
	ln   net.Listener
	caps amf0.Object
	tags []message
	// dropAfter, when set, ends the first play after that many tags.
	dropAfter int

	mu    sync.Mutex
	plays int
}

func newHarness(t testing.TB, tags []message) *harness {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, ln: ln, tags: tags}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go h.serve(nc)
		}
	}()
	return h
}

func (h *harness) url(key string) string { return "rtmp://" + h.ln.Addr().String() + "/live/" + key }

func (h *harness) serve(nc net.Conn) {
	defer nc.Close()
	if serverHandshake(nc) != nil {
		return
	}
	c := newConn(nc)
	for {
		msg, err := c.read(0)
		if err != nil {
			return
		}
		if msg.typ != typeCommandAMF0 {
			continue
		}
		cmd, err := decodeCommand(msg)
		if err != nil {
			return
		}
		switch cmd.name {
		case "connect":
			_ = c.announce(true)
			properties := append(amf0.Object{{Name: "fmsVer", Value: "FMS/3,0,1,123"}}, h.caps...)
			_ = c.command(csCommand, 0, "_result", cmd.tx, properties, amf0.Object{{Name: "level", Value: "status"}, {Name: "code", Value: "NetConnection.Connect.Success"}})
		case "createStream":
			_ = c.command(csCommand, 0, "_result", cmd.tx, nil, 1.0)
		case "publish":
			_ = status(c, 1, "status", "NetStream.Publish.Start", "")
		case "play":
			h.mu.Lock()
			h.plays++
			first := h.plays == 1
			h.mu.Unlock()
			_ = status(c, 1, "status", "NetStream.Play.Reset", "")
			_ = status(c, 1, "status", "NetStream.Play.Start", "")
			start := time.Now()
			for i, tag := range h.tags {
				if first && h.dropAfter > 0 && i == h.dropAfter {
					return
				}
				// Live: each tag at its time.
				if wait := time.Duration(tag.timestamp)*time.Millisecond - time.Since(start); wait > 0 {
					time.Sleep(wait)
				}
				chunkStream := uint32(csVideo)
				if tag.typ == typeAudio {
					chunkStream = csAudio
				}
				if c.write(message{chunkStream: chunkStream, typ: tag.typ, streamID: 1, timestamp: tag.timestamp, body: tag.body}) != nil {
					return
				}
			}
			// Hold the play open, as a live stream does, until the client
			// leaves.
			for {
				if _, err := c.read(0); err != nil {
					return
				}
			}
		}
	}
}

// testCertificates is a self-signed certificate for 127.0.0.1, and the
// client configuration that trusts it.
func testCertificates(t testing.TB) (server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "goav test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, &tls.Config{RootCAs: pool}
}

// freePort is a loopback port nothing listens on.
func freePort(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func testContext(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
