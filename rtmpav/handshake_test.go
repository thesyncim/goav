package rtmpav

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"testing"
)

// goav's client and server complete the signed handshake with each other.
func TestHandshakeSigned(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	done := make(chan error, 1)
	go func() { done <- serverHandshake(b) }()
	if err := clientHandshake(a); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// A client that sends an unsigned C1 (version zero) gets it echoed in S2
// and an unsigned S1; a signed C1 gets a signed S1 and an S2 keyed by
// C1's digest, as Flash Player checks it.
func TestServerHandshakeAnswersEachForm(t *testing.T) {
	for _, signed := range []bool{false, true} {
		a, b := net.Pipe()
		go func() { _ = serverHandshake(b) }()
		c1 := make([]byte, handshakeSize)
		var digest []byte
		if signed {
			digest, _ = sign(c1, clientVersion, playerKey)
		} else {
			copy(c1[8:], bytes.Repeat([]byte{7}, handshakeSize-8))
		}
		go func() { _, _ = a.Write(append([]byte{rtmpVersion}, c1...)) }()
		reply := make([]byte, 1+2*handshakeSize)
		if _, err := io.ReadFull(a, reply); err != nil {
			t.Fatal(err)
		}
		s1, s2 := reply[1:1+handshakeSize], reply[1+handshakeSize:]
		_, s1Signed := findDigest(s1, serverKey)
		if s1Signed != signed {
			t.Fatalf("signed C1 %v: S1 signed %v", signed, s1Signed)
		}
		if !signed && !bytes.Equal(s2, c1) {
			t.Fatal("S2 does not echo an unsigned C1")
		}
		if signed {
			derive := hmac.New(sha256.New, append(bytes.Clone(serverKey), keyTail...))
			derive.Write(digest)
			mac := hmac.New(sha256.New, derive.Sum(nil))
			mac.Write(s2[:handshakeSize-digestSize])
			if !hmac.Equal(mac.Sum(nil), s2[handshakeSize-digestSize:]) {
				t.Fatal("S2 is not signed with the key C1's digest derives")
			}
		}
		_, _ = a.Write(make([]byte, handshakeSize))
		a.Close()
		b.Close()
	}
}

// The server refuses an encrypted (RTMPE) handshake by its version byte.
func TestServerHandshakeRefusesRTMPE(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() { _, _ = a.Write(append([]byte{6}, make([]byte, handshakeSize)...)) }()
	if err := serverHandshake(b); !errors.Is(err, errProtocol) {
		t.Fatalf("RTMPE: %v", err)
	}
}
