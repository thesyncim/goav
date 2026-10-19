package rtmpav

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

// The handshake (RTMP 1.0 section 5.2): C0/S0 is the version, C1/S1 and
// C2/S2 are 1536 bytes each. Flash Player 9 and later sign C1 with an
// HMAC-SHA256 digest, the "complex" handshake; servers that check it
// answer in kind, and the other side keys its C2 or S2 signature with the
// peer's digest. goav signs C1, and as a server answers whichever form the
// client used.
const (
	rtmpVersion   = 3
	handshakeSize = 1536
	digestSize    = 32
)

var (
	playerKey = []byte("Genuine Adobe Flash Player 001")
	serverKey = []byte("Genuine Adobe Flash Media Server 001")
	// keyTail completes each key for the C2 and S2 signatures.
	keyTail = []byte{
		0xf0, 0xee, 0xc2, 0x4a, 0x80, 0x68, 0xbe, 0xe8, 0x2e, 0x00, 0xd0, 0xd1, 0x02, 0x9e, 0x7e, 0x57,
		0x6e, 0xec, 0x5d, 0x2d, 0x29, 0x80, 0x6f, 0xab, 0x93, 0xb8, 0xe6, 0x36, 0xcf, 0xeb, 0x31, 0xae,
	}
	// signedVersions are the C1 and S1 version fields of a signed packet:
	// Flash Player 9.0.124.2, which servers expect before they sign back,
	// and a media server's.
	clientVersion = []byte{9, 0, 124, 2}
	serverVersion = []byte{4, 5, 0, 1}
)

// digestOffset is where scheme 0 or 1 puts a C1 or S1 digest: the sum of
// four pointer bytes, modulo the 728 bytes the digest may start in, past
// them.
func digestOffset(packet []byte, scheme int) int {
	base := 8
	if scheme == 1 {
		base = 772
	}
	p := packet[base : base+4]
	return base + 4 + (int(p[0])+int(p[1])+int(p[2])+int(p[3]))%728
}

// packetDigest is the HMAC of a C1 or S1 packet without the digest at
// offset.
func packetDigest(packet []byte, offset int, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(packet[:offset])
	mac.Write(packet[offset+digestSize:])
	return mac.Sum(nil)
}

// sign fills a C1 or S1 with random bytes, the version, and a scheme 0
// digest.
func sign(packet, version, key []byte) ([]byte, error) {
	if _, err := rand.Read(packet[8:]); err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(packet, 0)
	copy(packet[4:], version)
	offset := digestOffset(packet, 0)
	digest := packetDigest(packet, offset, key)
	copy(packet[offset:], digest)
	return digest, nil
}

// findDigest returns the digest of a signed C1 or S1, trying both schemes.
func findDigest(packet, key []byte) ([]byte, bool) {
	if binary.BigEndian.Uint32(packet[4:]) == 0 {
		return nil, false // a zero version: the simple handshake
	}
	for scheme := range 2 {
		offset := digestOffset(packet, scheme)
		if digest := packetDigest(packet, offset, key); hmac.Equal(digest, packet[offset:offset+digestSize]) {
			return digest, true
		}
	}
	return nil, false
}

// answer is a C2 or S2: the peer's packet echoed when it was not signed,
// else random bytes signed with a key derived from the peer's digest and
// this side's full key.
func answer(peer, peerKey, ownKey []byte) ([]byte, error) {
	out := make([]byte, handshakeSize)
	digest, signed := findDigest(peer, peerKey)
	if !signed {
		copy(out, peer)
		return out, nil
	}
	if _, err := rand.Read(out); err != nil {
		return nil, err
	}
	derive := hmac.New(sha256.New, append(bytes.Clone(ownKey), keyTail...))
	derive.Write(digest)
	mac := hmac.New(sha256.New, derive.Sum(nil))
	mac.Write(out[:handshakeSize-digestSize])
	copy(out[handshakeSize-digestSize:], mac.Sum(nil))
	return out, nil
}

// clientHandshake runs the client side: a signed C0+C1, then C2 once
// S0+S1+S2 arrive (the server sends S2 on C1 alone). S2 is not checked:
// servers echo C1 or sign it, and both are in use.
func clientHandshake(rw io.ReadWriter) error {
	c0c1 := make([]byte, 1+handshakeSize)
	c0c1[0] = rtmpVersion
	if _, err := sign(c0c1[1:], clientVersion, playerKey); err != nil {
		return err
	}
	if _, err := rw.Write(c0c1); err != nil {
		return err
	}
	s0s1 := make([]byte, 1+2*handshakeSize)
	if _, err := io.ReadFull(rw, s0s1); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if s0s1[0] != rtmpVersion {
		return fmt.Errorf("%w: the server speaks RTMP version %d, not 3", errProtocol, s0s1[0])
	}
	c2, err := answer(s0s1[1:1+handshakeSize], serverKey, playerKey)
	if err != nil {
		return err
	}
	_, err = rw.Write(c2)
	return err
}

// serverHandshake runs the server side: S0+S1+S2 once C0+C1 arrive, signed
// when C1 was, then C2. An encrypted (RTMPE, version 6) handshake is
// refused.
func serverHandshake(rw io.ReadWriter) error {
	c0c1 := make([]byte, 1+handshakeSize)
	if _, err := io.ReadFull(rw, c0c1); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if c0c1[0] != rtmpVersion {
		return fmt.Errorf("%w: the client asks for RTMP version %d; this server speaks 3 (plain or over TLS)", errProtocol, c0c1[0])
	}
	c1 := c0c1[1:]
	out := make([]byte, 1, 1+2*handshakeSize)
	out[0] = rtmpVersion
	s1 := make([]byte, handshakeSize)
	if _, signed := findDigest(c1, playerKey); signed {
		if _, err := sign(s1, serverVersion, serverKey); err != nil {
			return err
		}
	} else if _, err := rand.Read(s1[8:]); err != nil {
		return err
	}
	s2, err := answer(c1, playerKey, serverKey)
	if err != nil {
		return err
	}
	if _, err := rw.Write(append(append(out, s1...), s2...)); err != nil {
		return err
	}
	if _, err := io.ReadFull(rw, make([]byte, handshakeSize)); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	return nil
}
