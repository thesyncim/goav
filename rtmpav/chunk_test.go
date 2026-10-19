package rtmpav

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
)

// Messages round-trip through the chunk writer and reader: every basic
// header form, bodies across many chunks, timestamps past 24 bits on the
// first and on later messages, and a message stream change.
func TestChunkRoundTrip(t *testing.T) {
	var wire bytes.Buffer
	w := newChunkWriter(&wire)
	w.size = 100
	var sent []message
	for i, id := range []uint32{2, 3, 63, 64, 319, 320, 65599} {
		for j, ts := range []uint32{0, 40, 0x00fffffe, 0x01000005, 0xfffffff0} {
			body := bytes.Repeat([]byte{byte(i), byte(j)}, 30+i*70+j)
			msg := message{chunkStream: id, typ: typeVideo, streamID: uint32(1 + j/3), timestamp: ts, body: body}
			if err := w.write(msg); err != nil {
				t.Fatal(err)
			}
			sent = append(sent, msg)
		}
	}
	r := newChunkReader(&wire)
	if err := r.setChunkSize(100); err != nil {
		t.Fatal(err)
	}
	for i, want := range sent {
		got, err := r.read()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("message %d = cs %d type %d stream %d ts %#x %d bytes, want cs %d stream %d ts %#x %d bytes",
				i, got.chunkStream, got.typ, got.streamID, got.timestamp, len(got.body), want.chunkStream, want.streamID, want.timestamp, len(want.body))
		}
	}
	if _, err := r.read(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the messages: %v", err)
	}
}

// Messages on two chunk streams interleave chunk by chunk, as a server
// sends audio between a video frame's chunks; a type 3 header after a
// type 0 starts a message one delta later; a continuation repeats an
// extended timestamp.
func TestChunkReaderInterleavingAndHeaderForms(t *testing.T) {
	var wire []byte
	chunk := func(format byte, id uint32, header []byte, payload []byte) {
		wire = append(wire, format<<6|byte(id))
		wire = append(wire, header...)
		wire = append(wire, payload...)
	}
	header0 := func(ts uint32, length int, typ byte, stream uint32) []byte {
		h := []byte{byte(ts >> 16), byte(ts >> 8), byte(ts), byte(length >> 16), byte(length >> 8), byte(length), typ}
		return binary.LittleEndian.AppendUint32(h, stream)
	}
	video := bytes.Repeat([]byte{'v'}, 200)
	audio := bytes.Repeat([]byte{'a'}, 50)
	chunk(0, 6, header0(1000, len(video), typeVideo, 1), video[:128])
	chunk(0, 4, header0(1000, len(audio), typeAudio, 1), audio)
	chunk(3, 6, nil, video[128:])
	chunk(3, 4, nil, audio) // a new message, one delta (the type 0's timestamp) on
	// An extended timestamp on a type 0, its continuation without it.
	big := bytes.Repeat([]byte{'x'}, 130)
	chunk(0, 5, append(header0(extendedMark, len(big), typeVideo, 1), 0x01, 0x00, 0x00, 0x00), big[:128])
	chunk(3, 5, []byte{0x01, 0, 0, 0}, big[128:])
	r := newChunkReader(bytes.NewReader(wire))
	for _, want := range []struct {
		id   uint32
		ts   uint32
		body []byte
	}{{4, 1000, audio}, {6, 1000, video}, {4, 2000, audio}, {5, 0x01000000, big}} {
		msg, err := r.read()
		if err != nil {
			t.Fatal(err)
		}
		if msg.chunkStream != want.id || msg.timestamp != want.ts || !bytes.Equal(msg.body, want.body) {
			t.Fatalf("message cs %d ts %d %d bytes, want cs %d ts %d %d bytes", msg.chunkStream, msg.timestamp, len(msg.body), want.id, want.ts, len(want.body))
		}
	}
}

// The reader refuses what the protocol forbids, and bounds what a peer
// can make it hold.
func TestChunkReaderRefusals(t *testing.T) {
	big := []byte{0x06, 0, 0, 0, 0xff, 0xff, 0xff, typeVideo, 1, 0, 0, 0}
	for name, wire := range map[string][]byte{
		"continuation first": {0xc6, 1, 2, 3},
		"interrupted message": append(append([]byte{0x06, 0, 0, 0, 0, 1, 0, typeVideo, 1, 0, 0, 0}, make([]byte, 128)...),
			0x46, 0, 0, 0, 0, 0, 1, typeVideo),
	} {
		r := newChunkReader(bytes.NewReader(wire))
		if _, err := r.read(); !errors.Is(err, errProtocol) {
			t.Errorf("%s: %v, want a protocol error", name, err)
		}
	}
	// Many chunk streams, each with most of a 16 MiB message: the reader
	// stops at its bound instead of holding them all.
	var wire []byte
	for id := byte(3); id < 63; id++ {
		h := bytes.Clone(big)
		h[0] = id
		wire = append(wire, h...)
		wire = append(wire, make([]byte, maxChunkSize)...)
	}
	r := newChunkReader(bytes.NewReader(wire))
	if err := r.setChunkSize(maxChunkSize - 1); err != nil {
		t.Fatal(err)
	}
	if _, err := r.read(); !errors.Is(err, errProtocol) {
		t.Fatalf("unbounded assembly: %v", err)
	}
	if err := r.setChunkSize(0); !errors.Is(err, errProtocol) {
		t.Fatalf("chunk size 0: %v", err)
	}
}

// The connection applies the peer's chunk size and window, answers pings,
// and acknowledges what it reads.
func TestConnControl(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ca, cb := newConn(a), newConn(b)
	go func() {
		_ = ca.setChunkSize(64)
		_ = ca.control(typeWindowAckSize, 0, 0, 0, 100)
		_ = ca.userControl(eventPingRequest, 77)
		_ = ca.write(message{chunkStream: csVideo, typ: typeVideo, streamID: 1, timestamp: 5, body: bytes.Repeat([]byte{1}, 300)})
	}()
	got := make(chan message, 1)
	go func() {
		msg, err := cb.read(0)
		if err != nil {
			t.Error(err)
		}
		got <- msg
	}()
	// The ping response and the acknowledgement come back to a.
	var answers []message
	for len(answers) < 2 {
		msg, err := ca.rd.read()
		if err != nil {
			t.Fatal(err)
		}
		answers = append(answers, msg)
	}
	msg := <-got
	if msg.typ != typeVideo || len(msg.body) != 300 || cb.rd.size != 64 {
		t.Fatalf("message %d with %d bytes, chunk size %d", msg.typ, len(msg.body), cb.rd.size)
	}
	if answers[0].typ != typeUserControl || binary.BigEndian.Uint16(answers[0].body) != eventPingResponse || binary.BigEndian.Uint32(answers[0].body[2:]) != 77 {
		t.Fatalf("ping answer %+v", answers[0])
	}
	if answers[1].typ != typeAcknowledgement {
		t.Fatalf("no acknowledgement: %+v", answers[1])
	}
}

// FuzzChunkReader feeds the chunk reader and command decoding arbitrary
// bytes: they must never panic, and every message stays in bounds.
func FuzzChunkReader(f *testing.F) {
	var wire bytes.Buffer
	w := newChunkWriter(&wire)
	_ = w.write(message{chunkStream: 3, typ: typeCommandAMF0, body: []byte{2, 0, 7, 'c', 'o', 'n', 'n', 'e', 'c', 't', 0, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 9}})
	_ = w.write(message{chunkStream: 400, typ: typeVideo, timestamp: 0x01020304, body: make([]byte, 300)})
	f.Add(wire.Bytes())
	f.Add([]byte{0x02, 0, 0, 0, 0, 0, 4, typeSetChunkSize, 0, 0, 0, 0, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := newChunkReader(bytes.NewReader(data))
		for range 64 {
			msg, err := r.read()
			if err != nil {
				return
			}
			if len(msg.body) > maxChunkSize {
				t.Fatalf("a %d-byte message", len(msg.body))
			}
			switch msg.typ {
			case typeSetChunkSize:
				if len(msg.body) >= 4 {
					_ = r.setChunkSize(binary.BigEndian.Uint32(msg.body) & 0x7fffffff)
				}
			case typeAbort:
				if len(msg.body) >= 4 {
					r.abort(binary.BigEndian.Uint32(msg.body))
				}
			case typeCommandAMF0, typeCommandAMF3:
				cmd, err := decodeCommand(msg)
				if err == nil {
					_ = cmd.info()
					_ = cmd.stringArg(0)
				}
			}
			_ = appendTags(nil, msg)
		}
	})
}
