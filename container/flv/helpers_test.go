package flv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// demuxed is everything a demuxer produced: its streams, then each packet
// and event in order.
type demuxed struct {
	streams []av.Stream
	packets []av.Packet
	events  []av.Event
	// order interleaves both: an event at events[i] is "e", a packet "p".
	order string
}

func demuxBytes(t testing.TB, data []byte) demuxed {
	t.Helper()
	out, err := demux(data)
	if err != nil {
		t.Fatalf("demux: %v", err)
	}
	return out
}

func demux(data []byte) (demuxed, error) {
	d := &Demuxer{}
	if err := d.Open(context.Background(), format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		return demuxed{}, err
	}
	out := demuxed{streams: d.Streams()}
	res := format.ReadResult{Packet: &av.Packet{}, Events: make([]av.Event, 0, 1)}
	for {
		res.Reset()
		if err := d.ReadInto(context.Background(), &res); err != nil {
			if errors.Is(err, io.EOF) {
				return out, d.Close()
			}
			return out, err
		}
		for _, event := range res.Events {
			out.events = append(out.events, event)
			out.order += "e"
		}
		if res.PacketReady {
			packet := *res.Packet
			packet.Payload.Bytes = bytes.Clone(packet.Payload.Bytes)
			out.packets = append(out.packets, packet)
			out.order += "p"
		}
	}
}

func (d demuxed) count(id av.StreamID) int {
	n := 0
	for _, p := range d.packets {
		if p.StreamID == id {
			n++
		}
	}
	return n
}

func fixture(t testing.TB, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name + ".flv")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// muxPackets muxes packets of streams into FLV bytes.
func muxPackets(t testing.TB, streams []av.Stream, packets []av.Packet) []byte {
	t.Helper()
	var out bytes.Buffer
	m := &Muxer{}
	if err := m.Open(context.Background(), format.Output{Writer: &out}, streams, format.OpenOptions{}); err != nil {
		t.Fatalf("mux open: %v", err)
	}
	for i := range packets {
		if err := m.Write(context.Background(), &packets[i], nil); err != nil {
			t.Fatalf("mux write %d: %v", i, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("mux close: %v", err)
	}
	return out.Bytes()
}

// rawTag builds one tag the way a foreign muxer might.
func rawTag(tagType byte, timestamp uint32, body []byte) []byte {
	tag := appendTagHeader(nil, tagType, len(body), timestamp)
	tag = append(tag, body...)
	return binary.BigEndian.AppendUint32(tag, uint32(len(tag)))
}

// rawFile is an FLV header with flags, then tags.
func rawFile(flags byte, tags ...[]byte) []byte {
	out := []byte{'F', 'L', 'V', 1, flags, 0, 0, 0, 9, 0, 0, 0, 0}
	for _, tag := range tags {
		out = append(out, tag...)
	}
	return out
}

// validateFLV checks the structure of muxed FLV bytes on its own reading of
// the spec, sharing no code with the muxer: the header and its flags, each
// tag's size against its PreviousTagSize, onMetaData first, a sequence start
// before each track's first frame, and decode times that never go back
// within a track.
func validateFLV(t testing.TB, data []byte) {
	t.Helper()
	if len(data) < 13 || string(data[:3]) != "FLV" || data[3] != 1 || binary.BigEndian.Uint32(data[5:]) != 9 || binary.BigEndian.Uint32(data[9:]) != 0 {
		t.Fatalf("bad FLV header % x", data[:min(len(data), 13)])
	}
	flags := data[4]
	type key struct {
		media byte
		track byte
	}
	configured := map[key]bool{}
	last := map[key]uint32{}
	var sawAudio, sawVideo bool
	pos := 13
	for index := 0; pos < len(data); index++ {
		if pos+11 > len(data) {
			t.Fatalf("tag %d: truncated header at %d", index, pos)
		}
		h := data[pos : pos+11]
		tagType := h[0]
		size := int(h[1])<<16 | int(h[2])<<8 | int(h[3])
		ts := uint32(h[7])<<24 | uint32(h[4])<<16 | uint32(h[5])<<8 | uint32(h[6])
		if h[8] != 0 || h[9] != 0 || h[10] != 0 {
			t.Fatalf("tag %d: stream id not zero", index)
		}
		end := pos + 11 + size
		if end+4 > len(data) {
			t.Fatalf("tag %d: body runs past the file", index)
		}
		if prev := binary.BigEndian.Uint32(data[end:]); int(prev) != 11+size {
			t.Fatalf("tag %d: PreviousTagSize %d, want %d", index, prev, 11+size)
		}
		body := data[pos+11 : end]
		switch tagType {
		case tagScript:
			if index != 0 {
				t.Fatalf("tag %d: script data after the first tag", index)
			}
			if !bytes.HasPrefix(body, []byte{2, 0, 10, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', 8}) {
				t.Fatalf("first tag is not onMetaData with an ECMA array")
			}
		case tagVideo, tagAudio:
			if index == 0 {
				t.Fatalf("media before onMetaData")
			}
			if len(body) == 0 {
				t.Fatalf("tag %d: empty media tag", index)
			}
			k := key{media: tagType}
			config, end := false, false
			if tagType == tagVideo {
				sawVideo = true
				if body[0]&0x80 == 0 { // legacy AVC
					if body[0]&0x0f != 7 || len(body) < 5 {
						t.Fatalf("tag %d: legacy video codec %d", index, body[0]&0x0f)
					}
					config, end = body[1] == 0, body[1] == 2
				} else {
					packetType := body[0] & 0x0f
					if packetType == 6 { // multitrack, one track
						if body[1]>>4 != 0 || len(body) < 7 {
							t.Fatalf("tag %d: multitrack layout %d", index, body[1]>>4)
						}
						packetType, k.track = body[1]&0x0f, body[6]
					}
					config, end = packetType == 0, packetType == 2
				}
			} else {
				sawAudio = true
				switch body[0] >> 4 {
				case 10:
					if body[0] != 0xaf || len(body) < 2 {
						t.Fatalf("tag %d: AAC header % x", index, body[:min(len(body), 2)])
					}
					config = body[1] == 0
				case 9:
					packetType := body[0] & 0x0f
					if packetType == 5 {
						if body[1]>>4 != 0 || len(body) < 7 {
							t.Fatalf("tag %d: audio multitrack layout %d", index, body[1]>>4)
						}
						packetType, k.track = body[1]&0x0f, body[6]
					}
					config, end = packetType == 0, packetType == 2
				default:
					t.Fatalf("tag %d: sound format %d", index, body[0]>>4)
				}
			}
			switch {
			case config:
				configured[k] = true
			case !configured[k]:
				t.Fatalf("tag %d: frame of media %d track %d before its sequence start", index, k.media, k.track)
			case end:
			default:
				if previous, ok := last[k]; ok && ts < previous {
					t.Fatalf("tag %d: decode time %d after %d", index, ts, previous)
				}
				last[k] = ts
			}
		default:
			t.Fatalf("tag %d: type %d", index, tagType)
		}
		pos = end + 4
	}
	if sawVideo != (flags&flagVideo != 0) || sawAudio != (flags&flagAudio != 0) {
		t.Fatalf("header flags %#x, saw video %v audio %v", flags, sawVideo, sawAudio)
	}
}
