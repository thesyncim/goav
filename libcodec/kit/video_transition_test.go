package kit

import (
	"fmt"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

type transitionDecoder struct {
	decodeOrder
	closes, flushes *int
	owner           *countingOwner
}

func (s *transitionDecoder) Decode(unit *Packet, emit func(*Picture) error) error {
	return emit(&Picture{Width: 4, Height: 4, Format: av.PixelFormatI420, Planes: [3][]byte{s.y, s.c, s.c}, Strides: [3]int{4, 2, 2}, PTS: unit.PTS, Owner: s.owner})
}
func (s *transitionDecoder) Close() error                     { *s.closes++; s.owner.Release(); return nil }
func (s *transitionDecoder) Flush(func(*Picture) error) error { *s.flushes++; return nil }

func TestVideoDecoderTransitions(t *testing.T) {
	for _, framing := range []Bitstream{Raw, AVCC} {
		t.Run(fmt.Sprint(framing), func(t *testing.T) {
			opened, closed, flushed := 0, 0, 0
			var configs []VideoConfig
			var owners []*countingOwner
			id := av.CodecAV1
			if framing == AVCC {
				id = av.CodecH264
			}
			c := &Codec{ID: id, Media: av.MediaVideo, Backend: "fake", Bitstream: framing, Reorder: true,
				NewVideoDecoder: func(config VideoConfig) (VideoDecoder, error) {
					opened++
					configs = append(configs, config)
					owner := &countingOwner{refs: 1}
					owners = append(owners, owner)
					return &transitionDecoder{decodeOrder: decodeOrder{y: make([]byte, 16), c: make([]byte, 4)}, closes: &closed, flushes: &flushed, owner: owner}, nil
				}}
			base := av.TimeBase{Num: 1, Den: 1000}
			stream := av.Stream{ID: "v", Epoch: 1, TimeBase: base, Codec: av.CodecParameters{ID: id, Width: 4, Height: 4}}
			if framing == AVCC {
				stream.Codec.ExtraData = av.Buffer{Bytes: avcC([][]byte{testSPS, testPPS})}
			}
			d := &videoDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
			if err := d.Open(t.Context(), codec.DecodeConfig{Stream: stream}); err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			payload := []byte{1}
			if framing == AVCC {
				payload = appendAnnexB(nil, testIDR)
			}
			decode := func(pts, dts int64, discontinuous bool) []av.Frame {
				t.Helper()
				packet := av.Packet{Payload: av.Buffer{Bytes: payload}, Keyframe: true, Discontinuous: discontinuous,
					PTS: av.Timestamp{Value: pts, Base: d.stream.TimeBase}, DTS: av.Timestamp{Value: dts, Base: d.stream.TimeBase}}
				var out codec.DecodeResult
				if err := d.DecodeInto(t.Context(), &packet, &out); err != nil {
					t.Fatal(err)
				}
				return out.Frames
			}
			for i := range 8 {
				frames := decode(100, 90, false)
				if len(frames) != 0 {
					releaseFrames(frames)
					t.Fatal("expected held picture")
				}
				if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventDiscontinuity}); err != nil {
					t.Fatal(err)
				}
				frames = decode(200, 200, false)
				if len(frames) != 1 || frames[0].PTS.Value != 200 {
					releaseFrames(frames)
					t.Fatal("pre-seek picture escaped")
				}
				releaseFrames(frames)
				if frames := decode(300, 290, false); len(frames) != 0 {
					releaseFrames(frames)
					t.Fatal("expected held picture")
				}
				stream.Epoch = av.Epoch(i + 2)
				stream.TimeBase = av.TimeBase{Num: 1, Den: 90000}
				stream.Codec.Width = 8
				if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Stream: &stream, Epoch: stream.Epoch}); err != nil {
					t.Fatal(err)
				}
				frames = decode(400, 400, false)
				if len(frames) != 1 || frames[0].CodecEpoch != stream.Epoch || frames[0].PTS.Base != stream.TimeBase {
					releaseFrames(frames)
					t.Fatal("old configuration escaped codec boundary")
				}
				releaseFrames(frames)
				if configs[len(configs)-1].Width != 8 {
					t.Fatal("backend configuration not updated")
				}
			}
			releaseFrames(decode(500, 490, false))
			frames := decode(600, 600, true)
			if len(frames) != 1 || frames[0].PTS.Value != 600 {
				releaseFrames(frames)
				t.Fatal("packet discontinuity retained stale frame")
			}
			releaseFrames(frames)
			releaseFrames(decode(700, 690, false))
			var out codec.DecodeResult
			if err := d.FlushInto(t.Context(), &out); err != nil {
				t.Fatal(err)
			}
			if len(out.Frames) != 1 || out.Frames[0].PTS.Value != 700 {
				t.Fatal("EOS did not drain delayed frame")
			}
			releaseFrames(out.Frames)
			if flushed != 1 {
				t.Fatalf("seek/change drained backend %d times", flushed)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if opened != closed {
				t.Fatalf("sessions opened=%d closed=%d", opened, closed)
			}
			for i, owner := range owners {
				if owner.refs != 0 {
					t.Fatalf("session %d retained %d picture references", i, owner.refs)
				}
			}
		})
	}
}
