package kit

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

type transitionAudioDecoder struct {
	config          AudioConfig
	pending         *Samples
	closed, flushed *int
}

func (s *transitionAudioDecoder) Decode(p *Packet, emit func(*Samples) error) error {
	// A one-packet delay makes stale output and EOF drain observable.
	if s.pending != nil {
		if err := emit(s.pending); err != nil {
			return err
		}
	}
	s.pending = &Samples{PCM: make([]int16, 16*s.config.Channels), SampleRate: s.config.SampleRate, Channels: s.config.Channels, PTS: p.PTS}
	return nil
}
func (s *transitionAudioDecoder) Flush(emit func(*Samples) error) error {
	*s.flushed++
	if s.pending == nil {
		return nil
	}
	p := s.pending
	s.pending = nil
	return emit(p)
}
func (s *transitionAudioDecoder) Close() error { *s.closed++; s.pending = nil; return nil }

func TestAudioDecoderTransitions(t *testing.T) {
	opened, closed, flushed := 0, 0, 0
	refused := errors.New("unsupported audio configuration")
	var configs []AudioConfig
	c := &Codec{ID: av.CodecAAC, Media: av.MediaAudio, Backend: "fake",
		NewAudioDecoder: func(config AudioConfig) (AudioDecoder, error) {
			if config.SampleRate == 123 {
				return nil, refused
			}
			opened++
			configs = append(configs, config)
			return &transitionAudioDecoder{config: config, closed: &closed, flushed: &flushed}, nil
		}}
	stream := av.Stream{ID: "audio", Type: av.MediaAudio, Epoch: 1, TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{ID: av.CodecAAC, SampleRate: 48000, Channels: 2, ChannelLayout: "stereo", ExtraData: av.Buffer{Bytes: []byte{0x11, 0x90}}}}
	d := &audioDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
	if err := d.Open(t.Context(), codec.DecodeConfig{Stream: stream}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	decode := func(ms int64, discontinuous bool) []av.Frame {
		t.Helper()
		var out codec.DecodeResult
		p := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}, PTS: av.Timestamp{Value: ms, Base: av.TimeBase{Num: 1, Den: 1000}}, Discontinuous: discontinuous}
		if err := d.DecodeInto(t.Context(), &p, &out); err != nil {
			t.Fatal(err)
		}
		return out.Frames
	}
	for i := range 8 {
		releaseFrames(decode(100, false))
		next := stream
		next.ID = "replacement"
		next.Epoch = av.Epoch(i + 2)
		next.TimeBase = av.TimeBase{Num: 1, Den: 1000}
		next.Codec.SampleRate, next.Codec.Channels, next.Codec.ChannelLayout = 44100, 1, "mono"
		next.Codec.ExtraData = av.Buffer{Bytes: []byte{0x12, 0x08}}
		if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Stream: &next, Epoch: next.Epoch}); err != nil {
			t.Fatal(err)
		}
		if frames := decode(200, false); len(frames) != 0 {
			releaseFrames(frames)
			t.Fatal("old samples survived codec change")
		}
		frames := decode(300, false)
		if len(frames) != 1 {
			t.Fatalf("frames=%d", len(frames))
		}
		frame := frames[0]
		if frame.StreamID != next.ID || frame.CodecEpoch != next.Epoch || frame.Audio.SampleRate != 44100 || frame.Audio.Channels != 1 || frame.Audio.ChannelLayout != "mono" || frame.PTS.Value != 8820 || frame.PTS.Base.Den != 44100 {
			t.Fatalf("wrong audio transition: %+v audio=%+v", frame, frame.Audio)
		}
		releaseFrames(frames)
		if got := configs[len(configs)-1]; got.SampleRate != 44100 || got.Channels != 1 || string(got.ExtraData) != string(next.Codec.ExtraData.Bytes) {
			t.Fatalf("config=%+v", got)
		}
		if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventDiscontinuity}); err != nil {
			t.Fatal(err)
		}
		if frames := decode(400, false); len(frames) != 0 {
			releaseFrames(frames)
			t.Fatal("seek drained old samples")
		}
		if frames := decode(500, true); len(frames) != 0 {
			releaseFrames(frames)
			t.Fatal("packet discontinuity drained old samples")
		}
		var out codec.DecodeResult
		if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventEndOfStream}); err != nil {
			t.Fatal(err)
		}
		if err := d.FlushInto(t.Context(), &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Frames) != 1 || out.Frames[0].PTS.Value != 22050 {
			t.Fatal("EOS did not drain valid samples")
		}
		releaseFrames(out.Frames)
		stream = next
	}
	before := d.stream
	bad := stream.Codec
	bad.SampleRate = 123
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Codec: &bad, Epoch: 99}); !errors.Is(err, refused) {
		t.Fatalf("refusal=%v", err)
	}
	if d.stream.Epoch != before.Epoch || d.config.SampleRate != 44100 {
		t.Fatal("rejected configuration was committed")
	}
	bad.ID = av.CodecOpus
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Codec: &bad}); !errors.Is(err, codec.ErrUnsupportedCodecSwitch) {
		t.Fatalf("codec switch=%v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if opened != closed || flushed != 8 {
		t.Fatalf("sessions open=%d close=%d flush=%d", opened, closed, flushed)
	}
}
