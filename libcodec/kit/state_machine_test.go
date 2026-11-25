package kit

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestVideoDecoderUnopenedAndFailedStartNoPanic(t *testing.T) {
	openErr := errors.New("backend open failed")
	calls := 0
	c := &Codec{
		ID:        av.CodecAV1,
		Media:     av.MediaVideo,
		Backend:   "fake",
		Bitstream: Raw,
		NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) {
			calls++
			if calls == 1 || calls == 3 {
				return nil, openErr
			}
			return &decodeOrder{y: make([]byte, 16), c: make([]byte, 4)}, nil
		},
	}
	d := &videoDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
	pkt := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}, Keyframe: true}
	var out codec.DecodeResult

	// Never opened: must return ErrClosed, never panic.
	if err := d.DecodeInto(t.Context(), &pkt, &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("unopened DecodeInto = %v, want ErrClosed", err)
	}
	if err := d.FlushInto(t.Context(), &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("unopened FlushInto = %v, want ErrClosed", err)
	}
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventDiscontinuity}); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("unopened HandleEvent = %v, want ErrClosed", err)
	}

	// Open fails: state remains unopened, DecodeInto returns ErrClosed.
	cfg := codec.DecodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecAV1}}}
	if err := d.Open(t.Context(), cfg); !errors.Is(err, openErr) {
		t.Fatalf("failed Open = %v, want %v", err, openErr)
	}
	if d.state != vdecUnopened {
		t.Fatalf("state after failed Open = %v, want vdecUnopened", d.state)
	}
	if err := d.DecodeInto(t.Context(), &pkt, &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("DecodeInto after failed Open = %v, want ErrClosed", err)
	}

	// Open succeeds (call 2), then restart on discontinuity fails (call 3).
	if err := d.Open(t.Context(), cfg); err != nil {
		t.Fatalf("second Open = %v", err)
	}
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventDiscontinuity}); !errors.Is(err, openErr) {
		t.Fatalf("failed restart = %v, want %v", err, openErr)
	}
	if d.state != vdecUnopened || d.session != nil {
		t.Fatalf("after failed restart: state=%v session=%v", d.state, d.session)
	}
	if err := d.DecodeInto(t.Context(), &pkt, &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("DecodeInto after failed restart = %v, want ErrClosed", err)
	}
}

func TestVideoDecoderReopenResetsState(t *testing.T) {
	c := &Codec{
		ID:        av.CodecAV1,
		Media:     av.MediaVideo,
		Backend:   "fake",
		Bitstream: Raw,
		Reorder:   true,
		NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) {
			return &decodeOrder{y: make([]byte, 16), c: make([]byte, 4)}, nil
		},
	}
	d := &videoDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
	base := av.TimeBase{Num: 1, Den: 1000}
	cfg := codec.DecodeConfig{Stream: av.Stream{TimeBase: base, Codec: av.CodecParameters{ID: av.CodecAV1}}}
	if err := d.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	// Populate held/pending/decoded, then enter dropping and Close.
	var out codec.DecodeResult
	heldPkt := av.Packet{
		Payload:  av.Buffer{Bytes: []byte{1}},
		PTS:      av.Timestamp{Value: 100, Base: base},
		DTS:      av.Timestamp{Value: 90, Base: base},
		Keyframe: true,
	}
	if err := d.DecodeInto(t.Context(), &heldPkt, &out); err != nil {
		t.Fatal(err)
	}
	if len(d.held) != 1 {
		t.Fatalf("expected 1 held frame, got %d", len(d.held))
	}
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventPacketLoss}); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: all transient state must be reset and non-keyframes must decode immediately.
	if err := d.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.state != vdecRunning || len(d.held) != 0 || len(d.pending) != 0 || d.decoded != 0 || d.fedSets {
		t.Fatalf("stale state after reopen: state=%v held=%d pending=%d decoded=%d fedSets=%v",
			d.state, len(d.held), len(d.pending), d.decoded, d.fedSets)
	}
	nonKey := av.Packet{
		Payload:  av.Buffer{Bytes: []byte{2}},
		PTS:      av.Timestamp{Value: 200, Base: base},
		DTS:      av.Timestamp{Value: 200, Base: base},
		Keyframe: false,
	}
	out.Reset()
	if err := d.DecodeInto(t.Context(), &nonKey, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Frames) != 1 || out.Frames[0].PTS.Value != 200 {
		releaseFrames(out.Frames)
		t.Fatalf("non-keyframe after reopen was dropped or wrong: %+v", out.Frames)
	}
	releaseFrames(out.Frames)
}

func TestVideoDecoderAVCCDroppingPreservedWithoutParamSets(t *testing.T) {
	c := &Codec{
		ID:        av.CodecH264,
		Media:     av.MediaVideo,
		Backend:   "fake",
		Bitstream: AVCC,
		NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) {
			return &decodeOrder{y: make([]byte, 16), c: make([]byte, 4)}, nil
		},
	}
	d := &videoDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
	base := av.TimeBase{Num: 1, Den: 1000}
	cfg := codec.DecodeConfig{
		Stream: av.Stream{TimeBase: base, Codec: av.CodecParameters{ID: av.CodecH264}},
	}
	cfg.Resilience.RequestKeyframes = true
	if err := d.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.state != vdecAwaitingParamSets {
		t.Fatalf("initial AVCC state = %v, want vdecAwaitingParamSets", d.state)
	}
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventPacketLoss}); err != nil {
		t.Fatal(err)
	}
	if d.state != vdecDropping {
		t.Fatalf("after packet loss state = %v, want vdecDropping", d.state)
	}

	// Keyframe without SPS/PPS: must request keyframe and remain in vdecDropping.
	var out codec.DecodeResult
	idrOnly := av.Packet{
		Payload:  av.Buffer{Bytes: appendAnnexB(nil, testIDR)},
		PTS:      av.Timestamp{Value: 10, Base: base},
		Keyframe: true,
	}
	if err := d.DecodeInto(t.Context(), &idrOnly, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Frames) != 0 || len(out.Requests) != 1 {
		t.Fatalf("expected 0 frames and 1 keyframe request, got %d frames, %d requests", len(out.Frames), len(out.Requests))
	}
	if d.state != vdecDropping {
		t.Fatalf("state cleared prematurely to %v without parameter sets", d.state)
	}

	// Non-keyframe slice (even carrying SPS/PPS) must still be dropped while in vdecDropping.
	nonIDR := []byte{0x41, 0x9a, 0x01}
	out.Reset()
	pFrame := av.Packet{
		Payload:  av.Buffer{Bytes: appendAnnexB(nil, testSPS, testPPS, nonIDR)},
		PTS:      av.Timestamp{Value: 20, Base: base},
		Keyframe: false,
	}
	if err := d.DecodeInto(t.Context(), &pFrame, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Frames) != 0 || d.state != vdecDropping {
		releaseFrames(out.Frames)
		t.Fatalf("non-keyframe should be dropped while vdecDropping: frames=%d state=%v", len(out.Frames), d.state)
	}

	// Keyframe with SPS/PPS+IDR opens session and transitions to vdecRunning.
	out.Reset()
	fullKey := av.Packet{
		Payload:  av.Buffer{Bytes: appendAnnexB(nil, testSPS, testPPS, testIDR)},
		PTS:      av.Timestamp{Value: 30, Base: base},
		Keyframe: true,
	}
	if err := d.DecodeInto(t.Context(), &fullKey, &out); err != nil {
		t.Fatal(err)
	}
	if d.state != vdecRunning || len(out.Frames) != 1 || out.Frames[0].PTS.Value != 30 {
		releaseFrames(out.Frames)
		t.Fatalf("expected decoded keyframe and vdecRunning, got state=%v frames=%d", d.state, len(out.Frames))
	}
	releaseFrames(out.Frames)
}

func TestVideoDecoderCodecChangedValidatesBeforeDiscard(t *testing.T) {
	closed := 0
	c := &Codec{
		ID:        av.CodecH264,
		Media:     av.MediaVideo,
		Backend:   "fake",
		Bitstream: AVCC,
		NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) {
			return &transitionDecoder{
				decodeOrder: decodeOrder{y: make([]byte, 16), c: make([]byte, 4)},
				closes:      &closed,
				flushes:     new(int),
				owner:       &countingOwner{refs: 1},
			}, nil
		},
	}
	base := av.TimeBase{Num: 1, Den: 1000}
	stream := av.Stream{
		ID:       "v",
		Epoch:    1,
		TimeBase: base,
		Codec: av.CodecParameters{
			ID:        av.CodecH264,
			Width:     4,
			Height:    4,
			ExtraData: av.Buffer{Bytes: avcC([][]byte{testSPS, testPPS})},
		},
	}
	d := &videoDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
	if err := d.Open(t.Context(), codec.DecodeConfig{Stream: stream}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	origSession := d.session

	// Incompatible codec switch must fail WITHOUT destroying live session.
	badCodec := stream.Codec
	badCodec.ID = av.CodecHEVC
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Codec: &badCodec, Epoch: 2}); !errors.Is(err, codec.ErrUnsupportedCodecSwitch) {
		t.Fatalf("codec switch = %v, want ErrUnsupportedCodecSwitch", err)
	}
	if closed != 0 || d.session != origSession || d.state != vdecRunning || d.stream.Epoch != 1 {
		t.Fatalf("invalid codec switch mutated live session: closed=%d same=%v state=%v epoch=%d",
			closed, d.session == origSession, d.state, d.stream.Epoch)
	}

	// Malformed extradata must fail WITHOUT destroying live session.
	badExtra := stream.Codec
	badExtra.ExtraData = av.Buffer{Bytes: []byte{0x01, 0x64}} // truncated avcC
	if err := d.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Codec: &badExtra, Epoch: 3}); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("bad extradata = %v, want ErrUnsupportedFormat", err)
	}
	if closed != 0 || d.session != origSession || d.state != vdecRunning || d.stream.Epoch != 1 {
		t.Fatalf("bad extradata mutated live session: closed=%d same=%v state=%v epoch=%d",
			closed, d.session == origSession, d.state, d.stream.Epoch)
	}

	// Live session must still decode cleanly.
	var out codec.DecodeResult
	pkt := av.Packet{
		Payload:  av.Buffer{Bytes: appendAnnexB(nil, testIDR)},
		PTS:      av.Timestamp{Value: 100, Base: base},
		Keyframe: true,
	}
	if err := d.DecodeInto(t.Context(), &pkt, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Frames) != 1 || out.Frames[0].PTS.Value != 100 {
		releaseFrames(out.Frames)
		t.Fatalf("live session failed after rejected codec change: frames=%d", len(out.Frames))
	}
	releaseFrames(out.Frames)
}

type fakeVideoEncSession struct {
	config      []byte
	flushPacket *Packet
	encoded     []Picture
	closed      int
}

func (s *fakeVideoEncSession) Config() []byte { return s.config }
func (s *fakeVideoEncSession) Encode(pic *Picture, emit func(*Packet) error) error {
	s.encoded = append(s.encoded, *pic)
	if s.flushPacket == nil {
		return emit(&Packet{Data: []byte{1}, PTS: pic.PTS, DTS: pic.PTS, Duration: pic.Duration, Keyframe: pic.Keyframe})
	}
	return nil
}
func (s *fakeVideoEncSession) Flush(emit func(*Packet) error) error {
	if s.flushPacket != nil {
		p := s.flushPacket
		s.flushPacket = nil
		return emit(p)
	}
	return nil
}
func (s *fakeVideoEncSession) Close() error { s.closed++; return nil }

func makeI420Frame(w, h int, format string, pts int64, meta av.Metadata) *av.Frame {
	return &av.Frame{
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: pts, Base: av.TimeBase{Num: 1, Den: 1000}},
		Metadata: meta,
		Video:    &av.VideoFrame{Width: w, Height: h, PixelFormat: format},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: make([]byte, w*h)}, Stride: w},
			{Buffer: av.Buffer{Bytes: make([]byte, (w/2)*(h/2))}, Stride: w / 2},
			{Buffer: av.Buffer{Bytes: make([]byte, (w/2)*(h/2))}, Stride: w / 2},
		},
	}
}

func TestVideoEncoderFailedKnobRestartCleansCarryAndCloses(t *testing.T) {
	restartErr := errors.New("reopen refused")
	calls := 0
	var first *fakeVideoEncSession
	speed := codec.KnobSpeed
	speed.Live = false
	speed.Default = "2"
	c := &Codec{
		ID:        av.CodecAV1,
		Media:     av.MediaVideo,
		Backend:   "fake",
		Bitstream: Raw,
		Knobs:     []codec.Knob{speed},
		NewVideoEncoder: func(VideoConfig) (VideoEncoder, error) {
			calls++
			if calls == 1 {
				first = &fakeVideoEncSession{flushPacket: &Packet{Data: []byte{9, 8, 7}, PTS: 10}}
				return first, nil
			}
			return nil, restartErr
		},
	}
	e := &videoEncoder{c: c, desc: c.descriptor(codec.ModeEncode)}
	cfg := codec.EncodeConfig{
		Stream:     av.Stream{TimeBase: av.TimeBase{Num: 1, Den: 1000}},
		Parameters: av.CodecParameters{Width: 4, Height: 4},
	}
	if err := e.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	ev := &av.Event{Type: EventSetting, Metadata: av.Metadata{"speed": "4"}}
	if err := e.HandleEvent(t.Context(), ev); !errors.Is(err, restartErr) {
		t.Fatalf("HandleEvent = %v, want %v", err, restartErr)
	}
	if e.state != encClosed || e.session != nil || len(e.carry.Packets) != 0 || first.closed != 1 {
		t.Fatalf("after failed restart: state=%v session=%v carry=%d closed=%d",
			e.state, e.session, len(e.carry.Packets), first.closed)
	}
	var out codec.EncodeResult
	if err := e.EncodeInto(t.Context(), makeI420Frame(4, 4, av.PixelFormatI420, 20, nil), &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("EncodeInto after failed restart = %v, want ErrClosed", err)
	}
	if err := e.FlushInto(t.Context(), &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("FlushInto after failed restart = %v, want ErrClosed", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close after failed restart = %v", err)
	}
}

func TestVideoEncoderReopenResetsPendingAndSets(t *testing.T) {
	calls := 0
	c := &Codec{
		ID:        av.CodecH264,
		Media:     av.MediaVideo,
		Backend:   "fake",
		Bitstream: AnnexB,
		NewVideoEncoder: func(VideoConfig) (VideoEncoder, error) {
			calls++
			if calls == 1 {
				// First session supplies parameter sets and buffers (doesn't emit on Encode).
				return &fakeVideoEncSession{
					config:      avcC([][]byte{testSPS, testPPS}),
					flushPacket: &Packet{Data: appendAnnexB(nil, testIDR), PTS: 1},
				}, nil
			}
			return &fakeVideoEncSession{}, nil
		},
	}
	e := &videoEncoder{c: c, desc: c.descriptor(codec.ModeEncode)}
	cfg := codec.EncodeConfig{
		Stream:     av.Stream{TimeBase: av.TimeBase{Num: 1, Den: 1000}},
		Parameters: av.CodecParameters{Width: 4, Height: 4},
	}
	if err := e.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	var out codec.EncodeResult
	if err := e.EncodeInto(t.Context(), makeI420Frame(4, 4, av.PixelFormatI420, 10, nil), &out); err != nil {
		t.Fatal(err)
	}
	if len(e.pending) == 0 || len(e.sets) == 0 {
		t.Fatalf("expected non-empty pending (%d) and sets (%d) before reopen", len(e.pending), len(e.sets))
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.state != encOpen || len(e.pending) != 0 || len(e.sets) != 0 {
		t.Fatalf("reopen leaked state: state=%v pending=%d sets=%d", e.state, len(e.pending), len(e.sets))
	}
}

func TestVideoEncoderInvalidPixelFormatPreservesForceKeyAndPending(t *testing.T) {
	var sess *fakeVideoEncSession
	c := &Codec{
		ID:        av.CodecAV1,
		Media:     av.MediaVideo,
		Backend:   "fake",
		Bitstream: Raw,
		Pixels:    av.PixelFormatI420,
		NewVideoEncoder: func(VideoConfig) (VideoEncoder, error) {
			sess = &fakeVideoEncSession{}
			return sess, nil
		},
	}
	e := &videoEncoder{c: c, desc: c.descriptor(codec.ModeEncode)}
	cfg := codec.EncodeConfig{
		Stream:     av.Stream{TimeBase: av.TimeBase{Num: 1, Den: 1000}},
		Parameters: av.CodecParameters{Width: 4, Height: 4},
	}
	if err := e.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Bad pixel format: must fail without consuming forceKey or pushing pending.
	var out codec.EncodeResult
	badFrame := &av.Frame{
		Type:  av.MediaVideo,
		PTS:   av.Timestamp{Value: 10, Base: av.TimeBase{Num: 1, Den: 1000}},
		Video: &av.VideoFrame{Width: 4, Height: 4, PixelFormat: NV12},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: make([]byte, 16)}, Stride: 4},
			{Buffer: av.Buffer{Bytes: make([]byte, 8)}, Stride: 4},
		},
	}
	if err := e.EncodeInto(t.Context(), badFrame, &out); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("bad pixel format = %v, want ErrUnsupportedFormat", err)
	}
	if !e.forceKey || len(e.pending) != 0 {
		t.Fatalf("bad pixel format mutated state: forceKey=%v pending=%d", e.forceKey, len(e.pending))
	}

	// Next valid frame must still be encoded as a keyframe with clean pending timing.
	goodMeta := av.Metadata{"tag": "ok"}
	if err := e.EncodeInto(t.Context(), makeI420Frame(4, 4, av.PixelFormatI420, 20, goodMeta), &out); err != nil {
		t.Fatal(err)
	}
	if len(sess.encoded) != 1 || !sess.encoded[0].Keyframe {
		t.Fatalf("valid frame did not get keyframe: %+v", sess.encoded)
	}
	if len(out.Packets) != 1 || out.Packets[0].Metadata["tag"] != "ok" {
		t.Fatalf("unexpected output packet: %+v", out.Packets)
	}
	releasePackets(out.Packets)
}

type fakeAudioEncSession struct {
	flushPacket *Packet
	closed      int
}

func (s *fakeAudioEncSession) Encode(*Samples, func(*Packet) error) error { return nil }
func (s *fakeAudioEncSession) Flush(emit func(*Packet) error) error {
	if s.flushPacket != nil {
		p := s.flushPacket
		s.flushPacket = nil
		return emit(p)
	}
	return nil
}
func (s *fakeAudioEncSession) Close() error { s.closed++; return nil }

func TestAudioEncoderFailedKnobRestartCleansCarryAndCloses(t *testing.T) {
	restartErr := errors.New("audio reopen refused")
	calls := 0
	var first *fakeAudioEncSession
	speed := codec.KnobSpeed
	speed.Live = false
	speed.Default = "2"
	c := &Codec{
		ID:      av.CodecAAC,
		Media:   av.MediaAudio,
		Backend: "fake",
		Knobs:   []codec.Knob{speed},
		NewAudioEncoder: func(AudioConfig) (AudioEncoder, error) {
			calls++
			if calls == 1 {
				first = &fakeAudioEncSession{flushPacket: &Packet{Data: []byte{1, 2, 3}, PTS: 0, Duration: 1024}}
				return first, nil
			}
			return nil, restartErr
		},
	}
	e := &audioEncoder{c: c, desc: c.descriptor(codec.ModeEncode)}
	cfg := codec.EncodeConfig{
		Parameters: av.CodecParameters{SampleRate: 48000, Channels: 2},
	}
	if err := e.Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	ev := &av.Event{Type: EventSetting, Metadata: av.Metadata{"speed": "4"}}
	if err := e.HandleEvent(t.Context(), ev); !errors.Is(err, restartErr) {
		t.Fatalf("HandleEvent = %v, want %v", err, restartErr)
	}
	if e.state != encClosed || e.session != nil || len(e.carry.Packets) != 0 || first.closed != 1 {
		t.Fatalf("after failed audio restart: state=%v session=%v carry=%d closed=%d",
			e.state, e.session, len(e.carry.Packets), first.closed)
	}
	var out codec.EncodeResult
	if err := e.EncodeInto(t.Context(), nil, &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("EncodeInto after failed restart = %v, want ErrClosed", err)
	}
	if err := e.FlushInto(t.Context(), &out); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("FlushInto after failed restart = %v, want ErrClosed", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close after failed restart = %v", err)
	}
}
