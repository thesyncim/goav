// Package integration runs the RTP-flavored goav recipe, runtime, and
// component tests: every test here drives the root module's public API with
// rtpav inputs, proving the provider seam end to end from the rtpav module's
// side of the boundary.
package integration

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/rtpav"
)

// runtimeRTPReceiver is a scripted rtpav.PacketReader: fixed streams, a fixed
// payload map, and a finite packet feed ending in EOF.
type runtimeRTPReceiver struct {
	streams []av.Stream
	payload rtpav.PayloadMap
	packets []*rtp.Packet
	events  chan av.Event
	index   int
	closed  bool
}

func (r *runtimeRTPReceiver) Streams(context.Context) ([]av.Stream, error) {
	streams := make([]av.Stream, len(r.streams))
	copy(streams, r.streams)
	return streams, nil
}

func (r *runtimeRTPReceiver) PayloadMap() rtpav.PayloadMap {
	return r.payload
}

func (r *runtimeRTPReceiver) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	return packet, nil
}

func (r *runtimeRTPReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *runtimeRTPReceiver) Close() error {
	r.closed = true
	return nil
}

// runtimeRTPErrorReceiver ends the packet feed with an error instead of EOF.
type runtimeRTPErrorReceiver struct {
	runtimeRTPReceiver
	err error
}

func (r *runtimeRTPErrorReceiver) ReadRTP(ctx context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, r.err
	}
	return r.runtimeRTPReceiver.ReadRTP(ctx)
}

// runtimeRTPSwitchReceiver swaps its payload map to a different codec after
// the first packet and announces the change on its event channel.
type runtimeRTPSwitchReceiver struct {
	initial  av.Stream
	updated  av.Stream
	payload  rtpav.PayloadMap
	packets  []*rtp.Packet
	events   chan av.Event
	index    int
	switched bool
	closed   bool
}

func newRuntimeRTPSwitchReceiver(initial av.Stream, updated av.Stream, packets []*rtp.Packet) *runtimeRTPSwitchReceiver {
	return &runtimeRTPSwitchReceiver{
		initial: initial,
		updated: updated,
		payload: rtpav.NewStaticPayloadMap(1, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  initial.Codec,
			MIMEType:    av.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: packets,
		events:  make(chan av.Event, 1),
	}
}

func (r *runtimeRTPSwitchReceiver) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.initial}, nil
}

func (r *runtimeRTPSwitchReceiver) PayloadMap() rtpav.PayloadMap {
	return r.payload
}

func (r *runtimeRTPSwitchReceiver) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	if r.index == 1 && !r.switched {
		r.payload = rtpav.NewStaticPayloadMap(2, []rtpav.PayloadCodec{{
			PayloadType: 97,
			Parameters:  r.updated.Codec,
			MIMEType:    av.MIMEH264,
			ClockRate:   90000,
		}})
		r.events <- av.Event{
			Type:     av.EventCodecChanged,
			StreamID: r.initial.ID,
			Epoch:    r.updated.Epoch,
			Stream:   &r.updated,
			Codec:    &r.updated.Codec,
		}
		r.switched = true
	}
	return packet, nil
}

func (r *runtimeRTPSwitchReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *runtimeRTPSwitchReceiver) Close() error {
	r.closed = true
	return nil
}

func audioOpusTestStream() av.Stream {
	return av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			ClockRate:    48000,
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatS16,
		},
	}
}

// Runtime registry options mirroring the root module's test fixtures: build a
// goav runtime whose codec/format registries carry exactly the test doubles.
func withTestCodecs(configure ...func(*codec.SimpleRegistry)) goav.Option {
	return goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		for i := range configure {
			configure[i](registry)
		}
	})
}

func withTestFormats(configure ...func(*format.SimpleRegistry)) goav.Option {
	return goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		for i := range configure {
			configure[i](registry)
		}
	})
}

func testCodecDecoder(desc codec.Descriptor, factory codec.DecoderFactory) func(*codec.SimpleRegistry) {
	return func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(desc, factory)
	}
}

func testFormatProber(prober format.Prober) func(*format.SimpleRegistry) {
	return func(registry *format.SimpleRegistry) {
		registry.RegisterProber(prober)
	}
}

func testFormatMuxer(id av.FormatID, factory format.MuxerFactory) func(*format.SimpleRegistry) {
	return func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(id, factory)
	}
}

func specText(spec pipeline.Spec) string {
	return specRenderURI(spec, "goav:graph")
}

func specMermaid(spec pipeline.Spec) string {
	return specRenderURI(spec, "goav:graph?format=mermaid")
}

func specRenderURI(spec pipeline.Spec, target string) string {
	out, err := graphrender.RenderURI(spec, target)
	if err != nil {
		return err.Error()
	}
	return out
}

func drainTaskEvents(task goav.Task) []av.Event {
	var events []av.Event
	for {
		select {
		case event := <-task.Events():
			events = append(events, event)
		default:
			return events
		}
	}
}

func countEventsForStream(events []av.Event, eventType av.EventType, stream av.StreamID) int {
	count := 0
	for i := range events {
		if events[i].Type == eventType && events[i].StreamID == stream {
			count++
		}
	}
	return count
}

// decodeTestDecoder counts decode calls and emits one empty frame per packet.
type decodeTestDecoderFactory struct {
	decoder *decodeTestDecoder
	config  codec.DecodeConfig
}

func (f *decodeTestDecoderFactory) NewDecoder(_ context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	f.config = config
	return f.decoder, nil
}

type decodeTestDecoder struct {
	decodes int
	flushes int
	closed  bool
}

func (d *decodeTestDecoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: av.CodecOpus}
}

func (d *decodeTestDecoder) Open(context.Context, codec.DecodeConfig) error {
	return nil
}

func (d *decodeTestDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.StreamID = packet.StreamID
	frame.Type = av.MediaAudio
	d.decodes++
	return nil
}

func (d *decodeTestDecoder) FlushInto(context.Context, *codec.DecodeResult) error {
	d.flushes++
	return nil
}

func (d *decodeTestDecoder) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (d *decodeTestDecoder) Close() error {
	d.closed = true
	return nil
}

// runtimeTestSink records the packets and frames a pipeline delivers to it.
type runtimeTestSink struct {
	name            string
	count           int
	frames          int
	lastPacket      *av.Packet
	lastPacketValue av.Packet
	lastFrame       av.Frame
	closed          bool
}

func (s *runtimeTestSink) Name() string {
	return s.name
}

func (s *runtimeTestSink) Handle(_ context.Context, msg *pipeline.Message) error {
	s.count++
	if msg.Kind == pipeline.MessagePacket {
		s.lastPacket = msg.Packet
		if msg.Packet != nil {
			s.lastPacketValue = *msg.Packet
		}
	}
	if msg.Kind == pipeline.MessageFrame {
		s.frames++
		if msg.Frame != nil {
			s.lastFrame = *msg.Frame
		}
	}
	return nil
}

func (s *runtimeTestSink) Close() error {
	s.closed = true
	return nil
}

// remuxTestMuxerFactory mirrors the root module's remux format fixture:
// recording Ogg muxers.
type remuxTestMuxerFactory struct {
	muxers []*remuxTestMuxer
}

func (f *remuxTestMuxerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	muxer := &remuxTestMuxer{}
	f.muxers = append(f.muxers, muxer)
	return muxer, nil
}

type remuxTestMuxer struct {
	opened          bool
	closed          bool
	writes          int
	streamCount     int
	lastStream      av.StreamID
	openedStreams   []av.StreamID
	writtenStreams  []av.StreamID
	writtenPayloads []byte
}

func (m *remuxTestMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *remuxTestMuxer) Open(_ context.Context, _ format.Output, streams []av.Stream, _ format.OpenOptions) error {
	m.opened = true
	m.streamCount = len(streams)
	m.openedStreams = m.openedStreams[:0]
	for i := range streams {
		m.openedStreams = append(m.openedStreams, streams[i].ID)
	}
	return nil
}

func (m *remuxTestMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	m.writes++
	m.lastStream = packet.StreamID
	m.writtenStreams = append(m.writtenStreams, packet.StreamID)
	if len(packet.Payload.Bytes) != 0 {
		m.writtenPayloads = append(m.writtenPayloads, packet.Payload.Bytes[0])
	}
	return nil
}

func (m *remuxTestMuxer) Close() error {
	m.closed = true
	return nil
}

// Writer-destination fixtures: a transactional write-closer recording opens,
// closes, commits, and aborts, plus the byte-forwarding muxer feeding it.
type writerDestinationState struct {
	bytes   bytes.Buffer
	info    provider.Info
	opens   int
	closes  int
	commits int
	aborts  int
}

type writerDestinationWriteCloser struct {
	state *writerDestinationState
}

func (w *writerDestinationWriteCloser) Write(p []byte) (int, error) {
	return w.state.bytes.Write(p)
}

func (w *writerDestinationWriteCloser) Close() error {
	w.state.closes++
	return nil
}

func (w *writerDestinationWriteCloser) Commit(context.Context) error {
	w.state.commits++
	return nil
}

func (w *writerDestinationWriteCloser) Abort(context.Context) error {
	w.state.aborts++
	return nil
}

type writerDestinationMuxerFactory struct {
	muxer *writerDestinationMuxer
}

func (f writerDestinationMuxerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return f.muxer, nil
}

type writerDestinationMuxer struct {
	writer io.Writer
	writes int
	closed bool
}

func (m *writerDestinationMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *writerDestinationMuxer) Open(_ context.Context, output format.Output, _ []av.Stream, _ format.OpenOptions) error {
	m.writer = output.Writer
	return nil
}

func (m *writerDestinationMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	m.writes++
	_, err := m.writer.Write(packet.Payload.Bytes)
	return err
}

func (m *writerDestinationMuxer) Close() error {
	m.closed = true
	return nil
}

type fileDestinationWriteCloser struct {
	bytes  bytes.Buffer
	closes int
}

func (w *fileDestinationWriteCloser) Write(p []byte) (int, error) {
	return w.bytes.Write(p)
}

func (w *fileDestinationWriteCloser) Close() error {
	w.closes++
	return nil
}

// taskHasBranch reports whether the task snapshot carries an attached branch
// with the given name.
func taskHasBranch(task goav.Task, name string) bool {
	branches := task.Snapshot().Branches
	for i := range branches {
		if branches[i].Name == name && branches[i].State == lifecycle.BranchAttached {
			return true
		}
	}
	return false
}

func waitForCount(t *testing.T, label string, want int32, get func() int32) {
	t.Helper()
	waitForCondition(t, label, func() bool { return get() >= want })
}

func waitForCondition(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: condition not reached", label)
		}
		time.Sleep(time.Millisecond)
	}
}
