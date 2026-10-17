package gopus

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	gopuslib "github.com/thesyncim/gopus"
)

type EncoderFactory struct{}

func NewEncoderFactory() EncoderFactory {
	return EncoderFactory{}
}

func (EncoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	encoder := &Encoder{}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

type Encoder struct {
	encoder    *gopuslib.Encoder
	stream     av.Stream
	sampleRate int
	channels   int
	closed     bool

	// Re-framing: Opus encodes only fixed frame durations (2.5-120 ms), but
	// decoders and resamplers deliver whatever they produce (AAC's 1024
	// samples, a resampled 1115). frameSize is chosen at the first frame — its
	// own size when Opus accepts it, else 20 ms — and pending carries the
	// interleaved samples not yet encoded. pendingPTS stamps pending[0] on the
	// encoder's 1/sampleRate timeline, so packet times advance by exact sample
	// counts instead of inheriting the input frames' cadence.
	frameSize  int
	pending    []int16
	pendingPTS av.Timestamp
}

// opusDefaultFrameMS is the re-framing duration when the input frame size is
// not itself a valid Opus frame: 20 ms, the codec's standard operating point.
const opusDefaultFrameMS = 20

func (e *Encoder) Descriptor() codec.Descriptor {
	return Descriptor()
}

func (e *Encoder) Open(ctx context.Context, config codec.EncodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, sampleRate, channels, err := normalizeEncodeStream(config)
	if err != nil {
		return err
	}
	encoder, err := gopuslib.NewEncoder(gopuslib.EncoderConfig{
		SampleRate:  sampleRate,
		Channels:    channels,
		Application: gopuslib.ApplicationAudio,
	})
	if err != nil {
		return mapEncodeError(err)
	}
	if config.Settings.Bitrate > 0 {
		if err := encoder.SetBitrate(config.Settings.Bitrate); err != nil {
			return mapEncodeError(err)
		}
	}
	// Tier-3 raw control: hand the caller the concrete *gopuslib.Encoder so it can
	// apply any Opus setting (FEC, DTX, complexity, …) the library exposes.
	if control := config.Settings.Control; control != nil {
		if err := control(encoder); err != nil {
			return mapEncodeError(err)
		}
	}
	e.encoder = encoder
	e.stream = stream
	e.sampleRate = sampleRate
	e.channels = channels
	e.closed = false
	e.frameSize = 0
	// Room for the longest Opus-sized input (120 ms) plus one partial frame,
	// so steady-state re-framing never grows the buffer.
	e.pending = make([]int16, 0, sampleRate*(120+opusDefaultFrameMS)/1000*channels)
	e.pendingPTS = av.Timestamp{Base: stream.TimeBase}
	return nil
}

func (e *Encoder) EncodeInto(ctx context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if e.closed {
		return codec.ErrClosed
	}
	if frame == nil {
		return nil
	}
	if e.encoder == nil {
		return codec.ErrUnsupportedFormat
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	pcm, samples, err := e.pcmFromFrame(frame)
	if err != nil {
		return err
	}
	if e.frameSize == 0 {
		if err := e.chooseFrameSize(samples); err != nil {
			return err
		}
	}
	// An exact frame with nothing pending encodes in place and keeps the
	// input's own timestamps — the zero-copy path for Opus-native sources.
	if len(e.pending) == 0 && samples == e.frameSize {
		duration := frame.Duration
		if duration.Value == 0 {
			duration = av.SamplesDuration(samples, e.sampleRate)
		}
		return e.encodePacket(pcm, frame.PTS, duration, frame.Metadata, out)
	}
	if len(e.pending) == 0 {
		e.pendingPTS = e.timelinePTS(frame.PTS)
	}
	e.pending = append(e.pending, pcm...)
	return e.drain(frame.Metadata, out)
}

// chooseFrameSize fixes the Opus frame duration: the first frame's own size
// when Opus accepts it, otherwise 20 ms with the input re-framed to fit.
func (e *Encoder) chooseFrameSize(samples int) error {
	if samples > 0 && e.encoder.SetFrameSize(samples) == nil {
		e.frameSize = samples
		return nil
	}
	frameSize := e.sampleRate * opusDefaultFrameMS / 1000
	if err := e.encoder.SetFrameSize(frameSize); err != nil {
		return mapEncodeError(err)
	}
	e.frameSize = frameSize
	return nil
}

// timelinePTS maps an input timestamp onto the encoder's 1/sampleRate
// timeline; an input without a usable time base continues the running one.
func (e *Encoder) timelinePTS(pts av.Timestamp) av.Timestamp {
	if rescaled, ok := pts.Rescale(e.stream.TimeBase); ok {
		return rescaled
	}
	return av.Timestamp{Value: e.pendingPTS.Value, Base: e.stream.TimeBase}
}

// drain encodes every whole frame in pending while the result has room, so
// the remainder stays below one frame whenever the runtime's result slots
// cover the input frame size.
func (e *Encoder) drain(metadata av.Metadata, out *codec.EncodeResult) error {
	step := e.frameSize * e.channels
	duration := av.SamplesDuration(e.frameSize, e.sampleRate)
	consumed := 0
	for len(e.pending)-consumed >= step && len(out.Packets) < cap(out.Packets) {
		if err := e.encodePacket(e.pending[consumed:consumed+step], e.pendingPTS, duration, metadata, out); err != nil {
			return err
		}
		consumed += step
		e.pendingPTS.Value += int64(e.frameSize)
	}
	e.pending = e.pending[:copy(e.pending, e.pending[consumed:])]
	return nil
}

// encodePacket encodes one Opus frame of interleaved pcm into the next result
// slot.
func (e *Encoder) encodePacket(pcm []int16, pts av.Timestamp, duration av.Duration, metadata av.Metadata, out *codec.EncodeResult) error {
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	if cap(packet.Payload.Bytes) == 0 {
		out.Packets = out.Packets[:index]
		return codec.ErrOutputBufferTooSmall
	}

	dst := packet.Payload.Bytes[:cap(packet.Payload.Bytes)]
	n, err := e.encoder.EncodeInt16(pcm, dst)
	if err != nil {
		out.Packets = out.Packets[:index]
		return mapEncodeError(err)
	}
	if n == 0 {
		out.Packets = out.Packets[:index]
		return nil
	}

	packet.StreamID = e.stream.ID
	packet.CodecEpoch = e.stream.Epoch
	packet.Payload.Bytes = dst[:n]
	packet.Payload.Ownership = av.BufferOwned
	packet.PTS = pts
	packet.Duration = duration
	packet.Metadata = metadata
	return nil
}

// FlushInto encodes the final partial frame at end of stream, padded with
// silence to a whole Opus frame; its packet duration is the real sample
// count, so the stream ends where the input did.
func (e *Encoder) FlushInto(ctx context.Context, out *codec.EncodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if e.closed {
		return codec.ErrClosed
	}
	if err := e.drain(nil, out); err != nil {
		return err
	}
	if len(e.pending) == 0 || len(out.Packets) == cap(out.Packets) {
		return nil
	}
	remaining := len(e.pending) / e.channels
	for len(e.pending) < e.frameSize*e.channels {
		e.pending = append(e.pending, 0)
	}
	err := e.encodePacket(e.pending, e.pendingPTS, av.SamplesDuration(remaining, e.sampleRate), nil, out)
	e.pending = e.pending[:0]
	return err
}

func (e *Encoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	if e.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventBitrateChanged:
		// libopus supports live bitrate retargeting (OPUS_SET_BITRATE applies
		// from the next encoded frame); gopus exposes it as SetBitrate in bits
		// per second. A malformed event or a rejected rate is reported, never
		// swallowed: the request either reaches the encoder or fails clearly.
		bitsPerSecond, ok := codec.EventBitrate(event)
		if !ok {
			return fmt.Errorf("gopus: bitrate event needs positive %s metadata in bits per second", av.MetadataBitrate)
		}
		if err := e.encoder.SetBitrate(bitsPerSecond); err != nil {
			return fmt.Errorf("gopus: live bitrate retarget to %d bps rejected: %w", bitsPerSecond, mapEncodeError(err))
		}
	case av.EventCodecChanged, av.EventDiscontinuity:
		// A partial frame belongs to the old timeline; the next frame restarts
		// the re-framing timeline from its own timestamp.
		e.encoder.Reset()
		e.pending = e.pending[:0]
	}
	return nil
}

func (e *Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.encoder = nil
	return nil
}

func (e *Encoder) pcmFromFrame(frame *av.Frame) ([]int16, int, error) {
	if frame.Type != "" && frame.Type != av.MediaAudio {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio == nil || len(frame.Planes) == 0 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio.SampleFormat != "" && frame.Audio.SampleFormat != av.SampleFormatS16 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	channels := frame.Audio.Channels
	if channels == 0 {
		channels = e.channels
	}
	if channels != e.channels {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	data := frame.Planes[0].Buffer.Bytes
	if len(data)%2 != 0 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	pcm := unsafe.Slice((*int16)(unsafe.Pointer(unsafe.SliceData(data))), len(data)/2)
	samples := frame.Audio.Samples
	if samples == 0 {
		if channels <= 0 || len(pcm)%channels != 0 {
			return nil, 0, codec.ErrUnsupportedFormat
		}
		samples = len(pcm) / channels
	}
	if len(pcm) != samples*channels {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	return pcm, samples, nil
}

func normalizeEncodeStream(config codec.EncodeConfig) (av.Stream, int, int, error) {
	stream := config.Stream
	parameters := config.Parameters
	if stream.Type == "" {
		stream.Type = av.MediaAudio
	}
	if parameters.Type == "" {
		parameters.Type = av.MediaAudio
	}
	if stream.Codec.ID != "" && stream.Codec.ID != av.CodecOpus {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	if parameters.ID != "" && parameters.ID != av.CodecOpus {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	sampleRate := firstPositive(parameters.SampleRate, int(parameters.ClockRate), stream.Codec.SampleRate, int(stream.Codec.ClockRate), 48000)
	channels := firstPositive(parameters.Channels, stream.Codec.Channels, 2)
	if channels != 1 && channels != 2 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	parameters.ID = av.CodecOpus
	parameters.Type = av.MediaAudio
	parameters.SampleRate = sampleRate
	parameters.ClockRate = uint32(sampleRate)
	parameters.Channels = channels
	if parameters.ChannelLayout == "" {
		if channels == 1 {
			parameters.ChannelLayout = "mono"
		} else {
			parameters.ChannelLayout = "stereo"
		}
	}
	parameters.SampleFormat = av.SampleFormatS16
	stream.Type = av.MediaAudio
	stream.Codec = parameters
	stream.TimeBase = av.TimeBase{Num: 1, Den: int64(sampleRate)}
	return stream, sampleRate, channels, nil
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func mapEncodeError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gopuslib.ErrBufferTooSmall):
		return codec.ErrOutputBufferTooSmall
	case errors.Is(err, gopuslib.ErrInvalidSampleRate),
		errors.Is(err, gopuslib.ErrInvalidChannels),
		errors.Is(err, gopuslib.ErrInvalidSampleFormat),
		errors.Is(err, gopuslib.ErrInvalidFrameSize),
		errors.Is(err, gopuslib.ErrInvalidBitrate),
		errors.Is(err, gopuslib.ErrInvalidApplication),
		errors.Is(err, gopuslib.ErrInvalidArgument):
		return codec.ErrUnsupportedFormat
	default:
		return err
	}
}
