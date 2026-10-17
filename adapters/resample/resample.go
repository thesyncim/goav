package resample

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

const backendName = "resample"

type Factory struct{}

func NewFactory() Factory {
	return Factory{}
}

func Descriptor() filter.Descriptor {
	return filter.Descriptor{
		Name:          filter.FactoryResample,
		Input:         av.MediaAudio,
		Output:        av.MediaAudio,
		SampleFormats: []string{av.SampleFormatS16},
		Realtime:      true,
		Metadata: av.Metadata{
			"backend":       backendName,
			"sample_format": av.SampleFormatS16,
		},
	}
}

func Register(registry *filter.SimpleRegistry) {
	registry.RegisterFactory(Descriptor(), NewFactory())
}

func (Factory) NewFilter(ctx context.Context, config filter.Config) (filter.FrameFilter, error) {
	resampler := &Filter{}
	if err := resampler.Open(ctx, config); err != nil {
		return nil, err
	}
	return resampler, nil
}

type Filter struct {
	stream          av.Stream
	inputRate       int
	outputRate      int
	inputChannels   int
	outputChannels  int
	outputLayout    string
	outputFormat    string
	outputAudio     av.AudioFrame
	outputTimeBase  av.TimeBase
	outputFrameType av.MediaType

	// Rate conversion streams across frames: inPos and outPos are the global
	// indexes of the next input and output samples, last holds the previous
	// input sample per output channel (after channel mapping), and startPTS is
	// the output timeline origin. Output sample k sits at input position
	// k*inputRate/outputRate and interpolates its two neighbors, so a frame
	// emits only the samples whose right neighbor has arrived (at most one
	// input sample of latency) and FlushInto emits the rest. Frame boundaries
	// are seamless and the total count is exact, whatever the frame sizes.
	inPos    int64
	outPos   int64
	last     []int
	started  bool
	startPTS av.Timestamp
	// identity carries the stream id and codec epoch of the latest input frame
	// onto the flushed tail.
	identity av.Frame
}

func (f *Filter) Descriptor() filter.Descriptor {
	return Descriptor()
}

func (f *Filter) Open(ctx context.Context, config filter.Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if config.Audio == nil {
		return filter.ErrUnsupportedFormat
	}
	stream := config.Stream
	if stream.Type != "" && stream.Type != av.MediaAudio {
		return filter.ErrUnsupportedFormat
	}
	if stream.Codec.Type != "" && stream.Codec.Type != av.MediaAudio {
		return filter.ErrUnsupportedFormat
	}
	inputRate := audioSampleRate(stream)
	inputChannels := stream.Codec.Channels
	if inputChannels == 0 {
		inputChannels = 1
	}
	inputFormat := stream.Codec.SampleFormat
	if inputFormat == "" {
		inputFormat = av.SampleFormatS16
	}
	if inputFormat != av.SampleFormatS16 {
		return filter.ErrUnsupportedFormat
	}

	outputRate := config.Audio.SampleRate
	if outputRate == 0 {
		outputRate = inputRate
	}
	outputChannels := config.Audio.Channels
	if outputChannels == 0 {
		outputChannels = inputChannels
	}
	outputFormat := config.Audio.SampleFormat
	if outputFormat == "" {
		outputFormat = inputFormat
	}
	if outputRate <= 0 || inputRate <= 0 || inputChannels <= 0 || outputChannels <= 0 || outputFormat != av.SampleFormatS16 {
		return filter.ErrUnsupportedFormat
	}

	outputLayout := config.Audio.ChannelLayout
	if outputLayout == "" {
		outputLayout = stream.Codec.ChannelLayout
	}
	stream.Type = av.MediaAudio
	stream.Codec.Type = av.MediaAudio
	stream.Codec.SampleRate = outputRate
	stream.Codec.ClockRate = uint32(outputRate)
	stream.Codec.Channels = outputChannels
	stream.Codec.ChannelLayout = outputLayout
	stream.Codec.SampleFormat = outputFormat
	stream.TimeBase = av.TimeBase{Num: 1, Den: int64(outputRate)}

	f.stream = stream
	f.inputRate = inputRate
	f.outputRate = outputRate
	f.inputChannels = inputChannels
	f.outputChannels = outputChannels
	f.outputLayout = outputLayout
	f.outputFormat = outputFormat
	f.outputTimeBase = stream.TimeBase
	f.outputFrameType = av.MediaAudio
	f.outputAudio = av.AudioFrame{
		SampleRate:    outputRate,
		Channels:      outputChannels,
		ChannelLayout: outputLayout,
		SampleFormat:  outputFormat,
	}
	f.last = make([]int, outputChannels)
	f.resetStream()
	return nil
}

// resetStream restarts the rate-conversion timeline: the next frame becomes
// the new origin.
func (f *Filter) resetStream() {
	f.inPos, f.outPos, f.started = 0, 0, false
	clear(f.last)
}

func (f *Filter) FilterInto(ctx context.Context, frame *av.Frame, out *filter.Result) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if frame == nil {
		return nil
	}
	if out == nil || len(out.Frames) == cap(out.Frames) {
		return filter.ErrResultFull
	}
	if frame.Type != "" && frame.Type != av.MediaAudio {
		return filter.ErrUnsupportedFormat
	}
	if len(frame.Planes) == 0 {
		return filter.ErrUnsupportedFormat
	}
	inputSamples := inputSampleCount(frame, f.inputChannels)
	if inputSamples == 0 {
		return nil
	}
	input := frame.Planes[0].Buffer.Bytes

	if f.inputRate == f.outputRate {
		// Equal rates need no interpolation, so they stay stateless and keep
		// the input's timestamps.
		dst, err := f.nextOutput(frame, out, inputSamples, frame.PTS)
		if err != nil {
			return err
		}
		resampleS16(dst, input, inputSamples, f.inputRate, f.inputChannels, inputSamples, f.outputRate, f.outputChannels)
		return nil
	}

	if !f.started {
		f.startPTS = f.outputTimeline(frame.PTS)
		f.started = true
	}
	f.identity.StreamID, f.identity.CodecEpoch = frame.StreamID, frame.CodecEpoch
	end := f.inPos + int64(inputSamples)
	// Emit the outputs whose right neighbor (input end-1) has arrived.
	count := int(ceilDiv((end-1)*int64(f.outputRate), int64(f.inputRate)) - f.outPos)
	if count > 0 {
		dst, err := f.nextOutput(frame, out, count, f.positionPTS())
		if err != nil {
			return err
		}
		f.convert(dst, input, count)
	}
	for channel := range f.outputChannels {
		f.last[channel] = mixedSampleS16(input, inputSamples-1, f.inputChannels, channel, f.outputChannels)
	}
	f.inPos = end
	return nil
}

// convert writes count output samples starting at outPos, interpolating
// between the previous frame's last sample and this frame's samples.
func (f *Filter) convert(dst []byte, input []byte, count int) {
	inRate, outRate := int64(f.inputRate), int64(f.outputRate)
	for j := range count {
		position := (f.outPos + int64(j)) * inRate
		base := position / outRate
		frac := int(position % outRate)
		for channel := range f.outputChannels {
			a := f.inputSample(input, base, channel)
			value := a
			if frac != 0 {
				b := f.inputSample(input, base+1, channel)
				value = (a*(f.outputRate-frac) + b*frac) / f.outputRate
			}
			putS16(dst, (j*f.outputChannels+channel)*2, int16(value))
		}
	}
	f.outPos += int64(count)
}

// inputSample returns global input sample index for one output channel: the
// carried last sample just before this frame, else a sample of this frame.
func (f *Filter) inputSample(input []byte, index int64, channel int) int {
	if index < f.inPos {
		return f.last[channel]
	}
	return mixedSampleS16(input, int(index-f.inPos), f.inputChannels, channel, f.outputChannels)
}

// nextOutput reserves the next result frame for samples output samples and
// returns its sample bytes.
func (f *Filter) nextOutput(frame *av.Frame, out *filter.Result, samples int, pts av.Timestamp) ([]byte, error) {
	outputBytes := samples * f.outputChannels * 2
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	output := &out.Frames[index]
	output.Reset()
	if cap(output.Planes) < 1 {
		out.Frames = out.Frames[:index]
		return nil, filter.ErrOutputBufferTooSmall
	}
	output.Planes = output.Planes[:1]
	plane := &output.Planes[0]
	if cap(plane.Buffer.Bytes) < outputBytes {
		out.Frames = out.Frames[:index]
		return nil, filter.ErrOutputBufferTooSmall
	}
	plane.Buffer.Bytes = plane.Buffer.Bytes[:outputBytes]
	plane.Buffer.Ownership = av.BufferOwned
	plane.Stride = f.outputChannels * 2

	f.outputAudio.Samples = samples
	output.StreamID = frame.StreamID
	output.CodecEpoch = frame.CodecEpoch
	output.Type = f.outputFrameType
	output.PTS = pts
	output.Duration = av.SamplesDuration(samples, f.outputRate)
	output.Audio = &f.outputAudio
	output.Metadata = frame.Metadata
	return plane.Buffer.Bytes, nil
}

// outputTimeline maps an input timestamp onto the 1/outputRate timeline.
func (f *Filter) outputTimeline(pts av.Timestamp) av.Timestamp {
	if rescaled, ok := pts.Rescale(f.outputTimeBase); ok {
		return rescaled
	}
	return av.Timestamp{Base: f.outputTimeBase}
}

// positionPTS is the timestamp of output sample outPos.
func (f *Filter) positionPTS() av.Timestamp {
	return av.Timestamp{Value: f.startPTS.Value + f.outPos, Base: f.outputTimeBase}
}

// FlushInto emits the outputs held back for a right neighbor that will never
// arrive, holding the final input sample, so the stream's total output count
// is exactly ceil(inputs*outputRate/inputRate).
func (f *Filter) FlushInto(ctx context.Context, out *filter.Result) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !f.started || f.inPos == 0 || out == nil || len(out.Frames) == cap(out.Frames) {
		return nil
	}
	count := int(ceilDiv(f.inPos*int64(f.outputRate), int64(f.inputRate)) - f.outPos)
	if count <= 0 {
		return nil
	}
	dst, err := f.nextOutput(&f.identity, out, count, f.positionPTS())
	if err != nil {
		return err
	}
	for j := range count {
		for channel := range f.outputChannels {
			putS16(dst, (j*f.outputChannels+channel)*2, int16(f.last[channel]))
		}
	}
	f.outPos += int64(count)
	return nil
}

// HandleEvent restarts the conversion timeline when the input jumps, so a
// discontinuity never interpolates across unrelated audio.
func (f *Filter) HandleEvent(_ context.Context, event *av.Event) error {
	if event != nil && (event.Type == av.EventDiscontinuity || event.Type == av.EventCodecChanged) {
		f.resetStream()
	}
	return nil
}

func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func (f *Filter) Close() error {
	return nil
}

func audioSampleRate(stream av.Stream) int {
	if stream.Codec.SampleRate != 0 {
		return stream.Codec.SampleRate
	}
	if stream.Codec.ClockRate != 0 {
		return int(stream.Codec.ClockRate)
	}
	if stream.TimeBase.Den != 0 && stream.TimeBase.Num == 1 {
		return int(stream.TimeBase.Den)
	}
	return 0
}

func inputSampleCount(frame *av.Frame, channels int) int {
	available := 0
	if channels > 0 && len(frame.Planes) != 0 {
		available = len(frame.Planes[0].Buffer.Bytes) / (channels * 2)
	}
	if frame.Audio != nil && frame.Audio.Samples > 0 {
		if available != 0 && frame.Audio.Samples > available {
			return available
		}
		return frame.Audio.Samples
	}
	return available
}

func resampledSampleCount(inputSamples int, inputRate int, outputRate int) int {
	if inputSamples <= 0 || inputRate <= 0 || outputRate <= 0 {
		return 0
	}
	return (inputSamples*outputRate + inputRate - 1) / inputRate
}

func resampleS16(dst []byte, src []byte, inputSamples int, inputRate int, inputChannels int, outputSamples int, outputRate int, outputChannels int) {
	if inputRate == outputRate && inputSamples == outputSamples {
		if inputChannels == outputChannels {
			copy(dst[:inputSamples*inputChannels*2], src[:inputSamples*inputChannels*2])
			return
		}
		remapS16Channels(dst, src, inputSamples, inputChannels, outputChannels)
		return
	}
	for sample := range outputSamples {
		position := sample * inputRate
		base := position / outputRate
		frac := position % outputRate
		next := base + 1
		if next >= inputSamples {
			next = base
			frac = 0
		}
		for channel := range outputChannels {
			a := mixedSampleS16(src, base, inputChannels, channel, outputChannels)
			b := mixedSampleS16(src, next, inputChannels, channel, outputChannels)
			value := a
			if frac != 0 {
				value = (a*(outputRate-frac) + b*frac) / outputRate
			}
			putS16(dst, (sample*outputChannels+channel)*2, int16(value))
		}
	}
}

func remapS16Channels(dst []byte, src []byte, samples int, inputChannels int, outputChannels int) {
	for sample := range samples {
		for channel := range outputChannels {
			value := mixedSampleS16(src, sample, inputChannels, channel, outputChannels)
			putS16(dst, (sample*outputChannels+channel)*2, int16(value))
		}
	}
}

func mixedSampleS16(src []byte, sample int, inputChannels int, outputChannel int, outputChannels int) int {
	switch {
	case inputChannels == outputChannels:
		return int(getS16(src, (sample*inputChannels+outputChannel)*2))
	case outputChannels == 1:
		sum := 0
		for channel := range inputChannels {
			sum += int(getS16(src, (sample*inputChannels+channel)*2))
		}
		return sum / inputChannels
	case inputChannels == 1:
		return int(getS16(src, sample*2))
	case outputChannel < inputChannels:
		return int(getS16(src, (sample*inputChannels+outputChannel)*2))
	default:
		return int(getS16(src, (sample*inputChannels+inputChannels-1)*2))
	}
}

func getS16(data []byte, offset int) int16 {
	value := uint16(data[offset]) | uint16(data[offset+1])<<8
	return int16(value)
}

func putS16(data []byte, offset int, value int16) {
	v := uint16(value)
	data[offset] = byte(v)
	data[offset+1] = byte(v >> 8)
}
