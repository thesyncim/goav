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
		Stateless:     true,
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
	return nil
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
	inputChannels := f.inputChannels
	inputRate := f.inputRate
	inputSamples := inputSampleCount(frame, inputChannels)
	if inputSamples == 0 {
		return nil
	}

	outputSamples := resampledSampleCount(inputSamples, inputRate, f.outputRate)
	outputBytes := outputSamples * f.outputChannels * 2
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	output := &out.Frames[index]
	output.Reset()
	if cap(output.Planes) < 1 {
		out.Frames = out.Frames[:index]
		return filter.ErrOutputBufferTooSmall
	}
	output.Planes = output.Planes[:1]
	plane := &output.Planes[0]
	if cap(plane.Buffer.Bytes) < outputBytes {
		out.Frames = out.Frames[:index]
		return filter.ErrOutputBufferTooSmall
	}

	input := frame.Planes[0].Buffer.Bytes
	dst := plane.Buffer.Bytes[:outputBytes]
	resampleS16(dst, input, inputSamples, inputRate, inputChannels, outputSamples, f.outputRate, f.outputChannels)
	plane.Buffer.Bytes = dst
	plane.Buffer.Ownership = av.BufferOwned
	plane.Stride = f.outputChannels * 2

	f.outputAudio.Samples = outputSamples
	output.StreamID = frame.StreamID
	output.CodecEpoch = frame.CodecEpoch
	output.Type = f.outputFrameType
	output.PTS = frame.PTS
	output.Duration = av.SamplesDuration(outputSamples, f.outputRate)
	output.Audio = &f.outputAudio
	output.Metadata = frame.Metadata
	return nil
}

func (f *Filter) FlushInto(context.Context, *filter.Result) error {
	return nil
}

func (f *Filter) HandleEvent(context.Context, *av.Event) error {
	return nil
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
	for sample := 0; sample < outputSamples; sample++ {
		position := sample * inputRate
		base := position / outputRate
		frac := position % outputRate
		next := base + 1
		if next >= inputSamples {
			next = base
			frac = 0
		}
		for channel := 0; channel < outputChannels; channel++ {
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
	for sample := 0; sample < samples; sample++ {
		for channel := 0; channel < outputChannels; channel++ {
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
		for channel := 0; channel < inputChannels; channel++ {
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
