package resample

import (
	"context"
	"encoding/binary"

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

type resampleState uint8

const (
	resampleIdle resampleState = iota
	resampleStreaming
	resampleFlushed
)

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
	state    resampleState
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
	f.inPos, f.outPos, f.state = 0, 0, resampleIdle
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
		if f.inputChannels == f.outputChannels {
			copy(dst[:inputSamples*f.inputChannels*2], input[:inputSamples*f.inputChannels*2])
		} else {
			remapS16Channels(dst, input, inputSamples, f.inputChannels, f.outputChannels)
		}
		return nil
	}

	if f.state != resampleStreaming {
		if f.state == resampleFlushed {
			f.resetStream()
		}
		f.startPTS = f.outputTimeline(frame.PTS)
		f.state = resampleStreaming
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
// Uses an incremental (base, frac) step and Q16 fixed-point reciprocal so the
// inner loop performs zero integer divisions.
func (f *Filter) convert(dst []byte, input []byte, count int) {
	inRate, outRate := int64(f.inputRate), int64(f.outputRate)
	outRateInt := f.outputRate
	position := f.outPos * inRate
	base := position / outRate
	frac := int(position % outRate)
	step := inRate / outRate
	stepInt := int(step)
	rem := int(inRate % outRate)
	inv := (uint64(1)<<32 + uint64(outRate)/2) / uint64(outRate)
	inPos := f.inPos
	inCh, outCh := f.inputChannels, f.outputChannels

	switch {
	case inCh == 1 && outCh == 1:
		last0 := int32(f.last[0])
		d := dst[:count*2]
		j := 0
		for ; j < count && base < inPos; j++ {
			a := last0
			value := a
			if frac != 0 {
				var b int32
				if base+1 < inPos {
					b = last0
				} else {
					b = int32(getS16(input, int(base+1-inPos)*2))
				}
				w := int32((uint64(frac) * inv) >> 16)
				value = a + ((b-a)*w)>>16
			}
			binary.LittleEndian.PutUint16(d[j*2:], uint16(int16(value)))
			base += step
			frac += rem
			if frac >= outRateInt {
				frac -= outRateInt
				base++
			}
		}
		if j < count {
			rel := int(base - inPos)
			if stepInt == 0 {
				v := binary.LittleEndian.Uint32(input[rel*2:])
				a := int32(int16(v))
				b := int32(int16(v >> 16))
				diff := b - a
				// Align to frac == 0 if we are on an exact 2x or 3x upsample ratio.
				if outRateInt == 3*f.inputRate {
					for ; j < count && frac != 0; j++ {
						w := int32((uint64(frac) * inv) >> 16)
						binary.LittleEndian.PutUint16(d[j*2:], uint16(int16(a+(diff*w)>>16)))
						frac += rem
						if frac >= outRateInt {
							frac -= outRateInt
							rel++
							if j+1 < count {
								a = b
								b = int32(getS16(input, (rel+1)*2))
								diff = b - a
							}
						}
					}
					w1 := int32((uint64(rem) * inv) >> 16)
					w2 := int32((uint64(2*rem) * inv) >> 16)
					for ; j+6 <= count; j += 6 {
						c := int32(getS16(input, (rel+2)*2))
						diff2 := c - b
						s0 := uint64(uint16(int16(a)))
						s1 := uint64(uint16(int16(a + (diff*w1)>>16)))
						s2 := uint64(uint16(int16(a + (diff*w2)>>16)))
						s3 := uint64(uint16(int16(b)))
						s4 := uint32(uint16(int16(b + (diff2*w1)>>16)))
						s5 := uint32(uint16(int16(b + (diff2*w2)>>16)))
						out := d[j*2 : j*2+12]
						binary.LittleEndian.PutUint64(out, s0|s1<<16|s2<<32|s3<<48)
						binary.LittleEndian.PutUint32(out[8:], s4|s5<<16)
						rel += 2
						if j+6 < count {
							a = c
							b = int32(getS16(input, (rel+1)*2))
							diff = b - a
						}
					}
					for ; j+3 <= count; j += 3 {
						s0 := uint16(int16(a))
						s1 := uint16(int16(a + (diff*w1)>>16))
						s2 := uint16(int16(a + (diff*w2)>>16))
						out := d[j*2 : j*2+6]
						binary.LittleEndian.PutUint32(out, uint32(s0)|uint32(s1)<<16)
						binary.LittleEndian.PutUint16(out[4:], s2)
						rel++
						if j+3 < count {
							a = b
							b = int32(getS16(input, (rel+1)*2))
							diff = b - a
						}
					}
				}
				for ; j < count; j++ {
					w := int32((uint64(frac) * inv) >> 16)
					binary.LittleEndian.PutUint16(d[j*2:], uint16(int16(a+(diff*w)>>16)))
					frac += rem
					if frac >= outRateInt {
						frac -= outRateInt
						rel++
						if j+1 < count {
							a = b
							b = int32(getS16(input, (rel+1)*2))
							diff = b - a
						}
					}
				}
			} else {
				for ; j < count; j++ {
					v := binary.LittleEndian.Uint32(input[rel*2:])
					a := int32(int16(v))
					b := int32(int16(v >> 16))
					w := int32((uint64(frac) * inv) >> 16)
					binary.LittleEndian.PutUint16(d[j*2:], uint16(int16(a+((b-a)*w)>>16)))
					rel += stepInt
					frac += rem
					if frac >= outRateInt {
						frac -= outRateInt
						rel++
					}
				}
			}
		}
	case inCh == 2 && outCh == 1:
		last0 := int32(f.last[0])
		d := dst[:count*2]
		j := 0
		for ; j < count && base < inPos; j++ {
			a := last0
			value := a
			if frac != 0 {
				var b int32
				if base+1 < inPos {
					b = last0
				} else {
					v := binary.LittleEndian.Uint32(input[int(base+1-inPos)*4:])
					b = (int32(int16(v)) + int32(int16(v>>16))) / 2
				}
				w := int32((uint64(frac) * inv) >> 16)
				value = a + ((b-a)*w)>>16
			}
			binary.LittleEndian.PutUint16(d[j*2:], uint16(int16(value)))
			base += step
			frac += rem
			if frac >= outRateInt {
				frac -= outRateInt
				base++
			}
		}
		if j < count {
			rel := int(base - inPos)
			if stepInt == 0 {
				v := binary.LittleEndian.Uint64(input[rel*4:])
				a := (int32(int16(v)) + int32(int16(v>>16))) / 2
				b := (int32(int16(v>>32)) + int32(int16(v>>48))) / 2
				diff := b - a
				limit := count - 1
				for ; j+2 <= limit; j += 2 {
					w0 := int32((uint64(frac) * inv) >> 16)
					s0 := uint32(uint16(int16(a + (diff*w0)>>16)))
					frac += rem
					if frac >= outRateInt {
						frac -= outRateInt
						rel++
						a = b
						nv := binary.LittleEndian.Uint32(input[(rel+1)*4:])
						b = (int32(int16(nv)) + int32(int16(nv>>16))) / 2
						diff = b - a
					}
					w1 := int32((uint64(frac) * inv) >> 16)
					s1 := uint32(uint16(int16(a + (diff*w1)>>16)))
					binary.LittleEndian.PutUint32(d[j*2:], s0|s1<<16)
					frac += rem
					if frac >= outRateInt {
						frac -= outRateInt
						rel++
						a = b
						nv := binary.LittleEndian.Uint32(input[(rel+1)*4:])
						b = (int32(int16(nv)) + int32(int16(nv>>16))) / 2
						diff = b - a
					}
				}
				for ; j < count; j++ {
					w := int32((uint64(frac) * inv) >> 16)
					binary.LittleEndian.PutUint16(d[j*2:], uint16(int16(a+(diff*w)>>16)))
					frac += rem
					if frac >= outRateInt {
						frac -= outRateInt
						rel++
						if j+1 < count {
							a = b
							nv := binary.LittleEndian.Uint32(input[(rel+1)*4:])
							b = (int32(int16(nv)) + int32(int16(nv>>16))) / 2
							diff = b - a
						}
					}
				}
			} else {
				for ; j < count; j++ {
					v := binary.LittleEndian.Uint64(input[rel*4:])
					a := (int32(int16(v)) + int32(int16(v>>16))) / 2
					b := (int32(int16(v>>32)) + int32(int16(v>>48))) / 2
					w := int32((uint64(frac) * inv) >> 16)
					binary.LittleEndian.PutUint16(d[j*2:], uint16(int16(a+((b-a)*w)>>16)))
					rel += stepInt
					frac += rem
					if frac >= outRateInt {
						frac -= outRateInt
						rel++
					}
				}
			}
		}
	case inCh == 2 && outCh == 2:
		last0, last1 := int32(f.last[0]), int32(f.last[1])
		d := dst[:count*4]
		j := 0
		for ; j < count && base < inPos; j++ {
			a0, a1 := last0, last1
			v0, v1 := a0, a1
			if frac != 0 {
				var b0, b1 int32
				if base+1 < inPos {
					b0, b1 = last0, last1
				} else {
					v := binary.LittleEndian.Uint32(input[int(base+1-inPos)*4:])
					b0, b1 = int32(int16(v)), int32(int16(v>>16))
				}
				w := int32((uint64(frac) * inv) >> 16)
				v0 = a0 + ((b0-a0)*w)>>16
				v1 = a1 + ((b1-a1)*w)>>16
			}
			binary.LittleEndian.PutUint32(d[j*4:], uint32(uint16(int16(v0)))|uint32(uint16(int16(v1)))<<16)
			base += step
			frac += rem
			if frac >= outRateInt {
				frac -= outRateInt
				base++
			}
		}
		if j < count {
			rel := int(base - inPos)
			for ; j < count; j++ {
				v := binary.LittleEndian.Uint64(input[rel*4:])
				a0, a1 := int32(int16(v)), int32(int16(v>>16))
				b0, b1 := int32(int16(v>>32)), int32(int16(v>>48))
				w := int32((uint64(frac) * inv) >> 16)
				u0 := uint32(uint16(int16(a0 + ((b0-a0)*w)>>16)))
				u1 := uint32(uint16(int16(a1 + ((b1-a1)*w)>>16)))
				binary.LittleEndian.PutUint32(d[j*4:], u0|u1<<16)
				rel += stepInt
				frac += rem
				if frac >= outRateInt {
					frac -= outRateInt
					rel++
				}
			}
		}
	case inCh == 1 && outCh == 2:
		last0 := int32(f.last[0])
		d := dst[:count*4]
		j := 0
		for ; j < count && base < inPos; j++ {
			a := last0
			value := a
			if frac != 0 {
				var b int32
				if base+1 < inPos {
					b = last0
				} else {
					b = int32(getS16(input, int(base+1-inPos)*2))
				}
				w := int32((uint64(frac) * inv) >> 16)
				value = a + ((b-a)*w)>>16
			}
			u := uint32(uint16(int16(value)))
			binary.LittleEndian.PutUint32(d[j*4:], u|u<<16)
			base += step
			frac += rem
			if frac >= outRateInt {
				frac -= outRateInt
				base++
			}
		}
		if j < count {
			rel := int(base - inPos)
			for ; j < count; j++ {
				v := binary.LittleEndian.Uint32(input[rel*2:])
				a := int32(int16(v))
				b := int32(int16(v >> 16))
				w := int32((uint64(frac) * inv) >> 16)
				u := uint32(uint16(int16(a + ((b-a)*w)>>16)))
				binary.LittleEndian.PutUint32(d[j*4:], u|u<<16)
				rel += stepInt
				frac += rem
				if frac >= outRateInt {
					frac -= outRateInt
					rel++
				}
			}
		}
	default:
		for j := range count {
			w := 0
			if frac != 0 {
				w = int((uint64(frac) * inv) >> 16)
			}
			for channel := range outCh {
				a := f.inputSample(input, base, channel)
				value := a
				if w != 0 {
					b := f.inputSample(input, base+1, channel)
					value = a + ((b-a)*w)>>16
				}
				putS16(dst, (j*outCh+channel)*2, int16(value))
			}
			base += step
			frac += rem
			if frac >= outRateInt {
				frac -= outRateInt
				base++
			}
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
	if f.state != resampleStreaming || f.inPos == 0 {
		return nil
	}
	count := int(ceilDiv(f.inPos*int64(f.outputRate), int64(f.inputRate)) - f.outPos)
	if count <= 0 {
		f.state = resampleFlushed
		return nil
	}
	if out == nil || len(out.Frames) == cap(out.Frames) {
		return filter.ErrResultFull
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
	f.state = resampleFlushed
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
		if frame.Audio.Samples > available {
			return available
		}
		return frame.Audio.Samples
	}
	return available
}

func remapS16Channels(dst []byte, src []byte, samples int, inputChannels int, outputChannels int) {
	switch {
	case inputChannels == 2 && outputChannels == 1:
		s := src[:samples*4]
		d := dst[:samples*2]
		i := 0
		for ; i+8 <= samples; i += 8 {
			in := s[i*4 : i*4+32]
			out := d[i*2 : i*2+16]
			v0 := binary.LittleEndian.Uint64(in)
			v1 := binary.LittleEndian.Uint64(in[8:])
			v2 := binary.LittleEndian.Uint64(in[16:])
			v3 := binary.LittleEndian.Uint64(in[24:])
			m0 := uint16((int32(int16(v0)) + int32(int16(v0>>16))) / 2)
			m1 := uint16((int32(int16(v0>>32)) + int32(int16(v0>>48))) / 2)
			m2 := uint16((int32(int16(v1)) + int32(int16(v1>>16))) / 2)
			m3 := uint16((int32(int16(v1>>32)) + int32(int16(v1>>48))) / 2)
			binary.LittleEndian.PutUint64(out, uint64(m0)|uint64(m1)<<16|uint64(m2)<<32|uint64(m3)<<48)
			m4 := uint16((int32(int16(v2)) + int32(int16(v2>>16))) / 2)
			m5 := uint16((int32(int16(v2>>32)) + int32(int16(v2>>48))) / 2)
			m6 := uint16((int32(int16(v3)) + int32(int16(v3>>16))) / 2)
			m7 := uint16((int32(int16(v3>>32)) + int32(int16(v3>>48))) / 2)
			binary.LittleEndian.PutUint64(out[8:], uint64(m4)|uint64(m5)<<16|uint64(m6)<<32|uint64(m7)<<48)
		}
		for ; i+4 <= samples; i += 4 {
			in := s[i*4 : i*4+16]
			v0 := binary.LittleEndian.Uint64(in)
			v1 := binary.LittleEndian.Uint64(in[8:])
			m0 := uint16((int32(int16(v0)) + int32(int16(v0>>16))) / 2)
			m1 := uint16((int32(int16(v0>>32)) + int32(int16(v0>>48))) / 2)
			m2 := uint16((int32(int16(v1)) + int32(int16(v1>>16))) / 2)
			m3 := uint16((int32(int16(v1>>32)) + int32(int16(v1>>48))) / 2)
			binary.LittleEndian.PutUint64(d[i*2:], uint64(m0)|uint64(m1)<<16|uint64(m2)<<32|uint64(m3)<<48)
		}
		for ; i < samples; i++ {
			v := binary.LittleEndian.Uint32(s[i*4:])
			m := (int32(int16(v)) + int32(int16(v>>16))) / 2
			binary.LittleEndian.PutUint16(d[i*2:], uint16(m))
		}
		return
	case inputChannels == 1 && outputChannels == 2:
		s := src[:samples*2]
		d := dst[:samples*4]
		i := 0
		for ; i+4 <= samples; i += 4 {
			v := binary.LittleEndian.Uint64(s[i*2:])
			s0 := v & 0xffff
			s1 := (v >> 16) & 0xffff
			s2 := (v >> 32) & 0xffff
			s3 := v >> 48
			out := d[i*4 : i*4+16]
			binary.LittleEndian.PutUint64(out, s0|s0<<16|s1<<32|s1<<48)
			binary.LittleEndian.PutUint64(out[8:], s2|s2<<16|s3<<32|s3<<48)
		}
		for ; i < samples; i++ {
			v := uint32(binary.LittleEndian.Uint16(s[i*2:]))
			binary.LittleEndian.PutUint32(d[i*4:], v|v<<16)
		}
		return
	}
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
	return int16(binary.LittleEndian.Uint16(data[offset:]))
}

func putS16(data []byte, offset int, value int16) {
	binary.LittleEndian.PutUint16(data[offset:], uint16(value))
}
