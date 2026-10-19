package codectest

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"io"
	"math"
	"slices"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/mp4"
	"github.com/thesyncim/goav/format"
)

// Fixtures encoded by independent encoders (x264, FFmpeg's AAC), so a
// decoder is checked against a bitstream its own encoder did not make.
//
//go:embed testdata/*.mp4
var fixtures embed.FS

const (
	videoWidth  = 320
	videoHeight = 240
	videoFrames = 30
	frameTicks  = 3000 // 30 fps at 90 kHz
	sampleRate  = 48000
	channels    = 2
	audioFrame  = 1024
	audioFrames = 47 // one second
)

var videoBase = av.TimeBase{Num: 1, Den: 90000}

// pattern is frame n of a textured, moving I420 test sequence: gradients, a
// drifting checkerboard, and a sliding bar, so a codec's PSNR measures real
// work.
func pattern(n int) *av.Frame {
	w, h := videoWidth, videoHeight
	cw, ch := w/2, h/2
	y, u, v := make([]byte, w*h), make([]byte, cw*ch), make([]byte, cw*ch)
	for r := range h {
		for c := range w {
			value := 40 + (c*3+r*2+n*4)%120
			if ((c+n*2)/20+(r+n)/20)%2 == 0 {
				value += 60
			}
			if c >= (n*7)%w && c < (n*7)%w+24 {
				value = 230
			}
			y[r*w+c] = byte(value)
		}
	}
	for r := range ch {
		for c := range cw {
			u[r*cw+c] = byte(64 + (c*2+n)%128)
			v[r*cw+c] = byte(64 + (r*2+n*3)%128)
		}
	}
	return &av.Frame{
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: int64(n) * frameTicks, Base: videoBase},
		Duration: av.Duration{Value: frameTicks, Base: videoBase},
		Video:    &av.VideoFrame{Width: w, Height: h, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: y}, Stride: w},
			{Buffer: av.Buffer{Bytes: u}, Stride: cw},
			{Buffer: av.Buffer{Bytes: v}, Stride: cw},
		},
	}
}

// tone is audio frame n of a two-tone stereo signal.
func tone(n int) *av.Frame {
	pcm := make([]int16, audioFrame*channels)
	for i := range audioFrame {
		t := float64(n*audioFrame+i) / sampleRate
		pcm[i*2] = int16(9000 * math.Sin(2*math.Pi*440*t))
		pcm[i*2+1] = int16(9000 * math.Sin(2*math.Pi*660*t))
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(pcm))), len(pcm)*2)
	base := av.TimeBase{Num: 1, Den: sampleRate}
	return &av.Frame{
		Type:     av.MediaAudio,
		PTS:      av.Timestamp{Value: int64(n * audioFrame), Base: base},
		Duration: av.Duration{Value: audioFrame, Base: base},
		Audio:    &av.AudioFrame{SampleRate: sampleRate, Channels: channels, ChannelLayout: "stereo", SampleFormat: av.SampleFormatS16, Samples: audioFrame},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: data}}},
	}
}

// lumaPSNR compares the luma of two I420 frames.
func lumaPSNR(a, b *av.Frame) float64 {
	w, h := a.Video.Width, a.Video.Height
	if b.Video == nil || b.Video.Width != w || b.Video.Height != h {
		return 0
	}
	pa, pb := a.Planes[0], b.Planes[0]
	var mse float64
	for r := range h {
		ra := pa.Buffer.Bytes[pa.Offset+r*stride(pa, w):]
		rb := pb.Buffer.Bytes[pb.Offset+r*stride(pb, w):]
		for c := range w {
			d := float64(ra[c]) - float64(rb[c])
			mse += d * d
		}
	}
	mse /= float64(w * h)
	if mse == 0 {
		return math.Inf(1)
	}
	return 10 * math.Log10(255*255/mse)
}

func stride(p av.Plane, w int) int {
	if p.Stride > 0 {
		return p.Stride
	}
	return w
}

// pcm reads a decoded s16 frame's samples.
func pcm(frame *av.Frame) []int16 {
	data := frame.Planes[0].Buffer.Bytes[frame.Planes[0].Offset:]
	return unsafe.Slice((*int16)(unsafe.Pointer(unsafe.SliceData(data))), len(data)/2)
}

// timeline lays decoded audio frames out by PTS, sample for sample.
func timeline(frames []*av.Frame) map[int64]int16 {
	out := map[int64]int16{}
	for _, frame := range frames {
		start := frame.PTS.Value * channels
		if frame.PTS.Base.Den != sampleRate && frame.PTS.Base.Valid() {
			if v, ok := frame.PTS.Rescale(av.TimeBase{Num: 1, Den: sampleRate}); ok {
				start = v.Value * channels
			}
		}
		for i, s := range pcm(frame) {
			out[start+int64(i)] = s
		}
	}
	return out
}

// snr is the signal-to-noise ratio, in dB, of got against want over the
// samples both cover, skipping edge samples codecs fade in and out.
func snr(want, got map[int64]int16, skip int64) (float64, int) {
	var signal, noise float64
	n := 0
	for i, w := range want {
		if i < skip || i >= int64(len(want))-skip {
			continue
		}
		g, ok := got[i]
		if !ok {
			continue
		}
		signal += float64(w) * float64(w)
		d := float64(w) - float64(g)
		noise += d * d
		n++
	}
	if noise == 0 {
		return math.Inf(1), n
	}
	return 10 * math.Log10(signal/noise), n
}

// demuxFixture reads one stream of an embedded MP4 fixture: its stream and
// its packets, with payloads copied out of the demuxer's scratch.
func demuxFixture(name string, media av.MediaType) (av.Stream, []av.Packet, error) {
	data, err := fixtures.ReadFile("testdata/" + name)
	if err != nil {
		return av.Stream{}, nil, err
	}
	return demux(data, media)
}

func demux(data []byte, media av.MediaType) (av.Stream, []av.Packet, error) {
	ctx := context.Background()
	demuxer, err := mp4.DemuxerFactory{}.NewDemuxer(ctx, format.ProbeResult{Format: av.FormatMP4})
	if err != nil {
		return av.Stream{}, nil, err
	}
	defer demuxer.Close()
	if err := demuxer.Open(ctx, format.Input{Name: "fixture.mp4", ReaderAt: bytes.NewReader(data), Size: int64(len(data))}, format.OpenOptions{}); err != nil {
		return av.Stream{}, nil, err
	}
	streams := demuxer.Streams()
	index := slices.IndexFunc(streams, func(s av.Stream) bool { return s.Type == media })
	if index < 0 {
		return av.Stream{}, nil, errors.New("codectest: no " + string(media) + " stream")
	}
	var packets []av.Packet
	for {
		packet := av.Packet{}
		result := format.ReadResult{Packet: &packet}
		err := demuxer.ReadInto(ctx, &result)
		if errors.Is(err, io.EOF) {
			return streams[index], packets, nil
		}
		if err != nil {
			return av.Stream{}, nil, err
		}
		if result.PacketReady && packet.StreamID == streams[index].ID {
			packet.Payload = av.Buffer{Bytes: slices.Clone(packet.Payload.Bytes)}
			packets = append(packets, packet)
		}
	}
}

// muxMP4 writes packets of stream into an MP4 file in memory: progressive,
// or fragmented as for HLS and other pipes.
func muxMP4(stream av.Stream, packets []*av.Packet, fragmented bool) ([]byte, error) {
	ctx := context.Background()
	muxer, err := mp4.MuxerFactory{}.NewMuxer(ctx, av.FormatMP4)
	if err != nil {
		return nil, err
	}
	var out seekBuffer
	var writer io.Writer = &out
	if fragmented {
		writer = pipe{&out}
	}
	if err := muxer.Open(ctx, format.Output{Name: "out.mp4", Writer: writer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		return nil, err
	}
	for _, packet := range packets {
		if err := muxer.Write(ctx, packet, nil); err != nil {
			muxer.Close()
			return nil, err
		}
	}
	if err := muxer.Close(); err != nil {
		return nil, err
	}
	return out.data, nil
}

// seekBuffer is an in-memory io.WriteSeeker, so the muxer writes a
// progressive file.
type seekBuffer struct {
	data []byte
	pos  int64
}

func (b *seekBuffer) Write(p []byte) (int, error) {
	end := b.pos + int64(len(p))
	if end > int64(len(b.data)) {
		b.data = append(b.data, make([]byte, end-int64(len(b.data)))...)
	}
	copy(b.data[b.pos:], p)
	b.pos = end
	return len(p), nil
}

func (b *seekBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.pos = offset
	case io.SeekCurrent:
		b.pos += offset
	case io.SeekEnd:
		b.pos = int64(len(b.data)) + offset
	}
	return b.pos, nil
}

// muxable reports a codec the MP4 muxer writes.
func muxable(id av.CodecID) bool {
	switch id {
	case av.CodecH264, av.CodecAAC, av.CodecAV1, av.CodecVP8, av.CodecVP9, av.CodecOpus:
		return true
	}
	return false
}

var _ = codec.ErrClosed

// pipe hides a writer's Seek, so the muxer writes fragments.
type pipe struct{ w io.Writer }

func (p pipe) Write(b []byte) (int, error) { return p.w.Write(b) }
