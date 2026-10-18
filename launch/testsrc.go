package launch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/cliargs"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

const (
	rawVideoCodec = av.CodecID("goav_raw_video")
	rawAudioCodec = av.CodecID("goav_raw_audio")
	// audioFrameMS is the length of one testsrc audio frame.
	audioFrameMS = 20
)

type testsrcArgs struct {
	Kind     string        `launch:",positional,required" help:"video|audio"`
	Name     string        `launch:"name"`
	Size     Size          `launch:"size"`
	FPS      string        `launch:"fps" help:"n[/d]"`
	Pattern  string        `launch:"pattern" help:"gradient|bars|solid"`
	Rate     int           `launch:"rate"`
	Channels int           `launch:"channels"`
	Tone     float64       `launch:"tone"`
	Frames   int           `launch:"frames"`
	Duration time.Duration `launch:"duration"`
	Live     bool          `launch:"live,flag"`
}

func testsrcElement() *element {
	el := define[testsrcArgs]("testsrc", "generate test video or audio; live paces it in real time", "source")
	el.start = func(c *compiler, args reflect.Value) (value, error) {
		a := argsOf[testsrcArgs](args)
		src, err := newTestSource(a)
		if err != nil {
			return value{}, err
		}
		src.name = c.uniqueName(firstNonEmpty(a.Name, a.Kind))
		// A test source carries one stream of a known kind: it is that stream.
		in := goav.Input(testProvider{src})
		if src.video {
			return value{kind: kindStream, stream: in.Video()}, nil
		}
		return value{kind: kindStream, stream: in.Audio()}, nil
	}
	return el
}

// uniqueName gives each generated input a distinct stream id.
func (c *compiler) uniqueName(name string) string {
	if c.names == nil {
		c.names = map[string]int{}
	}
	c.names[name]++
	if n := c.names[name]; n > 1 {
		return fmt.Sprintf("%s-%d", name, n)
	}
	return name
}

type testSource struct {
	name     string
	video    bool
	width    int
	height   int
	fps      cliargs.FPS
	pattern  string
	rate     int
	channels int
	tone     float64
	frames   int // 0: forever
	live     bool
}

func newTestSource(a testsrcArgs) (testSource, error) {
	s := testSource{live: a.Live}
	switch a.Kind {
	case "video":
		s.video = true
		s.width, s.height = 640, 360
		if a.Size.Width > 0 {
			s.width, s.height = a.Size.Width, a.Size.Height
		}
		s.fps = cliargs.FPS{Num: 30, Den: 1}
		if a.FPS != "" {
			fps, err := cliargs.ParseFPS(a.FPS)
			if err != nil {
				return s, &Error{Pos: -1, Msg: "fps " + quote(a.FPS) + " is not a frame rate", Hint: "write fps=30 or fps=30000/1001"}
			}
			s.fps = fps
		}
		s.pattern = firstNonEmpty(a.Pattern, "gradient")
		switch s.pattern {
		case "gradient", "bars", "solid":
		default:
			return s, &Error{Pos: -1, Msg: "pattern " + quote(a.Pattern) + " is not gradient, bars, or solid"}
		}
		if a.Rate != 0 || a.Channels != 0 || a.Tone != 0 {
			return s, &Error{Pos: -1, Msg: "rate, channels, and tone set testsrc audio", Hint: "drop them from testsrc video"}
		}
	case "audio":
		s.rate, s.channels, s.tone = 48_000, 2, 440
		if a.Rate > 0 {
			s.rate = a.Rate
		}
		if a.Channels > 0 {
			s.channels = a.Channels
		}
		if a.Tone > 0 {
			s.tone = a.Tone
		}
		s.fps = cliargs.FPS{Num: 1000, Den: audioFrameMS}
		if a.Size.Width != 0 || a.FPS != "" || a.Pattern != "" {
			return s, &Error{Pos: -1, Msg: "size, fps, and pattern set testsrc video", Hint: "drop them from testsrc audio"}
		}
	default:
		return s, &Error{Pos: -1, Msg: "testsrc makes video or audio, not " + quote(a.Kind), Hint: "write testsrc video or testsrc audio"}
	}
	switch {
	case a.Frames > 0 && a.Duration > 0:
		return s, &Error{Pos: -1, Msg: "set frames or duration, not both"}
	case a.Frames > 0:
		s.frames = a.Frames
	case a.Duration > 0:
		s.frames = int(math.Ceil(a.Duration.Seconds() * float64(s.fps.Num) / float64(s.fps.Den)))
	}
	return s, nil
}

func (s testSource) frameDuration() time.Duration {
	return time.Duration(float64(time.Second) * float64(s.fps.Den) / float64(s.fps.Num))
}

func (s testSource) stream() av.Stream {
	if s.video {
		return av.Stream{
			ID:       av.StreamID(s.name),
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: int64(s.fps.Den), Den: int64(s.fps.Num)},
			Codec:    av.CodecParameters{ID: rawVideoCodec, Type: av.MediaVideo, Width: s.width, Height: s.height, PixelFormat: av.PixelFormatI420},
			Name:     s.name,
		}
	}
	return av.Stream{
		ID:       av.StreamID(s.name),
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: int64(s.rate)},
		Codec:    av.CodecParameters{ID: rawAudioCodec, Type: av.MediaAudio, SampleRate: s.rate, Channels: s.channels, SampleFormat: av.SampleFormatS16},
		Name:     s.name,
	}
}

func (s testSource) shape() shape.Spec {
	if s.video {
		return shape.Frame(av.MediaVideo, shape.Video(s.width, s.height, av.PixelFormatI420), shape.Stream(av.StreamID(s.name)), shape.Codec(rawVideoCodec), shape.Realtime(s.live))
	}
	return shape.Frame(av.MediaAudio, shape.Audio(s.rate, s.channels, av.SampleFormatS16), shape.Stream(av.StreamID(s.name)), shape.Codec(rawAudioCodec), shape.Realtime(s.live))
}

func (s testSource) frame(n int) *av.Frame {
	if s.video {
		return s.videoFrame(n)
	}
	return s.audioFrame(n)
}

func (s testSource) videoFrame(n int) *av.Frame {
	chromaW, chromaH := (s.width+1)/2, (s.height+1)/2
	luma := make([]byte, s.width*s.height)
	for y := 0; y < s.height; y++ {
		for x := 0; x < s.width; x++ {
			luma[y*s.width+x] = s.luma(x, y, n)
		}
	}
	chroma := make([]byte, 2*chromaW*chromaH)
	for i := range chroma {
		chroma[i] = 128
	}
	base := av.TimeBase{Num: int64(s.fps.Den), Den: int64(s.fps.Num)}
	return &av.Frame{
		StreamID: av.StreamID(s.name),
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: int64(n), Base: base},
		Duration: av.Duration{Value: 1, Base: base},
		Video:    &av.VideoFrame{Width: s.width, Height: s.height, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: luma, Ownership: av.BufferImmutable}, Stride: s.width},
			{Buffer: av.Buffer{Bytes: chroma[:chromaW*chromaH], Ownership: av.BufferImmutable}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: chroma[chromaW*chromaH:], Ownership: av.BufferImmutable}, Stride: chromaW},
		},
	}
}

func (s testSource) luma(x, y, n int) byte {
	switch s.pattern {
	case "bars":
		bars := [...]byte{32, 64, 96, 128, 160, 192, 224, 240}
		return bars[x*len(bars)/max(s.width, 1)%len(bars)] + byte(n%16)
	case "solid":
		return byte(n)
	default:
		return byte((x + y + n*3) % 256)
	}
}

func (s testSource) audioFrame(n int) *av.Frame {
	samples := s.rate * audioFrameMS / 1000
	payload := make([]byte, samples*s.channels*2)
	start := n * samples
	for i := 0; i < samples; i++ {
		v := int16(math.Round(8000 * math.Sin(2*math.Pi*s.tone*float64(start+i)/float64(s.rate))))
		for ch := 0; ch < s.channels; ch++ {
			binary.LittleEndian.PutUint16(payload[(i*s.channels+ch)*2:], uint16(v))
		}
	}
	base := av.TimeBase{Num: 1, Den: int64(s.rate)}
	return &av.Frame{
		StreamID: av.StreamID(s.name),
		Type:     av.MediaAudio,
		PTS:      av.Timestamp{Value: int64(start), Base: base},
		Duration: av.Duration{Value: int64(samples), Base: base},
		Audio:    &av.AudioFrame{SampleRate: s.rate, Channels: s.channels, SampleFormat: av.SampleFormatS16, Samples: samples},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
	}
}

func (s testSource) frameIndex(position time.Duration, ceil bool) int {
	exact := position.Seconds() * float64(s.fps.Num) / float64(s.fps.Den)
	index := int(math.Floor(exact))
	if ceil {
		index = int(math.Ceil(exact))
	}
	index = max(index, 0)
	if s.frames > 0 {
		index = min(index, s.frames)
	}
	return index
}

// testProvider opens testsrc as a provider input: it takes the task clock
// (so rate and pause scale it) and seek and segment controls.
type testProvider struct{ src testSource }

func (p testProvider) Name() string { return p.src.name }
func (p testProvider) Detail() string {
	return "generated test " + map[bool]string{true: "video", false: "audio"}[p.src.video]
}
func (p testProvider) SourceShape() shape.Spec { return p.src.shape() }
func (p testProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	return &testPipelineSource{src: p.src, clock: av.MonotonicClock(), end: p.src.frames}, []av.Stream{p.src.stream()}, nil
}

type testPipelineSource struct {
	src   testSource
	clock av.Clock

	mu            sync.Mutex
	next          int
	end           int // 0: forever
	discontinuity bool
	closed        bool
}

var _ pipeline.ControllableSource = (*testPipelineSource)(nil)

func (s *testPipelineSource) UseClock(clock av.Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *testPipelineSource) Name() string { return s.src.name }

func (s *testPipelineSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.src.name, Kind: pipeline.NodeSource, Detail: testProvider{s.src}.Detail()}
}

func (s *testPipelineSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	for {
		index, discontinuity, done := s.nextFrame()
		if done {
			event := av.Event{Type: av.EventEndOfStream, StreamID: av.StreamID(s.src.name)}
			return s.emit(ctx, emitter, pipeline.Message{Kind: pipeline.MessageEvent, Event: &event})
		}
		if discontinuity {
			event := av.Event{Type: av.EventDiscontinuity, StreamID: av.StreamID(s.src.name), Reason: "testsrc repositioned"}
			if err := s.emit(ctx, emitter, pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}); err != nil {
				return err
			}
		}
		if err := s.emit(ctx, emitter, pipeline.Message{Kind: pipeline.MessageFrame, Frame: s.src.frame(index)}); err != nil {
			return err
		}
		if s.src.live {
			if err := s.clock.Sleep(ctx, s.src.frameDuration()); err != nil {
				return nil
			}
		}
	}
}

func (s *testPipelineSource) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return errors.New("testsrc: a control must be an event")
	}
	event := *msg.Event
	s.mu.Lock()
	defer s.mu.Unlock()
	switch event.Type {
	case av.EventSeek:
		position, ok := event.Timestamp.ToDuration()
		if !ok {
			return errors.New("testsrc: malformed seek")
		}
		s.next, s.end, s.discontinuity = s.src.frameIndex(position, false), s.src.frames, true
	case av.EventSegment:
		start, ok := event.Timestamp.ToDuration()
		end, endOK := av.EventSegmentEnd(&event)
		if !ok || !endOK || end <= start {
			return errors.New("testsrc: malformed segment")
		}
		s.next, s.end, s.discontinuity = s.src.frameIndex(start, false), max(s.src.frameIndex(end, true), s.src.frameIndex(start, false)+1), true
	default:
		return fmt.Errorf("testsrc: cannot apply control %q", event.Type)
	}
	return nil
}

func (s *testPipelineSource) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *testPipelineSource) nextFrame() (int, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || (s.end > 0 && s.next >= s.end) {
		return 0, false, true
	}
	index := s.next
	s.next++
	discontinuity := s.discontinuity
	s.discontinuity = false
	return index, discontinuity, false
}

func (s *testPipelineSource) emit(ctx context.Context, emitter pipeline.Emitter, msg pipeline.Message) error {
	for {
		err := emitter.Emit(ctx, &msg)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, pipeline.ErrBackpressure):
			if ctx.Err() != nil {
				return nil
			}
		case errors.Is(err, pipeline.ErrClosed):
			return nil
		default:
			return err
		}
	}
}
