package launch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/shape"
)

// builtins is goav's grammar as elements; Use replaces or extends it.
var builtins = map[string]*element{}

func builtin(el *element) {
	if el.err != nil {
		panic(el.err)
	}
	builtins[el.name] = el
}

type nameArgs struct {
	Name string `launch:"name,positional,required"`
}

func init() {
	// file: an input at a pipeline's head, a destination at its tail.
	type fileArgs struct {
		Path   string `launch:",positional,required" help:"path"`
		Format string `launch:"format"`
	}
	file := define[fileArgs]("file", "read a file at a pipeline's start, write one at its end", "source")
	fileValue := func(_ *compiler, args reflect.Value) (value, error) {
		a := argsOf[fileArgs](args)
		return value{kind: kindFile, file: fileRef{path: a.Path, format: av.FormatID(a.Format)}}, nil
	}
	file.start, file.dest = fileValue, fileValue
	builtin(file)

	builtin(testsrcElement())
	builtin(hlsElement())

	// tap: at the start, a running task's tap; mid-pipeline, a named outlet.
	tap := define[nameArgs]("tap", "name an attach point; start from one on a running task", "operation")
	tap.start = func(c *compiler, args reflect.Value) (value, error) {
		if c.cfg.task == nil {
			return value{}, &Error{Pos: -1, Msg: "a tap starts a pipeline only on a running task", Hint: "attach the program to a task (goav ctl attach), or tap a stream: ... ! tap " + argsOf[nameArgs](args).Name, cause: errNoTask}
		}
		return value{kind: kindStream, stream: c.cfg.task.Tap(argsOf[nameArgs](args).Name)}, nil
	}
	tap.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		return value{kind: kindStream, stream: in.stream.Tap(argsOf[nameArgs](args).Name)}, nil
	}
	builtin(tap)

	type selectArgs struct {
		ID    string `launch:"id"`
		Index string `launch:"index"`
	}
	for _, kind := range []string{"video", "audio", "stream"} {
		el := define[selectArgs](kind, "select the input's "+kind+" stream", "operation")
		if kind == "stream" {
			el.summary = "select one stream of any kind"
		}
		el.selects = true
		el.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
			a := argsOf[selectArgs](args)
			var opts []goav.StreamOption
			if a.ID != "" {
				opts = append(opts, goav.StreamID(av.StreamID(a.ID)))
			}
			if a.Index != "" {
				n, err := strconv.Atoi(a.Index)
				if err != nil || n < 0 {
					return value{}, fmt.Errorf("index %q is not a stream index", a.Index)
				}
				opts = append(opts, goav.StreamIndex(n))
			}
			switch kind {
			case "video":
				return value{kind: kindStream, stream: in.input.Video(opts...)}, nil
			case "audio":
				return value{kind: kindStream, stream: in.input.Audio(opts...)}, nil
			default:
				return value{kind: kindStream, stream: in.input.Stream(opts...)}, nil
			}
		}
		builtin(el)
	}

	op := func(name, summary string, fn func(goav.Stream) goav.Stream) {
		el := define[struct{}](name, summary, "operation")
		el.apply = func(_ *compiler, _ reflect.Value, in value) (value, error) {
			return value{kind: kindStream, stream: fn(in.stream)}, nil
		}
		builtin(el)
	}
	op("decode", "turn packets into frames", func(s goav.Stream) goav.Stream { return s.Decode() })
	op("syncbypts", "align a mix's or composite's inputs by presentation time", func(s goav.Stream) goav.Stream { return s.SyncByPTS() })

	builtin(encodeElement())

	type resizeArgs struct {
		Size   Size   `launch:"size,positional"`
		Width  int    `launch:"width"`
		Height int    `launch:"height"`
		Mode   string `launch:"mode" help:"exact|fit|fill"`
	}
	resize := define[resizeArgs]("resize", "scale video frames", "operation")
	resize.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		a := argsOf[resizeArgs](args)
		w, h := a.Size.Width, a.Size.Height
		if w == 0 {
			w, h = a.Width, a.Height
		}
		if w <= 0 || h <= 0 {
			return value{}, &Error{Pos: -1, Msg: "resize needs a size", Hint: "write resize 1280x720, or resize width=1280 height=720"}
		}
		switch a.Mode {
		case "", "exact":
			return value{kind: kindStream, stream: in.stream.Resize(w, h)}, nil
		case "fit":
			return value{kind: kindStream, stream: in.stream.Resize(w, h, filter.Fit)}, nil
		case "fill":
			return value{kind: kindStream, stream: in.stream.Resize(w, h, filter.Fill)}, nil
		}
		return value{}, &Error{Pos: -1, Msg: "resize mode " + quote(a.Mode) + " is not exact, fit, or fill"}
	}
	builtin(resize)

	type resampleArgs struct {
		Rate     int `launch:"rate,positional,required"`
		Channels int `launch:"channels,positional,required"`
	}
	resample := define[resampleArgs]("resample", "convert audio to a rate and channel count", "operation")
	resample.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		a := argsOf[resampleArgs](args)
		return value{kind: kindStream, stream: in.stream.Resample(a.Rate, a.Channels)}, nil
	}
	builtin(resample)

	as := define[nameArgs]("as", "name the stream from here on (its fork and encoded stream id)", "operation")
	as.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		return value{kind: kindStream, stream: in.stream.As(argsOf[nameArgs](args).Name)}, nil
	}
	builtin(as)

	type bufferArgs struct {
		Mode     string        `launch:"mode,positional,required"`
		Capacity int           `launch:"capacity,positional"`
		MaxBytes int64         `launch:"max_bytes"`
		MaxDelay time.Duration `launch:"max_delay"`
	}
	buffer := define[bufferArgs]("buffer", "queue the rest of this fork: block, drop-oldest, drop-newest, latest, unbounded", "operation")
	buffer.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		a := argsOf[bufferArgs](args)
		var opts []flow.BranchBufferOption
		if a.MaxBytes > 0 {
			opts = append(opts, flow.BufferMaxBytes(a.MaxBytes))
		}
		if a.MaxDelay > 0 {
			opts = append(opts, flow.BufferMaxDelay(a.MaxDelay))
		}
		var b flow.BranchBuffer
		switch a.Mode {
		case "block", "drop-oldest", "drop-newest":
			if a.Capacity <= 0 {
				return value{}, &Error{Pos: -1, Msg: "buffer " + a.Mode + " needs a capacity", Hint: "write buffer " + a.Mode + " 8"}
			}
			switch a.Mode {
			case "block":
				b = flow.Blocking(a.Capacity, opts...)
			case "drop-oldest":
				b = flow.DropOldest(a.Capacity, opts...)
			default:
				b = flow.DropNewest(a.Capacity, opts...)
			}
		case "latest":
			b = flow.Latest(opts...)
		case "unbounded":
			b = flow.Unbounded(opts...)
		default:
			return value{}, &Error{Pos: -1, Msg: "buffer mode " + quote(a.Mode) + " is not block, drop-oldest, drop-newest, latest, or unbounded"}
		}
		return value{kind: kindStream, stream: in.stream.Buffer(b)}, nil
	}
	builtin(buffer)

	type playoutArgs struct {
		Name string `launch:"name,positional"`
	}
	playout := define[playoutArgs]("playout", "hold media until it is due on the task timeline", "operation")
	playout.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		return value{kind: kindStream, stream: in.stream.Playout(flow.Playout(firstNonEmpty(argsOf[playoutArgs](args).Name, "playout")))}, nil
	}
	builtin(playout)

	type syncArgs struct {
		Name      string        `launch:"name,positional,required"`
		Tolerance time.Duration `launch:"tolerance"`
		DropLate  bool          `launch:"drop_late,flag"`
	}
	sync := define[syncArgs]("sync", "share one timeline across the streams that name it", "operation")
	sync.apply = func(c *compiler, args reflect.Value, in value) (value, error) {
		a := argsOf[syncArgs](args)
		policy, ok := c.syncs[a.Name]
		if !ok {
			var opts []flow.SyncPolicy
			if a.Tolerance > 0 {
				opts = append(opts, flow.SyncTolerance(a.Tolerance))
			}
			if a.DropLate {
				opts = append(opts, flow.SyncDropLate())
			}
			policy = flow.Sync(a.Name, opts...)
			c.syncs[a.Name] = policy
		} else if a.Tolerance > 0 || a.DropLate {
			return value{}, &Error{Pos: -1, Msg: "sync " + a.Name + " is already defined", Hint: "set tolerance and drop_late where sync " + a.Name + " first appears"}
		}
		return value{kind: kindStream, stream: in.stream.Sync(policy)}, nil
	}
	builtin(sync)

	type autoArgs struct {
		Resample bool `launch:"resample,flag"`
		Resize   bool `launch:"resize,flag"`
		Convert  bool `launch:"convert,flag"`
	}
	auto := define[autoArgs]("auto", "let the planner insert these conversions", "operation")
	auto.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		a := argsOf[autoArgs](args)
		var policies []shape.Policy
		if a.Resample {
			policies = append(policies, shape.AllowResample())
		}
		if a.Resize {
			policies = append(policies, shape.AllowResize())
		}
		if a.Convert {
			policies = append(policies, shape.AllowConvert())
		}
		if len(policies) == 0 {
			return value{}, &Error{Pos: -1, Msg: "auto needs a conversion to allow", Hint: "write auto resample, auto resize, or auto convert"}
		}
		return value{kind: kindStream, stream: in.stream.Auto(policies...)}, nil
	}
	builtin(auto)

	type atArgs struct {
		X int `launch:"x,positional,required"`
		Y int `launch:"y,positional,required"`
	}
	at := define[atArgs]("at", "place a composite input on the canvas", "operation")
	at.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		a := argsOf[atArgs](args)
		return value{kind: kindLayer, layer: goav.At(in.stream, a.X, a.Y)}, nil
	}
	builtin(at)

	null := define[struct{}]("null", "discard what arrives", "destination")
	null.dest = func(c *compiler, _ reflect.Value) (value, error) {
		if d, ok := c.dests["\x00null"]; ok {
			return value{kind: kindDest, dest: d}, nil
		}
		d := goav.Sink(component.SinkFunc("null", func(context.Context, component.Message) error { return nil }))
		c.dests["\x00null"] = d
		return value{kind: kindDest, dest: d}, nil
	}
	builtin(null)

	join := func(name, summary string, fn func(inputs []value, call *termNode) (goav.Stream, error)) {
		el := define[struct{}](name, summary, "join")
		el.join = func(_ *compiler, _ reflect.Value, inputs []value, call *termNode) (value, error) {
			s, err := fn(inputs, call)
			return value{kind: kindStream, stream: s}, err
		}
		builtin(el)
	}
	join("mix", "sum audio streams", func(inputs []value, call *termNode) (goav.Stream, error) {
		streams, err := joinStreams(inputs, call)
		return goav.Mix(streams...), err
	})
	join("select", "forward one stream at a time", func(inputs []value, call *termNode) (goav.Stream, error) {
		streams, err := joinStreams(inputs, call)
		return goav.Select(streams...), err
	})
	join("composite", "draw video streams on one canvas", func(inputs []value, call *termNode) (goav.Stream, error) {
		layers := make([]goav.Layer, 0, len(inputs))
		for i, in := range inputs {
			switch in.kind {
			case kindLayer:
				layers = append(layers, in.layer)
			case kindStream:
				layers = append(layers, in.stream)
			default:
				return goav.Stream{}, joinInputError(call, i, in)
			}
		}
		return goav.Composite(layers...), nil
	})
}

func joinStreams(inputs []value, call *termNode) ([]goav.Stream, error) {
	streams := make([]goav.Stream, 0, len(inputs))
	for i, in := range inputs {
		if in.kind != kindStream {
			return nil, joinInputError(call, i, in)
		}
		streams = append(streams, in.stream)
	}
	return streams, nil
}

func joinInputError(call *termNode, i int, in value) error {
	arg := call.callArgs[i]
	hint := "pass streams: " + call.name + "(a ! audio, b ! audio)"
	if in.kind == kindLayer {
		hint = "at places composite inputs only"
	}
	return &Error{Pos: arg.pos, End: arg.end, Msg: call.name + " takes streams, not " + in.kind.String(), Hint: hint}
}

func (c *compiler) call(t *termNode) (value, error) {
	el, ok := c.cfg.elements[t.name]
	if !ok {
		return value{}, c.unknown(t)
	}
	if el.join == nil {
		return value{}, &Error{Pos: t.pos, End: t.pos + len(t.name), Msg: t.name + " is not a join", Hint: "joins are called with streams: mix(a, b), composite(a, b), select(a, b)"}
	}
	args, err := el.bind(&termNode{name: t.name, pos: t.pos, end: t.end, args: t.args})
	if err != nil {
		return value{}, err
	}
	inputs := make([]value, 0, len(t.callArgs))
	for _, arg := range t.callArgs {
		v, _, err := c.pipe(arg, true)
		if err != nil {
			return value{}, err
		}
		inputs = append(inputs, v)
	}
	if len(inputs) < 2 {
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " needs at least two streams", Hint: t.name + "(a, b)"}
	}
	v, joinErr := el.join(c, args, inputs, t)
	if joinErr != nil {
		var e *Error
		if errors.As(joinErr, &e) {
			return value{}, e
		}
		return value{}, termError(t, joinErr)
	}
	return v, nil
}

func encodeElement() *element {
	type encodeArgs struct {
		Codec            string            `launch:"codec,positional,required"`
		Media            string            `launch:"media" help:"video|audio"`
		Bitrate          int               `launch:"bitrate"`
		FPS              string            `launch:"fps" help:"n[/d]"`
		KeyframeInterval int               `launch:"keyframe_interval"`
		Profile          string            `launch:"profile"`
		Level            string            `launch:"level"`
		Channels         int               `launch:"channels"`
		SampleRate       int               `launch:"sample_rate"`
		ClockRate        uint32            `launch:"clock_rate"`
		Native           map[string]string `launch:",rest"`
	}
	el := define[encodeArgs]("encode", "encode frames; native adapter settings pass through", "operation")
	el.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		a := argsOf[encodeArgs](args)
		var opts []codec.Option
		if a.Bitrate > 0 {
			opts = append(opts, codec.Bitrate(a.Bitrate))
		}
		if a.FPS != "" {
			fps, ok := parseRate(a.FPS)
			if !ok {
				return value{}, &Error{Pos: -1, Msg: "fps " + quote(a.FPS) + " is not a frame rate", Hint: "write fps=30 or fps=30000/1001"}
			}
			opts = append(opts, codec.FPS(fps.num, fps.den))
		}
		if a.KeyframeInterval > 0 {
			opts = append(opts, codec.KeyframeInterval(a.KeyframeInterval))
		}
		if a.Profile != "" {
			opts = append(opts, codec.Profile(a.Profile))
		}
		if a.Level != "" {
			opts = append(opts, codec.Level(a.Level))
		}
		if a.Channels > 0 {
			opts = append(opts, codec.Channels(a.Channels))
		}
		if a.SampleRate > 0 {
			opts = append(opts, codec.SampleRate(a.SampleRate))
		}
		if a.ClockRate > 0 {
			opts = append(opts, codec.ClockRate(a.ClockRate))
		}
		keys := make([]string, 0, len(a.Native))
		for k := range a.Native {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			opts = append(opts, codec.Setting(k, a.Native[k]))
		}
		id := av.CodecID(a.Codec)
		media := av.MediaType(a.Media)
		if media == "" && !knownCodec(id) {
			return value{}, &Error{Pos: -1, Msg: "goav does not know which media " + a.Codec + " encodes", Hint: "write encode " + a.Codec + " media=video (or media=audio)"}
		}
		return value{kind: kindStream, stream: in.stream.Encode(codecSpec(id, media, opts...))}, nil
	}
	return el
}

func knownCodec(id av.CodecID) bool {
	switch id {
	case av.CodecVP8, av.CodecVP9, av.CodecAV1, av.CodecH264, av.CodecOpus:
		return true
	}
	return false
}

// codecSpec is the codec package's constructor for a known codec, so a
// program's encoder is the value Go code would build.
func codecSpec(id av.CodecID, media av.MediaType, opts ...codec.Option) codec.Spec {
	switch id {
	case av.CodecVP8:
		return codec.VP8(opts...)
	case av.CodecVP9:
		return codec.VP9(opts...)
	case av.CodecAV1:
		return codec.AV1(opts...)
	case av.CodecH264:
		return codec.H264(opts...)
	case av.CodecOpus:
		return codec.Opus(opts...)
	}
	return codec.Codec(id, media, opts...)
}
