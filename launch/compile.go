package launch

import (
	"sort"
	"strings"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/flow"
)

// Compile turns a program into the Outputs it describes. The Outputs are
// ordinary goav values — run them with rt.Run, describe them with
// rt.Describe — and they are exactly the values the same recipe written in Go
// would produce: a program costs nothing at run time.
//
//	outs, err := launch.Compile(`
//	    in := file in.webm
//	    v  := in ! video ! decode
//	    v ! resize 1280x720 ! encode vp9 bitrate=2M ! as hd ! file hd.webm
//	    v ! resize 640x360  ! encode vp9 bitrate=800k ! as sd ! file sd.webm
//	`, launch.Files(launch.OS()))
//
// A refusal is a *Error that points at the offending text.
func Compile(program string, options ...Option) ([]goav.Output, error) {
	cfg := newConfig(options)
	if cfg.err != nil {
		return nil, cfg.err
	}
	stmts, err := parse(program)
	if err != nil {
		return nil, locate(err, program)
	}
	c := &compiler{
		cfg:    cfg,
		scope:  map[string]value{},
		inputs: map[string]goav.InputSpec{},
		dests:  map[string]goav.Destination{},
		syncs:  map[string]flow.SyncPolicy{},
	}
	for _, stmt := range stmts {
		if err := c.stmt(stmt); err != nil {
			return nil, locate(err, program)
		}
	}
	if len(c.outs) == 0 {
		return nil, (&Error{Pos: -1, Msg: "the program routes nothing", Hint: "end a pipeline at a destination: ... ! file out.webm"}).locate(program)
	}
	return c.outs, nil
}

func locate(err error, src string) error {
	if e, ok := err.(*Error); ok {
		return e.locate(src)
	}
	return err
}

type valueKind uint8

const (
	kindInput valueKind = iota + 1
	kindStream
	kindFile // a file path: an input at a pipeline's head, a destination at its tail
	kindDest
	kindLayer
)

func (k valueKind) String() string {
	switch k {
	case kindInput:
		return "an input"
	case kindStream:
		return "a stream"
	case kindFile:
		return "a file"
	case kindDest:
		return "a destination"
	default:
		return "a positioned layer"
	}
}

type value struct {
	kind   valueKind
	input  goav.InputSpec
	stream goav.Stream
	dest   goav.Destination
	layer  goav.JoinArg
	file   fileRef
}

type fileRef struct {
	path   string
	format av.FormatID
}

type compiler struct {
	cfg    *config
	scope  map[string]value
	inputs map[string]goav.InputSpec
	dests  map[string]goav.Destination
	syncs  map[string]flow.SyncPolicy
	names  map[string]int
	outs   []goav.Output
}

func (c *compiler) stmt(stmt *stmtNode) error {
	if stmt.name != "" {
		if _, taken := c.scope[stmt.name]; taken {
			return &Error{Pos: stmt.namePos, End: stmt.nameEnd, Msg: stmt.name + " is already bound", Hint: "bind each name once; a stream used twice forks"}
		}
		if _, ok := c.cfg.elements[stmt.name]; ok {
			return &Error{Pos: stmt.namePos, End: stmt.nameEnd, Msg: stmt.name + " is an element name", Hint: "choose another name for the binding"}
		}
	}
	v, routed, err := c.pipe(stmt.pipe, false)
	if err != nil {
		return err
	}
	switch {
	case stmt.name != "" && routed:
		return &Error{Pos: stmt.namePos, End: stmt.pipe.end, Msg: "a binding names a stream, not a routed pipeline", Hint: "bind the stream and route it in its own statement: " + stmt.name + " := ...; " + stmt.name + " ! ... ! file out.webm"}
	case stmt.name != "":
		c.scope[stmt.name] = v
	case !routed:
		return &Error{Pos: stmt.pipe.pos, End: stmt.pipe.end, Msg: "this pipeline goes nowhere", Hint: "end it at a destination (... ! file out.webm), or bind it: name := ..."}
	}
	return nil
}

// pipe evaluates a pipeline. It reports routed when the pipeline ended at
// destinations, which appends its Output.
func (c *compiler) pipe(p *pipeNode, inCall bool) (value, bool, error) {
	head := p.segs[0]
	if len(head) > 1 {
		return value{}, false, &Error{Pos: head[1].pos, End: head[1].end, Msg: "a comma separates destinations, which come last", Hint: "start a pipeline with one input or stream"}
	}
	cur, err := c.head(head[0])
	if err != nil {
		return value{}, false, err
	}
	for i, seg := range p.segs[1:] {
		last := i == len(p.segs)-2
		if last && !inCall && c.isDestinationSegment(seg) {
			out, err := c.route(cur, seg, head[0])
			if err != nil {
				return value{}, false, err
			}
			c.outs = append(c.outs, out)
			return value{}, true, nil
		}
		if len(seg) > 1 {
			return value{}, false, &Error{Pos: seg[1].pos, End: seg[1].end, Msg: "a comma separates destinations, which come last", Hint: "join operations with !"}
		}
		cur, err = c.apply(cur, seg[0])
		if err != nil {
			return value{}, false, err
		}
	}
	if cur.kind == kindFile && !inCall {
		// A lone file is a file value; its use decides input or destination.
		return cur, false, nil
	}
	if cur.kind == kindFile {
		cur, err = c.fileInput(cur.file, head[0])
	}
	return cur, false, err
}

// head evaluates a pipeline's first term: a binding, a call, or an element
// that starts a pipeline.
func (c *compiler) head(t *termNode) (value, error) {
	if t.call {
		return c.call(t)
	}
	if v, ok := c.scope[t.name]; ok && len(t.args) == 0 {
		return v, nil
	}
	el, ok := c.cfg.elements[t.name]
	if !ok {
		return value{}, c.unknown(t)
	}
	if el.start == nil {
		hint := "start a pipeline with an input (file, testsrc, a binding) or a join"
		if el.apply != nil {
			hint = t.name + " transforms a stream: put it after one, as in file in.webm ! video ! " + t.name
		}
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " cannot start a pipeline", Hint: hint}
	}
	args, err := el.bind(t)
	if err != nil {
		return value{}, err
	}
	v, buildErr := el.start(c, args)
	if buildErr != nil {
		return value{}, termError(t, buildErr)
	}
	return v, nil
}

func (c *compiler) apply(cur value, t *termNode) (value, error) {
	if t.call {
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + "(...) starts a pipeline", Hint: "use a join as a pipeline's first step: " + t.name + "(a, b) ! ..."}
	}
	el, ok := c.cfg.elements[t.name]
	if !ok {
		if _, bound := c.scope[t.name]; bound {
			return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " is a binding, not an operation", Hint: "start a pipeline with it: " + t.name + " ! ..."}
		}
		return value{}, c.unknown(t)
	}
	if el.apply == nil {
		if el.dest != nil {
			return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " is a destination, which comes last", Hint: "end the pipeline with it, and list several with commas: ... ! file a.webm, file b.webm"}
		}
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " starts a pipeline", Hint: "begin a new statement with it"}
	}
	if cur.kind == kindFile {
		in, err := c.fileInput(cur.file, t)
		if err != nil {
			return value{}, err
		}
		cur = in
	}
	if cur.kind == kindInput && !el.selects {
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " needs a stream, and an input carries several", Hint: "select one first: ... ! video ! " + t.name + ", or ... ! audio ! " + t.name}
	}
	if cur.kind == kindStream && el.selects {
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " selects a stream of an input", Hint: "select right after the input: file in.webm ! " + t.name}
	}
	if cur.kind == kindDest || cur.kind == kindLayer {
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: t.name + " cannot follow " + cur.kind.String()}
	}
	args, err := el.bind(t)
	if err != nil {
		return value{}, err
	}
	next, buildErr := el.apply(c, args, cur)
	if buildErr != nil {
		return value{}, termError(t, buildErr)
	}
	return next, nil
}

func (c *compiler) isDestinationSegment(seg []*termNode) bool {
	for _, t := range seg {
		if t.call {
			return false
		}
		if v, ok := c.scope[t.name]; ok && len(t.args) == 0 {
			if v.kind != kindFile && v.kind != kindDest {
				return false
			}
			continue
		}
		el, ok := c.cfg.elements[t.name]
		if !ok || el.dest == nil {
			return false
		}
	}
	return true
}

func (c *compiler) route(cur value, seg []*termNode, head *termNode) (goav.Output, error) {
	dests := make([]goav.Destination, 0, len(seg))
	for _, t := range seg {
		d, err := c.destination(t)
		if err != nil {
			return goav.Output{}, err
		}
		dests = append(dests, d)
	}
	switch cur.kind {
	case kindFile:
		in, err := c.fileInput(cur.file, head)
		if err != nil {
			return goav.Output{}, err
		}
		return in.input.To(dests...), nil
	case kindInput:
		return cur.input.To(dests...), nil
	case kindStream:
		return cur.stream.To(dests...), nil
	default:
		return goav.Output{}, &Error{Pos: seg[0].pos, End: seg[0].end, Msg: "only an input or a stream can be routed to destinations"}
	}
}

func (c *compiler) destination(t *termNode) (goav.Destination, error) {
	if v, ok := c.scope[t.name]; ok && len(t.args) == 0 {
		if v.kind == kindDest {
			return v.dest, nil
		}
		return c.fileDestination(v.file, t)
	}
	el := c.cfg.elements[t.name]
	args, err := el.bind(t)
	if err != nil {
		return goav.Destination{}, err
	}
	v, buildErr := el.dest(c, args)
	if buildErr != nil {
		return goav.Destination{}, termError(t, buildErr)
	}
	if v.kind == kindFile {
		return c.fileDestination(v.file, t)
	}
	return v.dest, nil
}

// fileInput is the one input a program reads a path through.
func (c *compiler) fileInput(ref fileRef, t *termNode) (value, error) {
	if in, ok := c.inputs[ref.path]; ok {
		return value{kind: kindInput, input: in}, nil
	}
	if c.cfg.files == nil {
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: "this program may not read files", Hint: "compile with launch.Files(...) to allow it"}
	}
	if ref.format != "" {
		return value{}, &Error{Pos: t.pos, End: t.end, Msg: "an input's format is detected from its contents", Hint: "format= applies where a file is written; drop it here"}
	}
	in := goav.FileInput(ref.path, &lazyReader{fs: c.cfg.files, name: ref.path})
	c.inputs[ref.path] = in
	return value{kind: kindInput, input: in}, nil
}

// fileDestination is the one destination — one mux — a program writes a
// path through, however many pipelines end there.
func (c *compiler) fileDestination(ref fileRef, t *termNode) (goav.Destination, error) {
	if d, ok := c.dests[ref.path]; ok {
		return d, nil
	}
	if c.cfg.files == nil {
		return goav.Destination{}, &Error{Pos: t.pos, End: t.end, Msg: "this program may not write files", Hint: "compile with launch.Files(...) to allow it"}
	}
	var opts []goav.DestinationOption
	if ref.format != "" {
		opts = append(opts, goav.Format(ref.format))
	}
	d := goav.Write(ref.path, &lazyWriter{fs: c.cfg.files, name: ref.path}, opts...)
	c.dests[ref.path] = d
	return d, nil
}

func (c *compiler) unknown(t *termNode) error {
	names := make([]string, 0, len(c.cfg.elements)+len(c.scope))
	for name := range c.cfg.elements {
		names = append(names, name)
	}
	for name := range c.scope {
		names = append(names, name)
	}
	sort.Strings(names)
	hint := suggest(t.name, names)
	if hint == "" {
		hint = "goav run --elements lists the elements"
	}
	return &Error{Pos: t.pos, End: t.pos + len(t.name), Msg: "unknown element " + quote(t.name), Hint: hint}
}

func termError(t *termNode, err error) error {
	if e, ok := err.(*Error); ok {
		if e.Pos < 0 {
			e.Pos, e.End = t.pos, t.end
		}
		return e
	}
	return &Error{Pos: t.pos, End: t.end, Msg: t.name + ": " + strings.TrimPrefix(err.Error(), "goav: "), cause: err}
}

func quote(s string) string { return "\"" + s + "\"" }
