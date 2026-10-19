package ctlserver

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
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/internal/spelling"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/pipeline"
)

// newMethod makes a method whose params are T's fields.
func newMethod[T any](name, summary string, fn func(ctx context.Context, s *Server, sess *session, p T) (any, error)) (*method, error) {
	schema, err := schemaFor(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return &method{name: name, summary: summary, schema: schema,
		call: func(ctx context.Context, s *Server, sess *session, params reflect.Value) (any, error) {
			return fn(ctx, s, sess, params.Interface().(T))
		}}, nil
}

func define[T any](name, summary string, fn func(ctx context.Context, s *Server, sess *session, p T) (any, error)) *method {
	m, err := newMethod(name, summary, fn)
	if err != nil {
		panic(err)
	}
	return m
}

type none struct{}

type describeParams struct {
	Format string `json:"format" help:"json (default), text, dot, or mermaid"`
}

type watchParams struct {
	Types  []string `json:"types" help:"event types to deliver; all when empty"`
	Stream string   `json:"stream" help:"only events of this stream"`
}

type subscriptionParams struct {
	Subscription string `json:"subscription" ctl:"required" help:"the id watch returned"`
}

type streamParams struct {
	Stream string `json:"stream" ctl:"required" help:"encoded stream id, an As name"`
}

type bitrateParams struct {
	Stream  string `json:"stream" ctl:"required" help:"encoded stream id, an As name"`
	Bitrate int    `json:"bitrate" ctl:"required" help:"bits per second: 800k, 2M"`
}

type selectParams struct {
	Active string `json:"active" ctl:"required" help:"stream id of the select input to forward"`
}

type seekParams struct {
	Position time.Duration `json:"position" ctl:"required" help:"media position: 2s"`
	Source   string        `json:"source" help:"source node; every source when empty"`
}

type rateParams struct {
	Value float64 `json:"value" ctl:"required" help:"playback rate: 0.5, 2"`
}

type segmentParams struct {
	Start  time.Duration `json:"start" ctl:"required" help:"window start"`
	End    time.Duration `json:"end" ctl:"required" help:"window end"`
	Source string        `json:"source" help:"source node; every source when empty"`
}

type attachParams struct {
	Program string `json:"program" ctl:"required" help:"a pipeline program starting at a tap: tap live ! encode vp8 ! as preview ! file p.webm"`
}

type detachParams struct {
	Attachment string `json:"attachment" ctl:"required" help:"the name attach returned"`
	Outcome    string `json:"outcome" help:"close (default), drain, or abort its destinations"`
}

type rebranchParams struct {
	Attachment string `json:"attachment" ctl:"required" help:"the name attach returned"`
	Program    string `json:"program" ctl:"required" help:"the replacement program"`
	Switch     string `json:"switch" help:"now (default), next-frame, next-keyframe, or a media position: 2s"`
	Old        string `json:"old" help:"close (default), drain, or abort the old destinations"`
}

// Attached is what attach and rebranch return: the attachment's name, for
// detach and rebranch.
type Attached struct {
	Attachment string `json:"attachment"`
}

// Capabilities is what capabilities returns: the wire protocol's version,
// which changes only when the wire breaks, the server's methods, and the
// elements attached programs may use.
type Capabilities struct {
	Protocol int                  `json:"protocol"`
	Methods  []MethodInfo         `json:"methods"`
	Elements []launch.ElementInfo `json:"elements"`
}

// MethodInfo describes one method.
type MethodInfo struct {
	Name    string  `json:"name"`
	Summary string  `json:"summary"`
	Params  []Param `json:"params,omitempty"`
}

func builtinMethods() []*method {
	return []*method{
		define("describe", "the task's graph", func(_ context.Context, s *Server, _ *session, p describeParams) (any, error) {
			switch p.Format {
			case "", "json":
				return s.task.Describe(), nil
			case "text", "dot", "mermaid":
				target := "goav:graph"
				if p.Format != "text" {
					target += "?format=" + p.Format
				}
				return graphrender.RenderTaskURI(s.task, target)
			}
			return nil, invalidParams("describe: format is json, text, dot, or mermaid")
		}),
		define("explain", "the task's plan", func(ctx context.Context, s *Server, _ *session, _ none) (any, error) {
			return s.task.Explain(ctx)
		}),
		define("snapshot", "the task's state: nodes, branches, taps, destinations", func(_ context.Context, s *Server, _ *session, _ none) (any, error) {
			return s.task.Snapshot(), nil
		}),
		define("stats", "per-node counters, drops by reason", func(_ context.Context, s *Server, _ *session, _ none) (any, error) {
			return s.task.Stats(), nil
		}),
		define("taps", "the task's attach points", func(_ context.Context, s *Server, _ *session, _ none) (any, error) {
			return s.task.Taps(), nil
		}),
		define("watch", "stream events as notifications until unwatch or disconnect", watch),
		define("unwatch", "stop a watch", func(_ context.Context, _ *Server, sess *session, p subscriptionParams) (any, error) {
			if sess == nil {
				return nil, invalidParams("unwatch: no watch on this connection")
			}
			sess.mu.Lock()
			w, ok := sess.watches[p.Subscription]
			delete(sess.watches, p.Subscription)
			sess.mu.Unlock()
			if !ok {
				return nil, invalidParams("unwatch: no watch " + strconv.Quote(p.Subscription))
			}
			w.Close()
			return struct{}{}, nil
		}),
		define("bitrate", "retarget an encoded stream's bitrate", func(ctx context.Context, s *Server, _ *session, p bitrateParams) (any, error) {
			c, err := control.SetBitrate(av.StreamID(p.Stream), p.Bitrate)
			if err != nil {
				return nil, err
			}
			return struct{}{}, s.task.Control(ctx, c)
		}),
		define("keyframe", "ask an encoded stream for a keyframe", func(ctx context.Context, s *Server, _ *session, p streamParams) (any, error) {
			return struct{}{}, s.task.Control(ctx, control.Keyframe(av.StreamID(p.Stream)))
		}),
		define("select", "switch a select to another input", func(ctx context.Context, s *Server, _ *session, p selectParams) (any, error) {
			return struct{}{}, s.task.Control(ctx, control.SelectActive(av.StreamID(p.Active)))
		}),
		define("seek", "reposition the sources", func(ctx context.Context, s *Server, _ *session, p seekParams) (any, error) {
			return struct{}{}, s.task.Control(ctx, atSource(control.Seek(p.Position), p.Source))
		}),
		define("rate", "change the task's playback rate", func(ctx context.Context, s *Server, _ *session, p rateParams) (any, error) {
			c, err := control.Rate(p.Value)
			if err != nil {
				return nil, err
			}
			return struct{}{}, s.task.Control(ctx, c)
		}),
		define("segment", "play one window of the sources", func(ctx context.Context, s *Server, _ *session, p segmentParams) (any, error) {
			c, err := control.Segment(p.Start, p.End)
			if err != nil {
				return nil, err
			}
			return struct{}{}, s.task.Control(ctx, atSource(c, p.Source))
		}),
		define("pause", "freeze the task's timeline", func(ctx context.Context, s *Server, _ *session, _ none) (any, error) {
			return struct{}{}, s.task.Pause(ctx)
		}),
		define("resume", "continue the task's timeline", func(ctx context.Context, s *Server, _ *session, _ none) (any, error) {
			return struct{}{}, s.task.Resume(ctx)
		}),
		define("attach", "add a program's outputs to the running task", func(ctx context.Context, s *Server, _ *session, p attachParams) (any, error) {
			outs, err := s.compile(p.Program)
			if err != nil {
				return nil, err
			}
			a, err := s.task.Attach(ctx, outs...)
			if err != nil {
				return nil, err
			}
			s.store(a)
			return Attached{a.Name()}, nil
		}),
		define("detach", "remove an attachment", func(ctx context.Context, s *Server, _ *session, p detachParams) (any, error) {
			a, err := s.lookup(p.Attachment)
			if err != nil {
				return nil, err
			}
			var opts []goav.DetachOption
			switch p.Outcome {
			case "", "close":
			case "drain":
				opts = append(opts, goav.DrainBranch())
			case "abort":
				opts = append(opts, goav.AbortBranch())
			default:
				return nil, invalidParams("detach: outcome is close, drain, or abort")
			}
			if err := s.task.Detach(ctx, a, opts...); err != nil {
				return nil, err
			}
			s.forget(p.Attachment)
			return struct{}{}, nil
		}),
		define("rebranch", "replace an attachment with a new program, gaplessly", func(ctx context.Context, s *Server, _ *session, p rebranchParams) (any, error) {
			a, err := s.lookup(p.Attachment)
			if err != nil {
				return nil, err
			}
			outs, err := s.compile(p.Program)
			if err != nil {
				return nil, err
			}
			args := make([]goav.RebranchArg, 0, len(outs)+2)
			for _, out := range outs {
				args = append(args, out)
			}
			switch p.Switch {
			case "", "now":
			case "next-frame":
				args = append(args, goav.SwitchAt(goav.NextFrame()))
			case "next-keyframe":
				args = append(args, goav.SwitchAt(goav.NextKeyframe()))
			default:
				at, err := time.ParseDuration(p.Switch)
				if err != nil {
					return nil, invalidParams("rebranch: switch is now, next-frame, next-keyframe, or a media position like 2s")
				}
				args = append(args, goav.SwitchAt(goav.AtMediaTime(at)))
			}
			switch p.Old {
			case "", "close":
			case "drain":
				args = append(args, goav.DrainBranch())
			case "abort":
				args = append(args, goav.AbortBranch())
			default:
				return nil, invalidParams("rebranch: old is close, drain, or abort")
			}
			next, err := a.Rebranch(ctx, args...)
			if err != nil {
				return nil, err
			}
			s.forget(p.Attachment)
			s.store(next)
			return Attached{next.Name()}, nil
		}),
		define("stop", "close the task", func(_ context.Context, s *Server, _ *session, _ none) (any, error) {
			return struct{}{}, s.task.Close()
		}),
		define("capabilities", "this server's methods and the elements attached programs may use", func(_ context.Context, s *Server, _ *session, _ none) (any, error) {
			caps := Capabilities{Protocol: protocolVersion, Elements: launch.Elements(launch.Use(s.elements...))}
			for _, name := range s.methodNames() {
				m := s.methods[name]
				caps.Methods = append(caps.Methods, MethodInfo{Name: m.name, Summary: m.summary, Params: m.schema.params()})
			}
			return caps, nil
		}),
	}
}

func watch(_ context.Context, s *Server, sess *session, p watchParams) (any, error) {
	if sess == nil {
		return nil, invalidParams("watch streams on a connection; over HTTP, GET /events")
	}
	var filters []goav.EventFilter
	if len(p.Types) > 0 {
		types := make([]av.EventType, len(p.Types))
		for i, t := range p.Types {
			types[i] = av.EventType(t)
		}
		filters = append(filters, goav.WatchTypes(types...))
	}
	if p.Stream != "" {
		filters = append(filters, goav.WatchStream(av.StreamID(p.Stream)))
	}
	sub := s.task.Watch(filters...)
	id := "w" + strconv.FormatUint(s.nextWatch.Add(1), 10)
	sess.mu.Lock()
	if sess.watches == nil {
		sess.mu.Unlock()
		sub.Close()
		return nil, errors.New("watch: the connection closed")
	}
	sess.watches[id] = sub
	sess.mu.Unlock()
	go func() {
		for event := range sub.Events() {
			sess.send(notification{JSONRPC: "2.0", Method: "event", Params: eventParams{Subscription: id, Event: wireEvent(event)}})
		}
	}()
	return struct {
		Subscription string `json:"subscription"`
	}{id}, nil
}

func atSource(c control.Control, source string) control.Control {
	if source == "" {
		return c
	}
	return c.At(pipeline.NodeRef(source))
}

func (s *Server) compile(program string) ([]goav.Output, error) {
	opts := []launch.Option{launch.Task(s.task), launch.Use(s.elements...)}
	if s.files != nil {
		opts = append(opts, launch.Files(s.files))
	}
	return launch.Compile(program, opts...)
}

func (s *Server) store(a goav.Attachment) {
	s.mu.Lock()
	s.attachments[a.Name()] = a
	s.mu.Unlock()
}

func (s *Server) forget(name string) {
	s.mu.Lock()
	delete(s.attachments, name)
	s.mu.Unlock()
}

func (s *Server) lookup(name string) (goav.Attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.attachments[name]; ok {
		return a, nil
	}
	names := make([]string, 0, len(s.attachments))
	for n := range s.attachments {
		names = append(names, n)
	}
	sort.Strings(names)
	msg := fmt.Sprintf("no attachment %q", name)
	if hint := spelling.Nearest(name, names); hint != "" {
		msg += fmt.Sprintf("; did you mean %q?", hint)
	} else if len(names) > 0 {
		msg += "; attachments: " + fmt.Sprint(names)
	}
	return nil, invalidParams(msg)
}
