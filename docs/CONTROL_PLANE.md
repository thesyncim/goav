# Control plane

Package `ctlserver` serves a running task to clients outside the process: an
operator at a shell, a dashboard, a supervisor, or a test. A client inspects
the task, watches its events, drives its controls, and attaches, rebranches,
and detaches outputs written in the [pipeline language](CLI.md), all while
media flows. `goav ctl` is its command-line client.

Use it when something outside your code must steer a live task: add a
preview or a recording on demand, retarget a bitrate, seek, watch QoS, or stop
cleanly. Keep media logic in Go: a stage, encoder, or sink your clients need
is a host element (below), not a script. Everything the control plane does is
a `LiveTask` call your Go code could make; it adds a wire, not a second API.

## Serving a task

```go
files, err := launch.Dir("/srv/recordings") // attached programs may touch nothing else
if err != nil {
	return err
}
server, err := ctlserver.New(task, ctlserver.Files(files))
if err != nil {
	return err
}
go func() { _ = task.Run(ctx) }()
return server.ListenAndServe(ctx, "unix:///run/goav/ctl.sock")
```

`ListenAndServe` takes `unix:///path` or `tcp://host:port` and serves until
ctx ends, then waits for calls in flight to answer. A unix socket is
owner-only from the moment it exists (on Windows, it takes its directory's
ACL); a stale socket is replaced, and a path another server is serving, or one
that is not a socket, is refused rather than deleted. TCP needs
`ctlserver.Token(t)`: every connection first sends
`{"method":"$/authenticate","params":{"token":t}}` (`Dial` does, from
`tcp://TOKEN@host:port`) or is answered `-32001` and closed; an HTTP request
needs `Authorization: Bearer t`. `Serve(ctx, listener)` takes a listener you made.

A `*Server` is an `http.Handler`, for mounting in a service you already run,
behind its own authentication:

```go
mux := http.NewServeMux()
mux.Handle("/goav/", requireOperator(server)) // your auth middleware
```

POST a JSON-RPC request to any path under the mount and the response comes
back as the body; GET a path ending in `/events` for server-sent events,
filtered by `?types=a,b` and `?stream=id`. `watch` needs a connection, so over
HTTP it is refused in favor of `/events`.

## Methods

| Method | Params | Does |
| --- | --- | --- |
| `describe` | `[format=json\|text\|dot\|mermaid]` | the task's graph (`pipeline.Spec` as JSON, or rendered text) |
| `explain` | | the task's plan (`plan.Report`) |
| `snapshot` | | nodes, branches, taps, destinations (`goav.TaskSnapshot`) |
| `stats` | | per-node counters and drops by reason (`pipeline.GraphStats`) |
| `taps` | | the attach points (`[]goav.TapSnapshot`) |
| `capabilities` | | protocol version, every method with its params, every element |
| `watch` | `[types=a,b] [stream=]` | events as notifications; returns `{"subscription": "w1"}` |
| `unwatch` | `subscription=` | stop a watch |
| `bitrate` | `stream= bitrate=` | retarget the encoder of an encoded stream (its `as` name) |
| `keyframe` | `stream=` | ask an encoded stream for a keyframe |
| `select` | `active=` | switch a select to another input |
| `seek` | `position= [source=]` | reposition the sources, or one source node |
| `rate` | `value=` | scale the task's timeline |
| `segment` | `start= end= [source=]` | play one window of the sources |
| `pause`, `resume` | | freeze and continue the task's timeline |
| `attach` | `program=` | add a program's outputs; returns `{"attachment": NAME}` |
| `rebranch` | `attachment= program= [switch=] [old=]` | replace an attachment gaplessly; returns the new name |
| `detach` | `attachment= [outcome=close\|drain\|abort]` | remove an attachment and finalize its destinations |
| `stop` | | close the task |

`switch` is `now` (default), `next-frame`, `next-keyframe`, or a media
position such as `2s`; `old` is `close` (default), `drain`, or `abort` for the
replaced destinations. A param value is JSON of its type or a string in the
CLI's spelling: `"bitrate": 1000000` and `"bitrate": "1M"` are the same, as
are `"types": ["qos"]` and `"types": "qos"`; durations are Go durations.
`capabilities` lists each param's type, whether it is required, and its help.
`bitrate` and `keyframe` travel the data path from the source, so one
addressed to a stream nothing encodes is accepted and has no effect.

## The wire

JSON-RPC 2.0, one JSON value per message, on one connection. Requests carry
ids and run concurrently; past 32 in flight on one connection the server stops
reading it, which backpressures the client. A request without an id is a
notification and gets no answer. A batch, a JSON array of requests, runs them
in order and answers every one but its notifications. The notification
`{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":7}}` cancels call
7; the Go client sends it when a `Call`'s ctx ends. `capabilities` returns
`"protocol": 1`, which changes only when the wire breaks. A call and its
answer:

```json
{"jsonrpc":"2.0","id":4,"method":"watch","params":{"types":["discontinuity"]}}
{"jsonrpc":"2.0","id":4,"result":{"subscription":"w1"}}
```

A refusal is an error with a JSON-RPC code: `-32700` malformed JSON,
`-32600` not a request, `-32601` unknown method, `-32602` bad params, and
`-32000` for a call goav refused, whose `data` carries the structured detail,
and `-32001` for a client that did not authenticate with the server's Token.
A request too broken to read is answered with `"id": null`. A program
refusal carries its position and the fix, and, when an element refused with a
`*goav.BuildError`, that error under `build`:

```json
{"jsonrpc":"2.0","id":2,"error":{"code":-32000,
  "message":"launch: 1:12: unknown element \"resise\"\n    tap live ! resise 320x180 ! null\n               ^^^^^^\n    did you mean \"resize\"?",
  "data":{"kind":"program","line":1,"column":12,"message":"unknown element \"resise\"","hint":"did you mean \"resize\"?"}}}
```

A build refusal carries the `*goav.BuildError`: `phase`, `family`, `code`,
`operation`, `node`, `reason`, `details`, and `fixes`, as in
[`ERRORS.md`](ERRORS.md). Bad params name the param and, for a near miss,
the spelling meant:

```json
{"jsonrpc":"2.0","id":3,"error":{"code":-32602,"message":"bitrate: missing param \"bitrate\" (integer)"}}
```

A watch pushes its events as notifications on the same connection. An event
carries its type, stream, epoch, time, media position, reason, and metadata;
`stream_added` and `codec_changed` also carry the `track`: its index, type,
codec, profile, size, sample rate, channels, language, and name.

```json
{"jsonrpc":"2.0","method":"event","params":{"subscription":"w1","event":{"type":"stream_added","stream":"cam",
  "at":"2026-10-18T15:20:01.5Z","track":{"index":0,"type":"video","codec":"vp8","width":1280,"height":720}}}}
```

## Attaching programs

An attached program starts at one of the task's taps and ends at
destinations, in the same language `goav run` speaks:

```text
tap live ! resize 640x360 ! encode vp8 bitrate=800k ! as preview ! file preview.webm
```

One `attach` applies a program's outputs atomically; a refusal leaves the
task as it was. The answer names the attachment, after its `as` name or else
its first destination; `rebranch` and `detach` take that name, and `detach`
finalizes the attachment's files. They address only attachments made through
the server: the host's own attachments stay the host's.

Files are the server's policy, not the client's. Without `Files`, an attached
program reads and writes nothing, and a `file` destination is refused.
`Files` given a `launch.Dir(root)` confines programs to root: a path that
escapes it, through `..` or a symlink, is refused. `launch.OS()` grants the
process's files. `goav run --control` grants `--control-dir` (default `.`)
as a `launch.Dir`. Serve untrusted clients a `launch.Dir`, or nothing.

## Host methods and elements

A host extends the server two ways, both typed, both listed by
`capabilities`. `ctlserver.Method[T]` adds a method whose params are T's
fields: `json` names them, `ctl:"required"` requires one, and `help`
describes it. Param types are strings, bools, integers, floats,
`time.Duration`, and `[]string`. The result is any JSON value, and an error
is a refusal.

```go
type cueParams struct {
	Name   string        `json:"name" ctl:"required" help:"intro, chorus, or outro"`
	Offset time.Duration `json:"offset" help:"how far past the cue: 500ms"`
}
cue := ctlserver.Method("cue", "seek the player to a named cue",
	func(ctx context.Context, task goav.LiveTask, p cueParams) (any, error) {
		at, ok := cues[p.Name]
		if !ok {
			return nil, fmt.Errorf("no cue %q", p.Name)
		}
		return struct{}{}, task.Control(ctx, control.Seek(at+p.Offset).At(pipeline.NodeRef("player")))
	})
```

`ctlserver.Elements` adds pipeline elements for attached programs to use:
`launch.Source`, `launch.Stage`, `launch.Encoder`, `launch.Sink`, and
`launch.Join`, typed by `launch:"..."` settings. A constructor's error refuses
the program at attach, pointing at the element:

```go
type thumbnailArgs struct {
	Every int    `launch:"every,required" help:"keep one frame in every N"`
	Name  string `launch:"name"`
}
type acmeArgs struct {
	Bitrate int `launch:"bitrate,required"`
}
thumbnail := launch.Sink("thumbnail", "keep one frame in every N",
	func(a thumbnailArgs) (goav.Destination, error) {
		if a.Every < 1 {
			return goav.Destination{}, fmt.Errorf("every=%d keeps no frames; write every=1 or more", a.Every)
		}
		return goav.Sink(newThumbnailer(a.Name, a.Every)), nil
	})
acme := launch.Encoder("acme", "encode with the host's ACME codec",
	func(a acmeArgs) (codec.Spec, error) {
		return codec.Codec("x_acme_video", av.MediaVideo, codec.Bitrate(a.Bitrate)), nil
	})
server, err := ctlserver.New(task, ctlserver.Elements(thumbnail, acme), cue)
```

A client then attaches `tap live ! thumbnail every=30` or
`tap live ! acme bitrate=250k ! as acme ! null`. A method name is one word and
unique: a host method cannot shadow a built-in. The runnable host is
[`examples/control-plane-host`](../examples/control-plane-host): a typed
`cue` method, a control-history method, a meter stage, thumbnail and memory
sinks, and a custom encoder whose native settings arrive through
`codec.Control`.

## The Go client

```go
client, err := ctlserver.Dial(ctx, "unix:///run/goav/ctl.sock")
if err != nil {
	return err
}
defer client.Close()

stats, err := client.Call[pipeline.GraphStats](ctx, "stats", nil)
att, err := client.Call[ctlserver.Attached](ctx, "attach", map[string]string{
	"program": "tap live ! encode vp8 ! as preview ! file preview.webm",
})
var refused *ctlserver.Error
if errors.As(err, &refused) {
	fmt.Println(refused.Code, refused.Message, string(refused.Data))
}

events, err := client.Watch(ctx, map[string][]string{"types": {"qos", "stream_added"}})
for e := range events {
	fmt.Println(e.Type, e.Stream, e.Reason)
}
```

`Call[T]` decodes the result into T (`json.RawMessage` keeps it raw); calls
on one client may run concurrently, and one whose ctx ends is cancelled on
the server too. `Watch` streams `ctlserver.Event` values until ctx ends or
the connection closes; a reader that falls behind loses events, never the
task's media.

## goav ctl

```text
goav ctl --control unix:///run/goav/ctl.sock stats
goav ctl --control unix:///run/goav/ctl.sock attach 'tap live ! encode vp8 ! as preview ! file p.webm'
goav ctl --control unix:///run/goav/ctl.sock bitrate stream=preview bitrate=500k
goav ctl --control unix:///run/goav/ctl.sock watch types=qos
```

`goav ctl <method> key=value...` calls any method, the host's included;
`attach`, `rebranch`, and `detach` take their attachment and program
positionally. [`CLI.md`](CLI.md#control) has the details, and `goav run
--control` serves any program with no Go at all.

## Cost

Serving costs the task's data plane nothing. Calls run on connection
goroutines, `stats` reads atomic counters, and a watch is a `goav.Subscription`
that sheds events when its client falls behind rather than block
media. `TestServingCostsTheDataPlaneNothing` pins it: a task allocates the
same per frame with a server, a connected client, and a watch as with none.
