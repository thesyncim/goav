# CLI

`goav` runs, describes, and explains programs in goav's pipeline language, and
`goav ctl` steers a task an application serves over a control socket. A
program is the same recipe the Go library builds, written as text: the CLI and
the library produce the same graph.

```text
goav run      [--runtime default|demo|test] [--control unix://PATH] [-f FILE] '<program>'
goav describe [--runtime ...] [--format text|dot|mermaid|json] [-f FILE] '<program>'
goav explain  [--runtime ...] [-f FILE] '<program>'
goav elements
goav ctl      --control unix://PATH <method> [key=value...]
```

Install it with `GOEXPERIMENT=simd go install github.com/thesyncim/goav/cmd/goav@latest`;
the SIMD codec paths run about twice as fast.

## The language

A program is pipelines, one per line or separated by `;`, with `#` comments.
A pipeline starts with an input, joins operations with `!`, and ends at one or
more destinations separated by commas. `name := ...` binds a stream to reuse;
using it twice forks it, and the work before the fork runs once.

```text
in := file in.webm
v  := in ! video ! decode
v ! resize 1280x720 ! encode vp9 bitrate=2M ! as hd ! file hd.webm
v ! resize 640x360  ! encode vp9 bitrate=800k ! as sd ! file sd.webm
in ! audio ! decode ! encode opus ! file hd.webm, file sd.webm
```

`file PATH` is an input at a pipeline's start and a destination at its end;
every use of one path is one input, or one mux. Settings are positional values,
then `key=value`: numbers take `k`, `M`, and `G` (`bitrate=2M`), durations are Go
durations (`duration=2s`), sizes are `WxH`. A line starting with `!` continues
the one above.

| Kind | Elements |
| --- | --- |
| Sources | `file PATH`, `testsrc video\|audio [size= fps= pattern= rate= channels= tone= frames= duration=] [live]` |
| Selection | `video`, `audio`, `stream` `[id= index=]` |
| Operations | `decode`, `encode CODEC [bitrate= fps= keyframe_interval= ...]`, `resize WxH [mode=]`, `resample RATE CHANNELS`, `tap NAME`, `as NAME`, `buffer MODE [N]`, `sync NAME`, `playout`, `auto`, `syncbypts` |
| Joins | `mix(a, b)`, `composite(a, b ! at X Y)`, `select(a, b)` |
| Destinations | `file PATH [format=]`, `hls PATH [segment= window=]`, `null` |

`goav elements` lists every element with its settings. Native adapter settings
pass through `encode` (`encode av1 tune=zerolatency`). A mistake is refused
with its line and column, the offending text underlined, and the fix:

```text
launch: 1:26: unknown element "decod"
    file clip.webm ! video ! decod ! file x.webm
                             ^^^^^
    did you mean "decode"?
```

Applications compile programs with [`launch.Compile`](../launch), add their
own elements with `launch.Use`, and choose which files a program may touch with
`launch.Files`.

## Running

`goav run` builds the program and runs it until its inputs end (Ctrl-C stops a
live program), then prints a JSON summary of the files it wrote:

```text
goav run 'testsrc video size=1280x720 duration=2s ! encode vp9 ! file clip.webm
          testsrc audio duration=2s ! encode opus ! file clip.webm'
{"runtime":"default","wrote":["clip.webm"]}
```

Runtimes: `default` is the bundled adapters; `demo` adds a deterministic clock,
so `live` test sources run as fast as the graph allows; `test` is the
`goavtest` runtime with fake codecs. `goav describe` prints the graph a program
builds and `goav explain` its plan, without opening or creating any file.

## Control

With `--control unix://PATH` (or `tcp://host:port`), `goav run` serves the
running task through [`ctlserver`](CONTROL_PLANE.md) until it stops, and
`goav ctl` calls its methods, one per invocation:

```text
goav run --control unix:///tmp/goav.sock 'testsrc video live ! tap live ! encode vp8 ! as cam ! file live.webm'
goav ctl --control unix:///tmp/goav.sock seek position=2s source=video
goav ctl --control unix:///tmp/goav.sock attach 'tap live ! resize 320x180 ! encode vp8 ! as preview ! file p.webm'
goav ctl --control unix:///tmp/goav.sock detach preview outcome=drain
goav ctl --control unix:///tmp/goav.sock stop
```

Params are `key=value`, in the CLI's spelling (`bitrate=1M`, `position=2s`,
`types=a,b`); `attach` takes its program, `detach` its attachment, and
`rebranch` both, positionally. A JSON result prints indented, a text result
(`describe format=text`) prints as is, and `watch` prints events as JSON lines
until Ctrl-C; a refusal prints to stderr and exits 1. `capabilities` lists a
server's methods, the host's own included, and its elements. Use `goav ctl`
for inspection, controls, and attach. Hot-path media logic belongs in
the Go library.
