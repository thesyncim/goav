// Package launch is goav's pipeline language: a goav recipe written as text,
// for command lines, configuration files, and control sockets.
//
//	in := file in.webm
//	v  := in ! video ! decode
//	v ! resize 1280x720 ! encode vp9 bitrate=2M ! as hd ! file hd.webm
//	v ! resize 640x360  ! encode vp9 bitrate=800k ! as sd ! file sd.webm
//	in ! audio ! decode ! encode opus ! file hd.webm, file sd.webm
//
// Compile turns a program into goav Outputs — the values the same recipe
// written in Go produces, so a program costs nothing at run time and runs,
// describes, and explains like any recipe:
//
//	outs, err := launch.Compile(program, launch.Files(launch.OS()))
//	err = rt.Run(ctx, outs...)
//
// # Programs
//
// A program is statements, one per line or separated by ;, with # comments.
// A statement is a pipeline: a first step, then operations joined by !, then
// one or more destinations separated by commas. name := pipeline binds a
// stream (or an input) to reuse; using a binding twice forks it, and the work
// before the fork runs once. A line that starts with ! or , continues the
// statement above it.
//
// A step is an element with settings: positional values first, then
// key=value. Values are words or quoted text; numbers take k, M, and G
// suffixes (96k, 2M), durations are Go durations (20ms, 2s), sizes are WxH.
//
// file names a file: at a pipeline's start it is an input, at its end a
// destination. Every use of one path is one input, or one mux: two pipelines
// that end at file call.webm write one file. Files are opened and created
// only when the recipe runs, and only through the FileSystem given to Files.
//
// uri names a URL the same way, and a quoted URL alone is one:
// "https://cdn.example/in.mp4" ! video ! .... The runtime's schemes resolve it
// (goav.URIInput), and only for the schemes URIs grants; a file URL is a
// file, opened through Files.
//
// # Elements
//
// Sources: file PATH, uri URL, testsrc video|audio [size= fps= pattern=
// rate= channels= tone= frames= duration=] [live], and tap NAME (on a
// running task, see Task).
//
// Operations: video, audio, and stream [id= index=] select an input's
// stream; decode; encode CODEC [bitrate= fps= keyframe_interval= profile=
// level= channels= sample_rate= clock_rate= media=] with native adapter
// settings passed through; resize WxH [mode=exact|fit|fill]; resample RATE
// CHANNELS; tap NAME; as NAME; buffer block|drop-oldest|drop-newest N,
// buffer latest|unbounded [max_bytes= max_delay=]; sync NAME [tolerance=]
// [drop_late] (one timeline per name); playout [NAME]; auto [resample]
// [resize] [convert]; at X Y (a composite input's position).
//
// Joins: mix(a, b, ...), composite(a, b ! at X Y, ...), select(a, b, ...).
// Each input is a pipeline yielding a stream. Mix and composite take
// sync=arrival|pts (sync=pts is goav.ByPTS()): mix(a, b, sync=pts).
//
// Destinations: file PATH [format=], uri URL [format=], null.
//
// Use adds a host's own sources, stages, encoders, sinks, and joins, with
// typed settings; Elements lists the vocabulary for help and discovery.
//
// # Refusals
//
// A program that cannot compile returns a *Error naming the problem, its line
// and column, and the fix; Error() underlines the offending text:
//
//	launch: 2:14: unknown element "resise"
//	    in ! video ! resise 1280x720 ! file out.webm
//	                 ^^^^^^
//	    did you mean "resize"?
package launch
