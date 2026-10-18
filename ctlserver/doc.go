// Package ctlserver is goav's control plane: a running task's capabilities, served
// to clients over a socket or HTTP. A host serves its task; an operator, a
// dashboard, or goav ctl connects and inspects it, attaches and detaches
// outputs written in the pipeline language, and drives its controls.
//
//	server, err := ctlserver.New(task, ctlserver.Files(dir))
//	go server.ListenAndServe(ctx, "unix:///run/goav.sock")
//
//	client, err := ctlserver.Dial(ctx, "unix:///run/goav.sock")
//	stats, err := client.Call[pipeline.GraphStats](ctx, "stats", nil)
//	_, err = client.Call[ctlserver.Attached](ctx, "attach", map[string]string{
//		"program": "tap live ! resize 640x360 ! encode vp8 ! as preview ! file preview.webm",
//	})
//
// # Protocol
//
// JSON-RPC 2.0 over one connection (unix:///path or tcp://host:port),
// one JSON value per message: requests carry ids and run concurrently (past
// 32 in flight the server stops reading, backpressuring the client), a batch
// array runs its requests in order, a {"method":"$/cancelRequest","params":
// {"id":7}} notification cancels call 7, and a watch pushes
// {"method":"event"} notifications on the same connection. Server is also an
// http.Handler: POST a request, GET .../events for server-sent events.
// capabilities reports the protocol version, which changes only when the
// wire breaks. A refusal is an Error whose Data carries goav's
// structured detail: a BuildError's code, node, details, and fixes, or a
// program's line, column, and hint.
//
// # Methods
//
// Inspect: describe [format=json|text|dot|mermaid], explain, snapshot,
// stats, taps, capabilities. Watch: watch [types= stream=], unwatch
// subscription=. Controls: bitrate stream= bitrate=, keyframe stream=,
// select active=, seek position= [source=], rate value=, segment start= end=
// [source=], pause, resume. Change the graph: attach program=, rebranch
// attachment= program= [switch=now|next-frame|next-keyframe|<position>]
// [old=close|drain|abort], detach attachment= [outcome=close|drain|abort].
// Stop: stop. Method adds a host's own, typed. detach and rebranch address
// the attachments made through the server; the host's own stay the host's.
//
// An attached program starts at a tap (tap live ! ...) and may use the
// elements Elements adds; it reads and writes files only through the
// FileSystem Files grants, so a server for untrusted clients grants a
// launch.Dir or none.
//
// # Cost
//
// Serving costs the task's data plane nothing: calls run on connection
// goroutines, stats are atomic reads, and a watch is an inspect subscription
// that sheds events when its client falls behind instead of blocking media.
package ctlserver
