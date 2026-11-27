// Package goav is a pure-Go realtime media grammar: describe media work as
// values, then run it with an explicit runtime.
//
// The grammar is small. An input's Video, Audio, or Stream method selects a
// Stream; operations are methods that return a new Stream (.Decode(),
// .Resize(), .Encode(codec.VP9(...))), so a Stream used twice fans out and the
// work before the fork runs once. Mix, Composite, Select, and Join converge
// Streams into one, Tap names attach points, and .To(destinations...) routes a
// Stream to destinations built by Write, ToURI, WriteFunc, Custom, or Sink, making
// an Output. A runtime runs Outputs together: rt.Run(ctx, outputs...), or
// rt.Build for a Task with events, snapshots, runtime Attach/Detach, and
// live controls.
//
// New(opts...) builds a bare runtime and returns option/configuration errors.
// Import github.com/thesyncim/goav/bundle when the application wants the
// bundled pure-Go adapters. Build errors are structured: every refusal is a
// *BuildError carrying an errcode.Family, detailed errcode.Code, the failing
// operation, and concrete fixes.
package goav
