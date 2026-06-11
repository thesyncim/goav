// Package format defines the container contracts: probing detects what bytes
// are (format and streams) before anything opens, demuxers read packets out
// of containers, and muxers write them back in. The per-runtime registry maps
// format ids to adapters; demuxers that implement Seeker make file inputs
// honour the Seek/Segment time controls.
package format
