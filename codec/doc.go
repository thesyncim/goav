// Package codec defines the decoder and encoder contracts: descriptors that
// declare what an implementation can do, factories that open it, and the
// caller-owned result buffers that keep decode/encode allocation-free. It
// also carries the codec spec grammar recipes use — codec.Opus(...),
// codec.VP9(codec.Bitrate(...)), codec.Control(fn) — and the per-runtime
// registry external codec adapters plug into.
package codec
