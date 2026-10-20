// Package kit is how a codec in a shared library or OS framework becomes a
// goav backend, from any module, with no cgo: a Codec declares the backend
// (its identity, capabilities, knobs, libraries, license, and releases) and
// a session per mode; Register adds it to a runtime.
//
//	type symbols struct {
//		compress func(dst *byte, dstLen *uint64, src *byte, srcLen uint64, level int32) int32 `sym:"compress2"`
//	}
//
//	sym := &symbols{}
//	option := kit.Register(kit.Codec{
//		ID: "zlib", Media: av.MediaVideo, Backend: "zlib", Kind: codec.KindLib,
//		Knobs:     []codec.Knob{{Name: "level", Type: codec.KnobInt, Min: 0, Max: 9, Default: "6"}},
//		Libraries: []kit.Library{{Name: "libz", System: []string{"/usr/lib/libz.1.dylib"}, Fix: "install zlib", Symbols: sym}},
//		License:   kit.License{Class: kit.LicensePermissive},
//		NewVideoEncoder: newEncoder, // Encode(*kit.Picture, emit), Flush, Close
//		NewVideoDecoder: newDecoder, // Decode(*kit.Packet, emit), Flush, Close
//	})
//
// The kit binds the symbols when the runtime is built, each library file
// once per process, and owns everything between goav and the sessions:
// I420 and NV12 pictures, Annex B and AVCC framing, parameter sets,
// timestamps and reordering, pooled payloads, knob checking, live controls,
// damaged units (a session returns Damaged and decoding goes on), and Close.
// A backend takes only the settings it declares; read them with the sized
// accessors (Int32, Uint32, Uint16) for C fields. Params exposes a C
// parameter struct as raw parameters, Picture.Owner lends decoded memory
// without a copy, and libcodec/codectest.Run is the conformance suite.
// github.com/thesyncim/goav/libcodec/proof/extbackend is a complete backend.
//
// Video decoder transitions have distinct semantics. EOS/FlushInto drains
// valid delayed pictures in presentation order. Discontinuity, including a
// discontinuous packet, discards pending pictures, timing and backend reference
// state and resumes from a keyframe. CodecChanged also discards old output
// before the event is forwarded, adopts the new stream identity, epoch,
// timebase, geometry and parameter sets, and opens a fresh session. No
// pre-change picture can appear after the new configuration event.
//
// Audio decoder transitions use the same drain/discard boundary: EOS drains
// delayed samples, while discontinuities discard them by opening a fresh
// session. CodecChanged adopts stream identity, epoch, rate, channels, layout
// and extradata. Backend packets use the sample clock even when input packets
// use another timebase. Unsupported codec switches or backend configuration
// refusals return an error before committing the replacement configuration.
package kit
