package goav

import (
	annexbadapter "github.com/thesyncim/goav/adapters/annexb"
	goav1adapter "github.com/thesyncim/goav/adapters/goav1"
	goh264adapter "github.com/thesyncim/goav/adapters/goh264"
	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	govpxadapter "github.com/thesyncim/goav/adapters/govpx"
	ivfadapter "github.com/thesyncim/goav/adapters/ivf"
	resampleadapter "github.com/thesyncim/goav/adapters/resample"
	resizeadapter "github.com/thesyncim/goav/adapters/resize"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	webmadapter "github.com/thesyncim/goav/container/webm"
)

func WithDefaults() Option {
	return func(runtime *runtime) {
		WithStdFormats()(runtime)
		WithStdCodecs()(runtime)
		WithStdFilters()(runtime)
	}
}

func WithStdFormats() Option {
	return func(runtime *runtime) {
		ivfadapter.Register(runtime.formats)
		annexbadapter.Register(runtime.formats)
		matroskaadapter.Register(runtime.formats)
		webmadapter.Register(runtime.formats)
	}
}

func WithStdCodecs() Option {
	return func(runtime *runtime) {
		gopusadapter.Register(runtime.codecs)
		govpxadapter.Register(runtime.codecs)
		goav1adapter.Register(runtime.codecs)
		goh264adapter.Register(runtime.codecs)
	}
}

func WithStdFilters() Option {
	return func(runtime *runtime) {
		resampleadapter.Register(runtime.filters)
		resizeadapter.Register(runtime.filters)
	}
}

// Default builds a runtime with the standard codecs, formats, and filters already
// registered, then applies opts on top. Because the defaults are applied first
// and registration is last-wins, opts can both ADD new implementations and
// OVERRIDE a default in the same call — solid batteries you can still change:
//
//	rt := goav.Default()                                   // stock everything
//	rt := goav.Default(goav.WithDemuxer("flv", myFLV))     // defaults + a new format
//	rt := goav.Default(goav.WithEncoder(vp9Desc, myVP9))   // defaults, but my VP9 wins
//
// Use New(opts...) instead to start bare and register only what you list.
func Default(opts ...Option) Runtime {
	return New(append([]Option{WithDefaults()}, opts...)...)
}
