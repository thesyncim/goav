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
