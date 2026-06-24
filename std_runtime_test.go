package goav

import (
	annexbadapter "github.com/thesyncim/goav/adapters/annexb"
	goaacadapter "github.com/thesyncim/goav/adapters/goaac"
	goav1adapter "github.com/thesyncim/goav/adapters/goav1"
	goh264adapter "github.com/thesyncim/goav/adapters/goh264"
	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	govpxadapter "github.com/thesyncim/goav/adapters/govpx"
	ivfadapter "github.com/thesyncim/goav/adapters/ivf"
	resampleadapter "github.com/thesyncim/goav/adapters/resample"
	resizeadapter "github.com/thesyncim/goav/adapters/resize"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	mp4adapter "github.com/thesyncim/goav/container/mp4"
	webmadapter "github.com/thesyncim/goav/container/webm"
)

func testStdRuntime(opts ...Option) Runtime {
	return New(append(testStdOptions(), opts...)...)
}

func testStdOptions() []Option {
	options := make([]Option, 0, 3)
	options = append(options, testStdFormats(), testStdCodecs(), testStdFilters())
	return options
}

func testStdFormats() Option {
	return func(runtime *runtime) {
		ivfadapter.Register(runtime.formats)
		annexbadapter.Register(runtime.formats)
		matroskaadapter.Register(runtime.formats)
		webmadapter.Register(runtime.formats)
		mp4adapter.Register(runtime.formats)
	}
}

func testStdCodecs() Option {
	return func(runtime *runtime) {
		gopusadapter.Register(runtime.codecs)
		goaacadapter.Register(runtime.codecs)
		govpxadapter.Register(runtime.codecs)
		goav1adapter.Register(runtime.codecs)
		goh264adapter.Register(runtime.codecs)
	}
}

func testStdFilters() Option {
	return func(runtime *runtime) {
		resampleadapter.Register(runtime.filters)
		resizeadapter.Register(runtime.filters)
	}
}
