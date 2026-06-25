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

func testBundleRuntime(opts ...Option) *Runtime {
	return MustRuntime(append(testBundleOptions(), opts...)...)
}

func testBundleOptions() []Option {
	options := make([]Option, 0, 3)
	options = append(options, testBundleFormats(), testBundleCodecs(), testBundleFilters())
	return options
}

func testBundleFormats() Option {
	return func(config *Config) error {
		ivfadapter.Register(config.Formats)
		annexbadapter.Register(config.Formats)
		matroskaadapter.Register(config.Formats)
		webmadapter.Register(config.Formats)
		mp4adapter.Register(config.Formats)
		return nil
	}
}

func testBundleCodecs() Option {
	return func(config *Config) error {
		gopusadapter.Register(config.Codecs)
		goaacadapter.Register(config.Codecs)
		govpxadapter.Register(config.Codecs)
		goav1adapter.Register(config.Codecs)
		goh264adapter.Register(config.Codecs)
		return nil
	}
}

func testBundleFilters() Option {
	return func(config *Config) error {
		resampleadapter.Register(config.Filters)
		resizeadapter.Register(config.Filters)
		return nil
	}
}
