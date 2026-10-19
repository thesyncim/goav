// Package bundle provides the bundled pure-Go runtime adapters for goav recipes.
//
// Import this package when an application wants the bundled codecs, container
// formats, and frame filters. The root package stays focused on the recipe
// grammar and runtime extension seams.
package bundle

import (
	"context"
	"sync"

	"github.com/thesyncim/goav"
	annexbadapter "github.com/thesyncim/goav/adapters/annexb"
	goaacadapter "github.com/thesyncim/goav/adapters/goaac"
	goav1adapter "github.com/thesyncim/goav/adapters/goav1"
	goh264adapter "github.com/thesyncim/goav/adapters/goh264"
	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	govpxadapter "github.com/thesyncim/goav/adapters/govpx"
	ivfadapter "github.com/thesyncim/goav/adapters/ivf"
	resampleadapter "github.com/thesyncim/goav/adapters/resample"
	resizeadapter "github.com/thesyncim/goav/adapters/resize"
	flvadapter "github.com/thesyncim/goav/container/flv"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	mp4adapter "github.com/thesyncim/goav/container/mp4"
	webmadapter "github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/scheme"
)

// New builds a runtime with the bundled formats, codecs, filters, and URL
// schemes already registered, then applies opts on top. Registration is
// last-wins, so opts can add or override bundled implementations.
func New(opts ...runconfig.Option) (*goav.Runtime, error) {
	return goav.New(appendOptions(Options(), opts...)...)
}

// MustNew is New for package-level setup and tests.
func MustNew(opts ...runconfig.Option) *goav.Runtime {
	return mustRuntime(New(opts...))
}

func mustRuntime(runtime *goav.Runtime, err error) *goav.Runtime {
	if err != nil {
		panic(err)
	}
	return runtime
}

// Default is the process-wide bundled runtime: built with default options on
// first use, then shared by every caller in the process for the life of the
// process, and used by this package's Run, Build, Describe, and Explain. It is
// safe to share because a runtime holds no per-run state. An application that wants its own clock,
// buffer policy, or adapters builds a runtime with New instead; nothing
// changes Default.
func Default() *goav.Runtime {
	return defaultRuntime()
}

var defaultRuntime = sync.OnceValue(func() *goav.Runtime { return MustNew() })

// Run builds outs with the default bundled runtime and runs them to
// completion: bundle.Run(ctx, in.Video().Encode(codec.VP9()).To(out)).
func Run(ctx context.Context, outs ...goav.Output) error {
	return Default().Run(ctx, outs...)
}

// Build builds outs with the default bundled runtime without starting them.
func Build(ctx context.Context, outs ...goav.Output) (goav.LiveTask, error) {
	return Default().Build(ctx, outs...)
}

// Describe returns the graph outs build with the default bundled runtime,
// without opening anything.
func Describe(outs ...goav.Output) (pipeline.Spec, error) {
	return Default().Describe(outs...)
}

// Explain reports the plan outs build with the default bundled runtime,
// without opening anything.
func Explain(ctx context.Context, outs ...goav.Output) (plan.Report, error) {
	return Default().Explain(ctx, outs...)
}

// Options returns fresh runtime options for all bundled formats, codecs,
// filters, and URL schemes. The returned slice is safe for callers to append
// to.
func Options() []runconfig.Option {
	return []runconfig.Option{Formats(), Codecs(), Filters(), Schemes()}
}

// Formats, Codecs, Filters, and Schemes are each one runtime option
// registering one bundled family, for runtimes that pair part of the bundle
// with their own adapters:
//
//	rt, err := goav.New(bundle.Filters(), myCodecOption)
//
// Formats registers the container adapters: IVF, Annex B, Matroska, WebM, MP4,
// FLV.
func Formats() runconfig.Option { return group(formatOptions) }

// Codecs registers the pure-Go codec adapters: Opus, AAC, VP8/VP9, AV1, H.264.
func Codecs() runconfig.Option { return group(codecOptions) }

// Filters registers the frame filters: resample and resize.
func Filters() runconfig.Option { return group(filterOptions) }

// Schemes registers the URL schemes goav.URIInput and goav.URI resolve
// through: file, http, and https (package scheme). Add others, or replace
// these, with runconfig.Scheme.
func Schemes() runconfig.Option { return group(schemeOptions) }

func group(options []runconfig.Option) runconfig.Option {
	return func(config *runconfig.Config) error {
		for _, option := range options {
			if err := option(config); err != nil {
				return err
			}
		}
		return nil
	}
}

var formatOptions = []runconfig.Option{
	runconfig.WithFormatAdapter(ivfadapter.Register),
	runconfig.WithFormatAdapter(annexbadapter.Register),
	runconfig.WithFormatAdapter(matroskaadapter.Register),
	runconfig.WithFormatAdapter(webmadapter.Register),
	runconfig.WithFormatAdapter(mp4adapter.Register),
	runconfig.WithFormatAdapter(flvadapter.Register),
}

var codecOptions = []runconfig.Option{
	runconfig.WithCodecAdapter(gopusadapter.Register),
	runconfig.WithCodecAdapter(goaacadapter.Register),
	runconfig.WithCodecAdapter(govpxadapter.Register),
	runconfig.WithCodecAdapter(goav1adapter.Register),
	runconfig.WithCodecAdapter(goh264adapter.Register),
}

var filterOptions = []runconfig.Option{
	runconfig.WithFilterAdapter(resampleadapter.Register),
	runconfig.WithFilterAdapter(resizeadapter.Register),
}

func appendOptions(base []runconfig.Option, extra ...runconfig.Option) []runconfig.Option {
	options := make([]runconfig.Option, 0, len(base)+len(extra))
	options = append(options, base...)
	options = append(options, extra...)
	return options
}

var schemeOptions = []runconfig.Option{
	runconfig.Scheme("file", scheme.File()),
	runconfig.Scheme("http", scheme.HTTP()),
	runconfig.Scheme("https", scheme.HTTP()),
}
