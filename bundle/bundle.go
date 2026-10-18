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
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	mp4adapter "github.com/thesyncim/goav/container/mp4"
	webmadapter "github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	runconfig "github.com/thesyncim/goav/runconfig"
)

// New builds a runtime with the bundled formats, codecs, and filters already
// registered, then applies opts on top. Registration is last-wins, so opts can
// add or override bundled implementations.
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

// Default is the bundled runtime with default options, built once and shared:
// a runtime holds no per-run state, so every Run, Build, Describe, and Explain
// here uses it. Build a runtime with New for other options.
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

// Options returns fresh runtime options for all bundled formats, codecs, and
// filters. The returned slice is safe for callers to append to.
func Options() []runconfig.Option {
	return []runconfig.Option{Formats(), Codecs(), Filters()}
}

// Formats, Codecs, and Filters are each one runtime option registering one
// bundled family, for runtimes that pair part of the bundle with their own
// adapters:
//
//	rt, err := goav.New(bundle.Filters(), myCodecOption)
//
// Formats registers the container adapters: IVF, Annex B, Matroska, WebM, MP4.
func Formats() runconfig.Option { return group(formatOptions) }

// Codecs registers the pure-Go codec adapters: Opus, AAC, VP8/VP9, AV1, H.264.
func Codecs() runconfig.Option { return group(codecOptions) }

// Filters registers the frame filters: resample and resize.
func Filters() runconfig.Option { return group(filterOptions) }

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
