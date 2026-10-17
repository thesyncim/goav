// Package bundle provides the bundled pure-Go runtime adapters for goav recipes.
//
// Import this package when an application wants the bundled codecs, container
// formats, and frame filters. The root package stays focused on the recipe
// grammar and runtime extension seams.
package bundle

import (
	"context"
	"errors"

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
	runconfig "github.com/thesyncim/goav/runconfig"
)

// ErrNilJob reports a nil recipe passed to Build or Run.
var ErrNilJob = errors.New("goav/bundle: nil job")

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

// Build compiles job with a bundled runtime into the narrow runnable Task. It
// is the batteries-included counterpart to
// job.UseRuntime(bundle.MustNew(...)).Build(ctx), while preserving New option
// errors as returned errors. For live control, build with
// job.UseRuntime(bundle.MustNew(...)).BuildLive(ctx).
func Build(ctx context.Context, job *goav.Job, opts ...runconfig.Option) (goav.Task, error) {
	if job == nil {
		return nil, ErrNilJob
	}
	runtime, err := New(opts...)
	if err != nil {
		return nil, err
	}
	return job.UseRuntime(runtime).BuildLive(ctx)
}

// Describe compiles job's graph shape with a bundled runtime without opening
// resources. It is the batteries-included counterpart to
// job.UseRuntime(bundle.MustNew(...)).Describe().
func Describe(ctx context.Context, job *goav.Job, opts ...runconfig.Option) (pipeline.Spec, error) {
	if job == nil {
		return pipeline.Spec{}, ErrNilJob
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return pipeline.Spec{}, err
	}
	runtime, err := New(opts...)
	if err != nil {
		return pipeline.Spec{}, err
	}
	return job.UseRuntime(runtime).Describe()
}

// Run compiles and runs job with a bundled runtime, then closes it.
func Run(ctx context.Context, job *goav.Job, opts ...runconfig.Option) error {
	if job == nil {
		return ErrNilJob
	}
	runtime, err := New(opts...)
	if err != nil {
		return err
	}
	return job.UseRuntime(runtime).Run(ctx)
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
