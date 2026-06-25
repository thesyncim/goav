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
	goavruntime "github.com/thesyncim/goav/runtime"
)

// ErrNilJob reports a nil recipe passed to Build or Run.
var ErrNilJob = errors.New("goav/bundle: nil job")

// New builds a runtime with the bundled formats, codecs, and filters already
// registered, then applies opts on top. Registration is last-wins, so opts can
// add or override bundled implementations.
func New(opts ...goavruntime.Option) (*goav.Runtime, error) {
	return goav.New(appendOptions(Options(), opts...)...)
}

// MustNew is New for package-level setup and tests.
func MustNew(opts ...goavruntime.Option) *goav.Runtime {
	return goav.MustNew(appendOptions(Options(), opts...)...)
}

// NewFormats builds a runtime with only the bundled container-format adapters.
func NewFormats(opts ...goavruntime.Option) (*goav.Runtime, error) {
	return goav.New(appendOptions(FormatOptions(), opts...)...)
}

// MustNewFormats is NewFormats for package-level setup and tests.
func MustNewFormats(opts ...goavruntime.Option) *goav.Runtime {
	return goav.MustNew(appendOptions(FormatOptions(), opts...)...)
}

// NewCodecs builds a runtime with only the bundled codec adapters.
func NewCodecs(opts ...goavruntime.Option) (*goav.Runtime, error) {
	return goav.New(appendOptions(CodecOptions(), opts...)...)
}

// MustNewCodecs is NewCodecs for package-level setup and tests.
func MustNewCodecs(opts ...goavruntime.Option) *goav.Runtime {
	return goav.MustNew(appendOptions(CodecOptions(), opts...)...)
}

// NewFilters builds a runtime with only the bundled frame-filter adapters.
func NewFilters(opts ...goavruntime.Option) (*goav.Runtime, error) {
	return goav.New(appendOptions(FilterOptions(), opts...)...)
}

// MustNewFilters is NewFilters for package-level setup and tests.
func MustNewFilters(opts ...goavruntime.Option) *goav.Runtime {
	return goav.MustNew(appendOptions(FilterOptions(), opts...)...)
}

// Build compiles job with a bundled runtime. It is the batteries-included
// counterpart to job.UseRuntime(bundle.MustNew(...)).Build(ctx), while preserving
// New option errors as returned errors.
func Build(ctx context.Context, job *goav.Job, opts ...goavruntime.Option) (goav.LiveTask, error) {
	if job == nil {
		return nil, ErrNilJob
	}
	runtime, err := New(opts...)
	if err != nil {
		return nil, err
	}
	return job.UseRuntime(runtime).Build(ctx)
}

// Run compiles and runs job with a bundled runtime, then closes it.
func Run(ctx context.Context, job *goav.Job, opts ...goavruntime.Option) error {
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
func Options() []goavruntime.Option {
	options := make([]goavruntime.Option, 0, len(formatOptions)+len(codecOptions)+len(filterOptions))
	options = append(options, FormatOptions()...)
	options = append(options, CodecOptions()...)
	options = append(options, FilterOptions()...)
	return options
}

// FormatOptions returns fresh options for the bundled container adapters: IVF,
// Annex B, Matroska, WebM, and MP4.
func FormatOptions() []goavruntime.Option {
	return appendOptions(formatOptions)
}

// CodecOptions returns fresh options for the bundled pure-Go codec adapters:
// Opus, AAC, VP8/VP9, AV1, and H264.
func CodecOptions() []goavruntime.Option {
	return appendOptions(codecOptions)
}

// FilterOptions returns fresh options for the bundled frame filters: resample
// and resize.
func FilterOptions() []goavruntime.Option {
	return appendOptions(filterOptions)
}

var formatOptions = []goavruntime.Option{
	goavruntime.WithFormatAdapter(ivfadapter.Register),
	goavruntime.WithFormatAdapter(annexbadapter.Register),
	goavruntime.WithFormatAdapter(matroskaadapter.Register),
	goavruntime.WithFormatAdapter(webmadapter.Register),
	goavruntime.WithFormatAdapter(mp4adapter.Register),
}

var codecOptions = []goavruntime.Option{
	goavruntime.WithCodecAdapter(gopusadapter.Register),
	goavruntime.WithCodecAdapter(goaacadapter.Register),
	goavruntime.WithCodecAdapter(govpxadapter.Register),
	goavruntime.WithCodecAdapter(goav1adapter.Register),
	goavruntime.WithCodecAdapter(goh264adapter.Register),
}

var filterOptions = []goavruntime.Option{
	goavruntime.WithFilterAdapter(resampleadapter.Register),
	goavruntime.WithFilterAdapter(resizeadapter.Register),
}

func appendOptions(base []goavruntime.Option, extra ...goavruntime.Option) []goavruntime.Option {
	options := make([]goavruntime.Option, 0, len(base)+len(extra))
	options = append(options, base...)
	options = append(options, extra...)
	return options
}
