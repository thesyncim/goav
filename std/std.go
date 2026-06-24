// Package std provides the bundled pure-Go runtime adapters for goav recipes.
//
// Import this package when an application wants the standard codecs, container
// formats, and frame filters. The root package stays focused on the recipe
// grammar and runtime extension seams.
package std

import (
	"context"

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
)

// New builds a runtime with the bundled formats, codecs, and filters already
// registered, then applies opts on top. Registration is last-wins, so opts can
// add or override standard implementations.
func New(opts ...goav.Option) goav.Runtime {
	return goav.New(appendOptions(Options(), opts...)...)
}

// NewFormats builds a runtime with only the bundled container-format adapters.
func NewFormats(opts ...goav.Option) goav.Runtime {
	return goav.New(appendOptions(FormatOptions(), opts...)...)
}

// NewCodecs builds a runtime with only the bundled codec adapters.
func NewCodecs(opts ...goav.Option) goav.Runtime {
	return goav.New(appendOptions(CodecOptions(), opts...)...)
}

// NewFilters builds a runtime with only the bundled frame-filter adapters.
func NewFilters(opts ...goav.Option) goav.Runtime {
	return goav.New(appendOptions(FilterOptions(), opts...)...)
}

// Build compiles job with a standard runtime. It is the batteries-included
// counterpart to job.UseRuntime(std.New(...)).Build(ctx).
func Build(ctx context.Context, job *goav.Job, opts ...goav.Option) (goav.Task, error) {
	if job == nil {
		return goav.From().UseRuntime(New(opts...)).Build(ctx)
	}
	return job.UseRuntime(New(opts...)).Build(ctx)
}

// Run compiles and runs job with a standard runtime, then closes it.
func Run(ctx context.Context, job *goav.Job, opts ...goav.Option) error {
	if job == nil {
		return goav.From().UseRuntime(New(opts...)).Run(ctx)
	}
	return job.UseRuntime(New(opts...)).Run(ctx)
}

// Options returns fresh runtime options for all bundled formats, codecs, and
// filters. The returned slice is safe for callers to append to.
func Options() []goav.Option {
	options := make([]goav.Option, 0, len(formatOptions)+len(codecOptions)+len(filterOptions))
	options = append(options, FormatOptions()...)
	options = append(options, CodecOptions()...)
	options = append(options, FilterOptions()...)
	return options
}

// FormatOptions returns fresh options for the bundled container adapters: IVF,
// Annex B, Matroska, WebM, and MP4.
func FormatOptions() []goav.Option {
	return appendOptions(formatOptions)
}

// CodecOptions returns fresh options for the bundled pure-Go codec adapters:
// Opus, AAC, VP8/VP9, AV1, and H264.
func CodecOptions() []goav.Option {
	return appendOptions(codecOptions)
}

// FilterOptions returns fresh options for the bundled frame filters: resample
// and resize.
func FilterOptions() []goav.Option {
	return appendOptions(filterOptions)
}

var formatOptions = []goav.Option{
	goav.WithFormatAdapter(ivfadapter.Register),
	goav.WithFormatAdapter(annexbadapter.Register),
	goav.WithFormatAdapter(matroskaadapter.Register),
	goav.WithFormatAdapter(webmadapter.Register),
	goav.WithFormatAdapter(mp4adapter.Register),
}

var codecOptions = []goav.Option{
	goav.WithCodecAdapter(gopusadapter.Register),
	goav.WithCodecAdapter(goaacadapter.Register),
	goav.WithCodecAdapter(govpxadapter.Register),
	goav.WithCodecAdapter(goav1adapter.Register),
	goav.WithCodecAdapter(goh264adapter.Register),
}

var filterOptions = []goav.Option{
	goav.WithFilterAdapter(resampleadapter.Register),
	goav.WithFilterAdapter(resizeadapter.Register),
}

func appendOptions(base []goav.Option, extra ...goav.Option) []goav.Option {
	options := make([]goav.Option, 0, len(base)+len(extra))
	options = append(options, base...)
	options = append(options, extra...)
	return options
}
