package bundle

import (
	"fmt"
	"sort"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	runconfig "github.com/thesyncim/goav/runconfig"
)

type bundledCapabilities struct {
	Demuxers []format.Descriptor
	Muxers   []format.Descriptor
	Codecs   []codec.Descriptor
	Filters  []filter.Descriptor
}

func collectBundledCapabilities() (bundledCapabilities, error) {
	config, err := runconfig.NewConfig(Options()...)
	if err != nil {
		return bundledCapabilities{}, err
	}
	caps := bundledCapabilities{
		Demuxers: config.Formats.DemuxerDescriptors(),
		Muxers:   config.Formats.MuxerDescriptors(),
		Codecs:   config.Codecs.Descriptors(),
		Filters:  config.Filters.Descriptors(),
	}
	sort.Slice(caps.Codecs, func(i, j int) bool {
		left := string(caps.Codecs[i].ID) + "\x00" + caps.Codecs[i].Backend.Name + "\x00" + caps.Codecs[i].Name
		right := string(caps.Codecs[j].ID) + "\x00" + caps.Codecs[j].Backend.Name + "\x00" + caps.Codecs[j].Name
		return left < right
	})
	return caps, nil
}

func bundledCapabilityMarkdown() (string, error) {
	caps, err := collectBundledCapabilities()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("<!-- BEGIN GENERATED BUNDLE CAPABILITIES -->\n")
	b.WriteString("The tables below are generated from `bundle.Options()` and the descriptors registered by the bundled adapters. Update the descriptors, not this section by hand.\n\n")
	b.WriteString("### Container Formats\n\n")
	b.WriteString("| Format | Direction | Media | Codecs | Streams | Notes |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- |\n")
	for _, desc := range caps.Demuxers {
		writeFormatRow(&b, desc, "demux")
	}
	for _, desc := range caps.Muxers {
		writeFormatRow(&b, desc, "mux")
	}
	b.WriteString("\n### Codecs\n\n")
	b.WriteString("| Codec | Media | Modes | Backend | Capabilities | Tags | Status |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- |\n")
	for _, desc := range caps.Codecs {
		fmt.Fprintf(&b, "| `%s` | %s | %s | `%s` | %s | %s | %s |\n",
			orDash(string(desc.ID)),
			orDash(string(desc.Type)),
			codecModes(desc.Modes),
			orDash(desc.Backend.Name),
			codecCapabilities(desc),
			stringList(desc.Capabilities.BuildTags),
			orDash(desc.Backend.Status),
		)
	}
	b.WriteString("\n### Frame Filters\n\n")
	b.WriteString("| Filter | Media | Formats | Modes | Traits |\n")
	b.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, desc := range caps.Filters {
		fmt.Fprintf(&b, "| `%s` | %s -> %s | %s | %s | %s |\n",
			orDash(desc.Name),
			orDash(string(desc.Input)),
			orDash(string(desc.Output)),
			filterFormats(desc),
			resizeModes(desc.ResizeModes),
			filterTraits(desc),
		)
	}
	b.WriteString("<!-- END GENERATED BUNDLE CAPABILITIES -->")
	return b.String(), nil
}

func writeFormatRow(b *strings.Builder, desc format.Descriptor, direction string) {
	fmt.Fprintf(b, "| `%s` | %s | %s | %s | %s | %s |\n",
		orDash(string(desc.Format)),
		direction,
		mediaList(desc.Media),
		codecList(desc.Codecs),
		streamRange(desc),
		orDash(desc.Metadata["summary"]),
	)
}

func mediaList(values []av.MediaType) string {
	if len(values) == 0 {
		return "any"
	}
	items := make([]string, len(values))
	for i := range values {
		items[i] = string(values[i])
	}
	sort.Strings(items)
	return strings.Join(items, ", ")
}

func codecList(values []av.CodecID) string {
	if len(values) == 0 {
		return "declared by input"
	}
	items := make([]string, len(values))
	for i := range values {
		items[i] = string(values[i])
	}
	sort.Strings(items)
	return strings.Join(items, ", ")
}

func streamRange(desc format.Descriptor) string {
	switch {
	case desc.MinStreams > 0 && desc.MaxStreams > 0 && desc.MinStreams == desc.MaxStreams:
		return fmt.Sprintf("%d", desc.MinStreams)
	case desc.MinStreams > 0 && desc.MaxStreams > 0:
		return fmt.Sprintf("%d-%d", desc.MinStreams, desc.MaxStreams)
	case desc.MinStreams > 0:
		return fmt.Sprintf("%d+", desc.MinStreams)
	case desc.MaxStreams > 0:
		return fmt.Sprintf("up to %d", desc.MaxStreams)
	default:
		return "any"
	}
}

func codecModes(values []codec.Mode) string {
	if len(values) == 0 {
		return "-"
	}
	items := make([]string, len(values))
	for i := range values {
		items[i] = string(values[i])
	}
	sort.Strings(items)
	return strings.Join(items, ", ")
}

func codecCapabilities(desc codec.Descriptor) string {
	var parts []string
	if len(desc.Profiles) != 0 {
		parts = append(parts, "profiles="+stringList(desc.Profiles))
	}
	if len(desc.Capabilities.SampleFormats) != 0 {
		parts = append(parts, "samples="+stringList(desc.Capabilities.SampleFormats))
	}
	if len(desc.Capabilities.PixelFormats) != 0 {
		parts = append(parts, "pixels="+stringList(desc.Capabilities.PixelFormats))
	}
	if len(desc.Capabilities.RTPPayloads) != 0 {
		parts = append(parts, "rtp="+stringList(desc.Capabilities.RTPPayloads))
	}
	if desc.Realtime {
		parts = append(parts, "realtime")
	}
	if desc.Experimental {
		parts = append(parts, "experimental")
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, "; ")
}

func filterFormats(desc filter.Descriptor) string {
	var parts []string
	if len(desc.SampleFormats) != 0 {
		parts = append(parts, "samples="+stringList(desc.SampleFormats))
	}
	if len(desc.PixelFormats) != 0 {
		parts = append(parts, "pixels="+stringList(desc.PixelFormats))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, "; ")
}

func resizeModes(values []filter.ResizeMode) string {
	if len(values) == 0 {
		return "-"
	}
	items := make([]string, len(values))
	for i := range values {
		items[i] = string(values[i])
	}
	sort.Strings(items)
	return strings.Join(items, ", ")
}

func filterTraits(desc filter.Descriptor) string {
	var parts []string
	if backend := desc.Metadata["backend"]; backend != "" {
		parts = append(parts, "backend="+backend)
	}
	if desc.Realtime {
		parts = append(parts, "realtime")
	}
	if desc.Stateless {
		parts = append(parts, "stateless")
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, "; ")
}

func stringList(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	items := append([]string(nil), values...)
	sort.Strings(items)
	return strings.Join(items, ", ")
}

func orDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
