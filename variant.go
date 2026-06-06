package goav

import "github.com/thesyncim/goav/av"

type VariantSpec struct {
	name       string
	transforms []TransformSpec
	taps       []string
	encode     CodecSpec
	labels     []string
	err        error
}

type VariantBuilder struct {
	spec VariantSpec
}

func Variant(name string) *VariantBuilder {
	return &VariantBuilder{spec: VariantSpec{name: name}}
}

func (b *VariantBuilder) Resize(width int, height int, options ...resizeOption) *VariantBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build variant", firstNonEmpty(b.spec.name, "variant"), "resize", b.spec.encode))
		return b
	}
	b.spec.transforms = append(b.spec.transforms, Resize(width, height, options...))
	return b
}

func (b *VariantBuilder) Resample(sampleRate int, channels int, options ...audioOption) *VariantBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build variant", firstNonEmpty(b.spec.name, "variant"), "resample", b.spec.encode))
		return b
	}
	b.spec.transforms = append(b.spec.transforms, Resample(sampleRate, channels, options...))
	return b
}

func (b *VariantBuilder) Tap(name string) *VariantBuilder {
	if b == nil {
		return b
	}
	if name == "" {
		b.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build variant",
			Node:      firstNonEmpty(b.spec.name, "variant"),
			Reason:    "tap name is empty",
			Suggestions: []string{
				"call .Tap(\"video.720p.frames\") or another stable tap name",
				"omit .Tap(...) when no runtime branch should attach at that point",
			},
			Cause: ErrUnsupportedBuild,
		})
		return b
	}
	b.spec.taps = append(b.spec.taps, name)
	return b
}

func (b *VariantBuilder) Encode(codec CodecSpec) *VariantBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(duplicateStreamEncodeError("build variant", firstNonEmpty(b.spec.name, "variant"), b.spec.encode, codec))
		return b
	}
	b.spec.encode = codec
	return b
}

func (b *VariantBuilder) Opus(bitrate int, options ...codecOption) *VariantBuilder {
	return b.Encode(Opus(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *VariantBuilder) VP8(bitrate int, options ...codecOption) *VariantBuilder {
	return b.Encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *VariantBuilder) VP9(bitrate int, options ...codecOption) *VariantBuilder {
	return b.Encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *VariantBuilder) To(labels ...string) VariantSpec {
	if b == nil {
		return VariantSpec{err: nilVariantError()}
	}
	spec := b.snapshot()
	spec.labels = append([]string(nil), labels...)
	return spec
}

func (b *VariantBuilder) snapshot() VariantSpec {
	spec := b.spec
	spec.transforms = cloneTransformSpecs(spec.transforms)
	spec.taps = append([]string(nil), spec.taps...)
	spec.labels = append([]string(nil), spec.labels...)
	return spec
}

func (b *VariantBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func (b *JobStreamBuilder) Variants(variants ...VariantSpec) *Job {
	stream := b.current()
	job := b.job
	if len(variants) == 0 {
		job.setErr(variantMissingError(jobStreamName(stream)))
		return job
	}
	if len(job.inputs) != 1 {
		job.setErr(flowTeeInputCountError(jobStreamName(stream), len(job.inputs)))
		return job
	}
	if len(job.outputs) != 0 || len(stream.outputs) != 0 {
		job.setErr(flowTeeOutputScopeError(jobStreamName(stream)))
		return job
	}

	for i := range variants {
		if err := validateVariantSpec(stream.selector.Type, i, variants[i]); err != nil {
			job.setErr(err)
			return job
		}
		job.teeStreams = append(job.teeStreams, streamBuild{
			name:       variants[i].name,
			selector:   stream.selector,
			fromTap:    lastStreamTap(stream),
			decode:     true,
			transforms: cloneTransformSpecs(variants[i].transforms),
			taps:       append([]string(nil), variants[i].taps...),
			encode:     variants[i].encode,
			labels:     append([]string(nil), variants[i].labels...),
		})
	}
	return job
}

func validateVariantSpec(selected av.MediaType, index int, spec VariantSpec) error {
	if spec.err != nil {
		return spec.err
	}
	if spec.name == "" {
		return transcodeIntentBranchNameMissingError(index, StreamIntent{Select: StreamSelect{Type: selected}})
	}
	if !codecIntentSet(spec.encode) {
		return transcodeEncodeMissingError(StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}})
	}
	if len(spec.labels) == 0 {
		return transcodeBranchOutputMissingError(StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}})
	}
	seen := make(map[string]int, len(spec.labels))
	for i, label := range spec.labels {
		if label == "" {
			return transcodeEmptyOutputLabelError(streamBuild{name: spec.name, selector: av.StreamSelector{Type: selected}}, i)
		}
		if firstIndex, ok := seen[label]; ok {
			return transcodeDuplicateBranchOutputError(
				StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}, RouteTo: spec.labels},
				label,
				firstIndex,
				i,
			)
		}
		seen[label] = i
	}
	return nil
}

func variantMissingError(node string) error {
	return &BuildError{
		Code:      "variant_missing",
		Operation: "build variants",
		Node:      node,
		Reason:    "Variants requires at least one encoded variant",
		Suggestions: []string{
			"pass variants with goav.Variant(name).VP9(...).To(label)",
			"define shared output labels once with .Output(label, goav.FileOutput(...))",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func nilVariantError() error {
	return &BuildError{
		Code:      "variant_invalid",
		Operation: "build variant",
		Reason:    "variant is nil",
		Suggestions: []string{
			"build variants with goav.Variant(name)",
		},
		Cause: ErrUnsupportedBuild,
	}
}
