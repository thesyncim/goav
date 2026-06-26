package goav

import (
	"context"
	"io"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
)

func (b *builder) openMuxDestinationStage(ctx context.Context, destination destinationSpec, index int, streams []av.Stream, formatID av.FormatID, detailFormat av.FormatID) (*format.MuxStage, error) {
	output, writer, err := b.openDestinationOutput(ctx, destination, streams, formatID)
	if err != nil {
		return nil, err
	}
	stage, err := b.openMuxStage(ctx, output, index, streams, formatID, detailFormat, writer)
	if err != nil {
		if writer != nil {
			closeDestinationWriterAfterFailure(writer)
		}
		return nil, err
	}
	return stage, nil
}

func (b *builder) openDestinationOutput(ctx context.Context, destination destinationSpec, streams []av.Stream, formatID av.FormatID) (format.Output, provider.Writer, error) {
	output := destination.output
	if formatID == "" {
		outputProbe, err := b.runtime.formats.Probe(ctx, outputProbeRequest(output))
		if err != nil {
			return output, nil, outputFormatProbeError(output, 0, err)
		}
		formatID = outputProbe.Format
	}
	if output.Writer != nil {
		// goav.File close contract: a writer that also implements io.Closer is
		// closed exactly once when the destination finalizes; a plain writer
		// stays the caller's to close.
		if closer, ok := output.Writer.(io.WriteCloser); ok {
			return output, closer, nil
		}
		return output, nil, nil
	}
	if destination.custom == nil {
		return output, nil, nil
	}
	info := provider.Info{
		Name:     firstNonEmpty(destination.name, output.Name, output.URI),
		Format:   formatID,
		MIMEType: output.MIMEType,
		Streams:  cloneStreams(streams),
		Metadata: cloneMetadata(output.Metadata),
		Realtime: b.runtime.realtime || output.Realtime,
	}
	writer, err := destination.custom.Open(ctx, info)
	if err != nil {
		return output, nil, err
	}
	if writer == nil {
		return output, nil, errNilWriter
	}
	output.Writer = writer
	return output, writer, nil
}

func (b *builder) openMuxStage(ctx context.Context, output format.Output, index int, streams []av.Stream, formatID av.FormatID, detailFormat av.FormatID, writer provider.Writer) (*format.MuxStage, error) {
	if formatID == "" {
		outputProbe, err := b.runtime.formats.Probe(ctx, outputProbeRequest(output))
		if err != nil {
			return nil, outputFormatProbeError(output, index, err)
		}
		formatID = outputProbe.Format
	}
	muxFactory, err := b.runtime.formats.MuxerFactory(formatID)
	if err != nil {
		return nil, outputMuxerMissingError(output, index, formatID, err)
	}
	muxer, err := muxFactory.NewMuxer(ctx, formatID)
	if err != nil {
		return nil, outputMuxerMissingError(output, index, formatID, err)
	}
	if err := muxer.Open(ctx, output, streams, format.OpenOptions{
		Realtime: b.runtime.realtime || output.Realtime,
		Metadata: output.Metadata,
	}); err != nil {
		muxer.Close()
		return nil, err
	}
	if writer != nil {
		transaction := &destinationTransaction{requireSuccess: b.requireRunOK, name: muxNodeName(output, index)}
		b.destinationTxs = append(b.destinationTxs, transaction)
		muxer = &destinationWriterMuxer{
			Muxer:       muxer,
			writer:      writer,
			transaction: transaction,
		}
	}
	stage, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   muxNodeName(output, index),
		Detail: outputNodeDetailWithFormat(output, detailFormat),
		Muxer:  muxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		muxer.Close()
		return nil, err
	}
	return stage, nil
}

type destinationWriterMuxer struct {
	format.Muxer
	writer      provider.Writer
	transaction *destinationTransaction
	failed      bool
}

func (m *destinationWriterMuxer) Write(ctx context.Context, packet *av.Packet, result *format.WriteResult) error {
	err := m.Muxer.Write(ctx, packet, result)
	if err != nil {
		m.MarkFailed()
	}
	return err
}

func (m *destinationWriterMuxer) MarkFailed() {
	m.failed = true
	m.transaction.Fail()
}

func (m *destinationWriterMuxer) Close() error {
	muxErr := m.Muxer.Close()
	if muxErr != nil {
		m.MarkFailed()
	}
	transactionErr := error(nil)
	if transactional, ok := m.writer.(provider.TransactionalWriter); ok {
		if m.failed || m.transaction.ShouldAbort() {
			transactionErr = transactional.Abort(context.Background())
		} else {
			transactionErr = transactional.Commit(context.Background())
		}
		if transactionErr != nil {
			m.transaction.Fail()
		}
	}
	writerErr := m.writer.Close()
	if writerErr != nil {
		m.transaction.Fail()
	}
	if muxErr != nil {
		return muxErr
	}
	if transactionErr != nil {
		return transactionErr
	}
	return writerErr
}

func closeDestinationWriterAfterFailure(writer provider.Writer) {
	if writer == nil {
		return
	}
	if transactional, ok := writer.(provider.TransactionalWriter); ok {
		_ = transactional.Abort(context.Background())
	}
	_ = writer.Close()
}

type pipelineStageFailureMarker interface {
	MarkFailed()
}

func markPipelineStageFailed(stage pipeline.Stage) {
	if marker, ok := stage.(pipelineStageFailureMarker); ok {
		marker.MarkFailed()
	}
}

func cloneStreams(streams []av.Stream) []av.Stream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]av.Stream, len(streams))
	copy(out, streams)
	for i := range out {
		out[i].Metadata = cloneMetadata(out[i].Metadata)
		out[i].Codec.Attributes = cloneMetadata(out[i].Codec.Attributes)
		out[i].Codec.ExtraData = cloneBuffer(out[i].Codec.ExtraData)
	}
	return out
}

func outputProbeRequest(output format.Output) format.ProbeRequest {
	return format.ProbeRequest{
		Name:     output.Name,
		MIMEType: output.MIMEType,
		Input: format.Input{
			Name:     output.Name,
			URI:      output.URI,
			Protocol: output.Protocol,
			MIMEType: output.MIMEType,
			Realtime: output.Realtime,
			Metadata: output.Metadata,
		},
	}
}

func muxNodeName(output format.Output, index int) string {
	if output.Name != "" {
		return output.Name
	}
	if output.URI != "" {
		return output.URI
	}
	if index == 0 {
		return "output"
	}
	return "output-" + strconv.Itoa(index)
}
