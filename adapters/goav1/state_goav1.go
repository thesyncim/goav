package goav1

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

const (
	defaultAlign         = 64
	defaultMaxFrames     = 1
	defaultMaxEvents     = 1
	defaultRuntimeEvents = 8
	defaultMaxPayload    = 1200
	defaultMaxRetained   = 4096
	defaultDecoderWorker = 1
	defaultRTPSeedBytes  = 64 * 1024
	maxAV1PayloadEvents  = 64
)

// DecoderStateConfig describes caller-owned AV1 decoder state for the realtime
// adapter path.
type DecoderStateConfig struct {
	// Bounds supplies generic per-input result, payload, retained-fragment, and
	// geometry limits from codec.DecodeConfig.
	Bounds codec.DecodeBounds
	// Format is the expected coded frame format. If empty, Bounds.MaxWidth and
	// Bounds.MaxHeight are used for 8-bit 4:2:0 output.
	Format backend.FrameFormat
	// Workers defaults to one.
	Workers int
}

// DecoderState owns the reusable goav1.Decoder and scratch buffers required by
// a packet-by-packet AV1 decoder. It can be passed as
// codec.DecodeConfig.OpaqueState.
type DecoderState struct {
	bounds  codec.DecodeBounds
	format  backend.FrameFormat
	workers int

	dec *backend.Decoder

	obuScratch    []backend.RTPPacketizerOBU
	packetScratch []backend.RTPPacketPlan
	workScratch   []backend.RTPPacketPlan
	rtpScratch    []byte
	paddingSeed   []byte

	initPayloads [][]byte
	initBuf      []byte
}

// DecoderFrameFormatFromBounds converts DecodeBounds into the default 8-bit
// 4:2:0 coded frame format used when no stream pixel format is supplied.
func DecoderFrameFormatFromBounds(bounds codec.DecodeBounds) (backend.FrameFormat, error) {
	bounds = normalizeDecoderBounds(bounds)
	if bounds.MaxWidth <= 0 || bounds.MaxHeight <= 0 {
		return backend.FrameFormat{}, codec.ErrUnsupportedFormat
	}
	format := backend.FrameFormat{
		Width:        bounds.MaxWidth,
		Height:       bounds.MaxHeight,
		BitDepth:     8,
		SubsamplingX: true,
		SubsamplingY: true,
		Align:        defaultAlign,
	}
	if _, _, err := backendFramePixelFormat(format); err != nil {
		return backend.FrameFormat{}, err
	}
	return format, nil
}

// DecoderFrameFormatFromStream converts stream metadata and bounds into the
// frame format used by the AV1 adapter.
func DecoderFrameFormatFromStream(stream av.Stream, bounds codec.DecodeBounds) (backend.FrameFormat, error) {
	bounds = normalizeDecoderBounds(bounds)
	if bounds.MaxWidth <= 0 {
		bounds.MaxWidth = stream.Codec.Width
	}
	if bounds.MaxHeight <= 0 {
		bounds.MaxHeight = stream.Codec.Height
	}
	format, err := DecoderFrameFormatFromBounds(bounds)
	if err != nil {
		return backend.FrameFormat{}, err
	}
	switch stream.Codec.PixelFormat {
	case "", av.PixelFormatI420:
		format.MonoChrome = false
		format.SubsamplingX = true
		format.SubsamplingY = true
	case av.PixelFormatI422:
		format.MonoChrome = false
		format.SubsamplingX = true
		format.SubsamplingY = false
	case av.PixelFormatI444:
		format.MonoChrome = false
		format.SubsamplingX = false
		format.SubsamplingY = false
	case av.PixelFormatGray8:
		format.MonoChrome = true
		format.SubsamplingX = true
		format.SubsamplingY = true
	default:
		return backend.FrameFormat{}, codec.ErrUnsupportedFormat
	}
	if _, _, err := backendFramePixelFormat(format); err != nil {
		return backend.FrameFormat{}, err
	}
	return format, nil
}

// NewDecoderState allocates reusable AV1 decoder state and scratch buffers from
// explicit bounds and format.
func NewDecoderState(config DecoderStateConfig) (*DecoderState, error) {
	bounds := normalizeDecoderBounds(config.Bounds)
	format := config.Format
	if format.Width == 0 || format.Height == 0 {
		var err error
		format, err = DecoderFrameFormatFromBounds(bounds)
		if err != nil {
			return nil, err
		}
	}
	if _, _, err := backendFramePixelFormat(format); err != nil {
		return nil, err
	}
	workers := config.Workers
	if workers <= 0 {
		workers = defaultDecoderWorker
	}
	seedEvents := min(max(bounds.MaxEventsPerInput, defaultRuntimeEvents), maxAV1PayloadEvents)
	seedBytes := max(bounds.MaxPayloadBytes+bounds.MaxRetainedBytes, defaultRTPSeedBytes)
	rtpCap := max(bounds.MaxPayloadBytes+64, defaultRTPSeedBytes)

	return &DecoderState{
		bounds:        bounds,
		format:        format,
		workers:       workers,
		obuScratch:    make([]backend.RTPPacketizerOBU, maxAV1PayloadEvents),
		packetScratch: make([]backend.RTPPacketPlan, 16),
		workScratch:   make([]backend.RTPPacketPlan, 16),
		rtpScratch:    make([]byte, rtpCap),
		paddingSeed:   buildAV1PaddingRTPSeed(seedEvents, seedBytes),
	}, nil
}

func normalizeDecoderBounds(bounds codec.DecodeBounds) codec.DecodeBounds {
	return bounds.WithDefaults(codec.DecodeBounds{
		MaxFramesPerInput: defaultMaxFrames,
		MaxEventsPerInput: defaultMaxEvents,
		MaxPayloadBytes:   defaultMaxPayload,
		MaxRetainedBytes:  defaultMaxRetained,
	})
}

// buildAV1PaddingRTPSeed constructs a valid AV1 RTP payload composed solely of
// OBU_PADDING elements so NewDecoderFromRTPPayloads sizes its reusable RTP
// buffer and event/span slices to handle subsequent live payloads without
// failing with ErrRTPShortBuffer or ErrEventBufferTooSmall.
func buildAV1PaddingRTPSeed(eventSlots, totalBytes int) []byte {
	if eventSlots < 1 {
		eventSlots = 1
	}
	if eventSlots > maxAV1PayloadEvents {
		eventSlots = maxAV1PayloadEvents
	}
	if totalBytes < 16 {
		totalBytes = 16
	}
	// Aggregation header: Z=0, Y=0, W=0 (all elements LEB128 length-prefixed), N=0.
	out := make([]byte, 0, totalBytes+eventSlots*2+8)
	out = append(out, 0x00)
	// (eventSlots - 1) minimal 1-byte OBU_PADDING elements (header 0x78: type=15, has_size=0).
	for i := 0; i < eventSlots-1; i++ {
		out = append(out, 0x01, 0x78)
	}
	// Final OBU_PADDING element sized to cover totalBytes.
	padOBULen := max(totalBytes-(eventSlots-1), 1)
	out = appendLEB128(out, uint64(padOBULen))
	out = append(out, 0x78)
	if padOBULen > 1 {
		out = append(out, make([]byte, padOBULen-1)...)
	}
	return out
}

func appendLEB128(dst []byte, v uint64) []byte {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if v == 0 {
			return dst
		}
	}
}

func (s *DecoderState) Bounds() codec.DecodeBounds {
	if s == nil {
		return codec.DecodeBounds{}
	}
	return s.bounds
}

func (s *DecoderState) Format() backend.FrameFormat {
	if s == nil {
		return backend.FrameFormat{}
	}
	return s.format
}

func (s *DecoderState) HasSequenceHeader() bool {
	return s != nil && s.dec != nil
}

func (s *DecoderState) ensureDecoder(seedPayloads [][]byte) error {
	if s == nil {
		return codec.ErrUnsupportedFormat
	}
	if s.dec != nil {
		s.dec.Close()
		s.dec = nil
	}
	seeds := make([][]byte, 0, len(seedPayloads)+1)
	seeds = append(seeds, seedPayloads...)
	if len(s.paddingSeed) > 0 {
		seeds = append(seeds, s.paddingSeed)
	}
	opts := make([]backend.Option, 0, 2)
	opts = append(opts, backend.WithWorkers(s.workers))
	if s.bounds.MaxWidth > 0 && s.bounds.MaxHeight > 0 {
		opts = append(opts, backend.WithFrameSizeLimit(s.bounds.MaxWidth*s.bounds.MaxHeight))
	}
	dec, err := backend.NewDecoderFromRTPPayloads(seeds, opts...)
	if err != nil {
		return err
	}
	s.dec = dec
	return nil
}

func (s *DecoderState) packetizeLowOverhead(payload []byte, keyframe bool) ([]byte, error) {
	limits := backend.RTPPayloadSizeLimits{
		MaxPayloadLen: max(len(payload)+64, 1024),
	}
	p, err := backend.NewRTPPacketizer(payload, limits, keyframe, true, s.obuScratch, s.packetScratch, s.workScratch)
	if err != nil {
		return nil, err
	}
	needed, ok := p.NextPacketSize()
	if !ok {
		return nil, nil
	}
	if needed > len(s.rtpScratch) {
		s.rtpScratch = make([]byte, max(needed, len(s.rtpScratch)*2))
	}
	n, _, ok, err := p.NextPacket(s.rtpScratch)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return s.rtpScratch[:n], nil
}

func (s *DecoderState) resetInitFragments() {
	s.initPayloads = s.initPayloads[:0]
	s.initBuf = s.initBuf[:0]
}

func (s *DecoderState) Reset() {
	if s == nil {
		return
	}
	if s.dec != nil {
		_ = s.dec.Reset()
	}
	s.resetInitFragments()
}

func (s *DecoderState) Close() {
	if s == nil {
		return
	}
	if s.dec != nil {
		s.dec.Close()
		s.dec = nil
	}
	s.resetInitFragments()
}
