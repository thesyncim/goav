package goav1

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

type EncoderFactory struct{}

func NewEncoderFactory() EncoderFactory {
	return EncoderFactory{}
}

func (EncoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	if config.Parameters.ID != "" && config.Parameters.ID != av.CodecAV1 {
		return nil, codec.ErrUnsupportedFormat
	}
	encoder := &Encoder{}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

type Encoder struct {
	encoder          *backend.VideoEncoder
	config           codec.EncodeConfig
	stream           av.Stream
	width            int
	height           int
	keyframeInterval int
	frameCount       int
	forceKey         bool
	closed           bool
}

func (e *Encoder) Descriptor() codec.Descriptor {
	return activeEncoderDescriptor()
}

func (e *Encoder) Open(ctx context.Context, config codec.EncodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, width, height, err := normalizeAV1EncodeStream(config)
	if err != nil {
		return err
	}
	config.Stream = stream
	config.Parameters = stream.Codec
	e.config = config
	e.stream = stream
	e.width = width
	e.height = height
	e.keyframeInterval = config.Settings.KeyframeInterval
	e.frameCount = 0
	e.forceKey = true
	e.closed = false
	return e.resetEncoder()
}

func (e *Encoder) EncodeInto(ctx context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if e.closed {
		return codec.ErrClosed
	}
	if frame == nil {
		return nil
	}
	if e.encoder == nil {
		return codec.ErrUnsupportedFormat
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}

	src, err := av1FrameFromFrame(frame, e.width, e.height)
	if err != nil {
		return err
	}

	forceKey := e.forceKey || e.frameCount == 0 || (e.keyframeInterval > 0 && e.frameCount%e.keyframeInterval == 0)
	encoded, err := e.encoder.Encode(src, forceKey)
	if err != nil {
		return mapGoav1EncodeError(err)
	}
	e.forceKey = false
	e.frameCount++
	if len(encoded.Data) == 0 {
		return nil
	}

	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	packet.StreamID = e.stream.ID
	packet.Type = av.MediaVideo
	packet.CodecEpoch = e.stream.Epoch
	packet.Payload.Bytes = append(packet.Payload.Bytes[:0], encoded.Data...)
	packet.Payload.Ownership = av.BufferOwned
	packet.PTS = rescaleAV1Timestamp(frame.PTS, e.stream.TimeBase)
	packet.DTS = packet.PTS
	packet.Duration = rescaleAV1Duration(frame.Duration, e.stream.TimeBase)
	packet.Keyframe = encoded.Keyframe
	packet.Metadata = frame.Metadata
	return nil
}

func (e *Encoder) FlushInto(ctx context.Context, out *codec.EncodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if e.closed {
		return codec.ErrClosed
	}
	return nil
}

func (e *Encoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	if e.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventKeyframeRequired:
		e.forceKey = true
	case av.EventBitrateChanged:
		bitsPerSecond, ok := codec.EventBitrate(event)
		if !ok {
			return fmt.Errorf("goav1: bitrate event needs positive %s metadata in bits per second", av.MetadataBitrate)
		}
		e.config.Settings.Bitrate = bitsPerSecond
		if err := e.resetEncoder(); err != nil {
			return err
		}
		e.forceKey = true
		e.frameCount = 0
	case av.EventCodecChanged, av.EventDiscontinuity:
		if err := e.resetEncoder(); err != nil {
			return err
		}
		e.forceKey = true
		e.frameCount = 0
	}
	return nil
}

func (e *Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.encoder = nil
	return nil
}

func (e *Encoder) resetEncoder() error {
	nativeConfig, err := av1EncoderConfig(e.config, e.width, e.height)
	if err != nil {
		return err
	}
	if control := e.config.Settings.Control; control != nil {
		if err := control(&nativeConfig); err != nil {
			return err
		}
	}
	encoder, err := backend.NewVideoEncoder(nativeConfig)
	if err != nil {
		return mapGoav1EncodeError(err)
	}
	e.encoder = encoder
	return nil
}

func normalizeAV1EncodeStream(config codec.EncodeConfig) (av.Stream, int, int, error) {
	stream := config.Stream
	parameters := config.Parameters
	if stream.Type == "" {
		stream.Type = av.MediaVideo
	}
	if parameters.Type == "" {
		parameters.Type = av.MediaVideo
	}
	if stream.Type != "" && stream.Type != av.MediaVideo {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	if parameters.Type != "" && parameters.Type != av.MediaVideo {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	if stream.Codec.ID != "" && stream.Codec.ID != av.CodecAV1 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	if parameters.ID != "" && parameters.ID != av.CodecAV1 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}

	width := pickPositive(parameters.Width, stream.Codec.Width)
	height := pickPositive(parameters.Height, stream.Codec.Height)
	if width < 16 || height < 16 || width%2 != 0 || height%2 != 0 {
		return av.Stream{}, 0, 0, fmt.Errorf("goav1: AV1 encode dimensions must be even and at least 16x16, got %dx%d: %w", width, height, codec.ErrUnsupportedFormat)
	}

	pixelFormat := firstNonEmpty(parameters.PixelFormat, stream.Codec.PixelFormat)
	if pixelFormat == "" {
		pixelFormat = av.PixelFormatI420
	}
	if normalizeAV1EncodePixelFormat(pixelFormat) != av.PixelFormatI420 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}

	parameters.ID = av.CodecAV1
	parameters.Type = av.MediaVideo
	parameters.Width = width
	parameters.Height = height
	parameters.PixelFormat = av.PixelFormatI420
	if parameters.ClockRate == 0 {
		parameters.ClockRate = pickClockRate(stream.Codec.ClockRate, 90000)
	}
	stream.Type = av.MediaVideo
	stream.Codec = parameters
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.RTPTimeBase(parameters.ClockRate)
	}
	return stream, width, height, nil
}

func av1EncoderConfig(config codec.EncodeConfig, width, height int) (backend.VideoEncoderConfig, error) {
	nativeConfig := backend.VideoEncoderConfig{
		Width:         width,
		Height:        height,
		TargetBitrate: config.Settings.Bitrate,
		Framerate:     encodeAV1FPS(config),
	}
	if nativeConfig.TargetBitrate <= 0 {
		nativeConfig.QIndex = 96
	}
	for key, value := range config.Settings.Custom {
		if err := applyAV1EncoderSetting(&nativeConfig, key, value); err != nil {
			return backend.VideoEncoderConfig{}, err
		}
	}
	if nativeConfig.TargetBitrate > 0 && nativeConfig.Framerate <= 0 {
		nativeConfig.Framerate = 30
	}
	if nativeConfig.TargetBitrate <= 0 && nativeConfig.QIndex == 0 {
		nativeConfig.QIndex = 96
	}
	return nativeConfig, nil
}

func applyAV1EncoderSetting(config *backend.VideoEncoderConfig, key, value string) error {
	normalized := normalizeAV1SettingKey(key)
	switch normalized {
	case "q", "qindex", "q_index":
		parsed, err := parseAV1Uint8Setting(key, value, 1, 255)
		if err != nil {
			return err
		}
		config.QIndex = parsed
		config.TargetBitrate = 0
	case "min_q", "min_qindex", "min_q_index":
		parsed, err := parseAV1Uint8Setting(key, value, 1, 255)
		if err != nil {
			return err
		}
		config.MinQIndex = parsed
	case "max_q", "max_qindex", "max_q_index":
		parsed, err := parseAV1Uint8Setting(key, value, 1, 255)
		if err != nil {
			return err
		}
		config.MaxQIndex = parsed
	case "temporal_layers", "layers":
		parsed, err := parseAV1IntSetting(key, value)
		if err != nil {
			return err
		}
		if parsed < 0 || parsed > 3 {
			return fmt.Errorf("goav1: %s must be between 0 and 3: %w", key, codec.ErrUnsupportedFormat)
		}
		config.TemporalLayers = parsed
	case "tile_columns", "tiles":
		parsed, err := parseAV1IntSetting(key, value)
		if err != nil {
			return err
		}
		if parsed < 0 {
			return fmt.Errorf("goav1: %s must be non-negative: %w", key, codec.ErrUnsupportedFormat)
		}
		config.TileColumns = parsed
	case "golden_interval", "golden":
		parsed, err := parseAV1IntSetting(key, value)
		if err != nil {
			return err
		}
		config.GoldenInterval = parsed
	case "tune":
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "", "zerolatency", "zero-latency", "realtime", "lowdelay", "low-delay":
			return nil
		default:
			return fmt.Errorf("goav1: unsupported tune %q: %w", value, codec.ErrUnsupportedFormat)
		}
	default:
		return fmt.Errorf("goav1: unsupported AV1 encoder setting %q (supported: qindex, min_qindex, max_qindex, temporal_layers, tile_columns, golden_interval, tune): %w", key, codec.ErrUnsupportedFormat)
	}
	return nil
}

func av1FrameFromFrame(frame *av.Frame, width, height int) (backend.I420Frame, error) {
	if frame.Type != "" && frame.Type != av.MediaVideo {
		return backend.I420Frame{}, codec.ErrUnsupportedFormat
	}
	if frame.Video == nil {
		return backend.I420Frame{}, codec.ErrUnsupportedFormat
	}
	if frame.Video.Width != width || frame.Video.Height != height {
		return backend.I420Frame{}, codec.ErrUnsupportedFormat
	}
	if format := normalizeAV1EncodePixelFormat(frame.Video.PixelFormat); format != "" && format != av.PixelFormatI420 {
		return backend.I420Frame{}, codec.ErrUnsupportedFormat
	}
	if len(frame.Planes) < 3 {
		return backend.I420Frame{}, codec.ErrOutputBufferTooSmall
	}
	y, err := planeData(frame.Planes[0])
	if err != nil {
		return backend.I420Frame{}, err
	}
	u, err := planeData(frame.Planes[1])
	if err != nil {
		return backend.I420Frame{}, err
	}
	v, err := planeData(frame.Planes[2])
	if err != nil {
		return backend.I420Frame{}, err
	}
	if frame.Planes[1].Stride != frame.Planes[2].Stride {
		return backend.I420Frame{}, codec.ErrUnsupportedFormat
	}
	chromaWidth := (width + 1) / 2
	chromaHeight := (height + 1) / 2
	if !planeHasExtent(y, frame.Planes[0].Stride, width, height) ||
		!planeHasExtent(u, frame.Planes[1].Stride, chromaWidth, chromaHeight) ||
		!planeHasExtent(v, frame.Planes[2].Stride, chromaWidth, chromaHeight) {
		return backend.I420Frame{}, codec.ErrOutputBufferTooSmall
	}
	return backend.I420Frame{
		Y:            y,
		U:            u,
		V:            v,
		YStride:      frame.Planes[0].Stride,
		ChromaStride: frame.Planes[1].Stride,
		Width:        width,
		Height:       height,
	}, nil
}

func normalizeAV1EncodePixelFormat(pixelFormat string) string {
	switch pixelFormat {
	case av.PixelFormatYUV420P:
		return av.PixelFormatI420
	default:
		return pixelFormat
	}
}

func planeData(plane av.Plane) ([]byte, error) {
	if plane.Offset < 0 || plane.Offset > len(plane.Buffer.Bytes) {
		return nil, codec.ErrOutputBufferTooSmall
	}
	return plane.Buffer.Bytes[plane.Offset:], nil
}

func planeHasExtent(data []byte, stride, width, height int) bool {
	if width <= 0 || height <= 0 || stride < width {
		return false
	}
	required := (height-1)*stride + width
	return required <= len(data)
}

func encodeAV1FPS(config codec.EncodeConfig) int {
	if fps := fpsFromAV1Duration(config.Settings.Framerate); fps > 0 {
		return fps
	}
	return 30
}

func fpsFromAV1Duration(duration av.Duration) int {
	std, ok := duration.ToDuration()
	if !ok || std <= 0 {
		return 0
	}
	fps := int64(time.Second) / int64(std)
	if fps <= 0 || fps > int64(^uint(0)>>1) {
		return 0
	}
	return int(fps)
}

func rescaleAV1Timestamp(timestamp av.Timestamp, base av.TimeBase) av.Timestamp {
	if !base.Valid() {
		return timestamp
	}
	if !timestamp.Base.Valid() {
		timestamp.Base = base
		return timestamp
	}
	if timestamp.Base == base {
		return timestamp
	}
	if rescaled, ok := timestamp.Rescale(base); ok {
		return rescaled
	}
	return timestamp
}

func rescaleAV1Duration(duration av.Duration, base av.TimeBase) av.Duration {
	if !base.Valid() {
		return duration
	}
	if !duration.Base.Valid() {
		duration.Base = base
		return duration
	}
	if duration.Base == base {
		return duration
	}
	if rescaled, ok := duration.Rescale(base); ok {
		return rescaled
	}
	return duration
}

func parseAV1Uint8Setting(key, value string, minValue, maxValue uint8) (uint8, error) {
	parsed, err := parseAV1IntSetting(key, value)
	if err != nil {
		return 0, err
	}
	if parsed < int(minValue) || parsed > int(maxValue) {
		return 0, fmt.Errorf("goav1: %s must be between %d and %d: %w", key, minValue, maxValue, codec.ErrUnsupportedFormat)
	}
	return uint8(parsed), nil
}

func parseAV1IntSetting(key, value string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("goav1: %s must be an integer: %w", key, codec.ErrUnsupportedFormat)
	}
	return parsed, nil
}

func normalizeAV1SettingKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.NewReplacer("-", "_", ".", "_").Replace(key)
	return key
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func pickPositive(a, b int) int {
	if a > 0 {
		return a
	}
	return b
}

func pickClockRate(a, b uint32) uint32 {
	if a != 0 {
		return a
	}
	return b
}

func mapGoav1EncodeError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, backend.ErrEncoderInvalidConfig),
		errors.Is(err, backend.ErrEncoderInvalidFrame),
		errors.Is(err, backend.ErrEncoderUnsupported):
		return fmt.Errorf("goav1: %v: %w", err, codec.ErrUnsupportedFormat)
	default:
		return err
	}
}
