package resize

import (
	"context"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

const backendName = "resize"

type Factory struct{}

func NewFactory() Factory {
	return Factory{}
}

func Descriptor() filter.Descriptor {
	return filter.Descriptor{
		Name:         filter.FactoryResize,
		Input:        av.MediaVideo,
		Output:       av.MediaVideo,
		PixelFormats: []string{av.PixelFormatI420, av.PixelFormatYUV420P},
		ResizeModes: []filter.ResizeMode{
			filter.ResizeExact,
			filter.ResizeFit,
			filter.ResizeFill,
			filter.ResizePassthrough,
		},
		Realtime:  true,
		Stateless: true,
		Metadata: av.Metadata{
			"backend":       backendName,
			"pixel_formats": av.PixelFormatI420 + "," + av.PixelFormatYUV420P,
			"modes":         string(filter.ResizeExact) + "," + string(filter.ResizeFit) + "," + string(filter.ResizeFill) + "," + string(filter.ResizePassthrough),
		},
	}
}

func Register(registry *filter.SimpleRegistry) {
	registry.RegisterFactory(Descriptor(), NewFactory())
}

func (Factory) NewFilter(ctx context.Context, config filter.Config) (filter.FrameFilter, error) {
	resizer := &Filter{}
	if err := resizer.Open(ctx, config); err != nil {
		return nil, err
	}
	return resizer, nil
}

type Filter struct {
	inputWidth   int
	inputHeight  int
	inputFormat  string
	targetWidth  int
	targetHeight int
	outputFormat string
	mode         filter.ResizeMode
	outputVideo  av.VideoFrame
}

type geometry struct {
	inputWidth   int
	outputWidth  int
	outputHeight int
	cropX        int
	cropY        int
	cropWidth    int
	cropHeight   int
}

func (f *Filter) Descriptor() filter.Descriptor {
	return Descriptor()
}

func (f *Filter) Open(ctx context.Context, config filter.Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if config.Video == nil {
		return filter.ErrUnsupportedFormat
	}
	stream := config.Stream
	if stream.Type != "" && stream.Type != av.MediaVideo {
		return filter.ErrUnsupportedFormat
	}
	if stream.Codec.Type != "" && stream.Codec.Type != av.MediaVideo {
		return filter.ErrUnsupportedFormat
	}

	inputFormat, ok := normalizePixelFormat(stream.Codec.PixelFormat)
	if !ok {
		return filter.ErrUnsupportedFormat
	}
	outputFormat, ok := normalizePixelFormat(config.Video.PixelFormat)
	if !ok {
		return filter.ErrUnsupportedFormat
	}
	if outputFormat == "" {
		outputFormat = inputFormat
	}
	if outputFormat == "" {
		outputFormat = av.PixelFormatYUV420P
	}

	mode := config.Video.Mode
	if mode == "" {
		mode = filter.ResizeExact
	}
	if !supportedMode(mode) {
		return filter.ErrUnsupportedFormat
	}

	inputWidth := stream.Codec.Width
	inputHeight := stream.Codec.Height
	if (inputWidth != 0 && !validI420Dimension(inputWidth)) || (inputHeight != 0 && !validI420Dimension(inputHeight)) {
		return filter.ErrUnsupportedFormat
	}
	targetWidth := config.Video.Width
	targetHeight := config.Video.Height
	if (targetWidth != 0 && !validI420Dimension(targetWidth)) || (targetHeight != 0 && !validI420Dimension(targetHeight)) {
		return filter.ErrUnsupportedFormat
	}
	if (mode == filter.ResizeFit || mode == filter.ResizeFill) && (targetWidth <= 0 || targetHeight <= 0) {
		return filter.ErrUnsupportedFormat
	}

	f.inputWidth = inputWidth
	f.inputHeight = inputHeight
	f.inputFormat = inputFormat
	f.targetWidth = targetWidth
	f.targetHeight = targetHeight
	f.outputFormat = outputFormat
	f.mode = mode
	if inputWidth > 0 && inputHeight > 0 {
		g, err := f.resolveGeometry(inputWidth, inputHeight)
		if err != nil {
			return err
		}
		f.outputVideo = av.VideoFrame{
			Width:       g.outputWidth,
			Height:      g.outputHeight,
			PixelFormat: outputFormat,
		}
	}
	return nil
}

func (f *Filter) FilterInto(ctx context.Context, frame *av.Frame, out *filter.Result) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if frame == nil {
		return nil
	}
	if out == nil || len(out.Frames) == cap(out.Frames) {
		return filter.ErrResultFull
	}
	if frame.Type != "" && frame.Type != av.MediaVideo {
		return filter.ErrUnsupportedFormat
	}
	if len(frame.Planes) < 3 {
		return filter.ErrUnsupportedFormat
	}

	inputWidth, inputHeight := f.frameDimensions(frame)
	if !validI420Dimension(inputWidth) || !validI420Dimension(inputHeight) {
		return filter.ErrUnsupportedFormat
	}
	frameFormat := f.framePixelFormat(frame)
	if frameFormat == "" {
		frameFormat = f.inputFormat
	}
	if frameFormat == "" {
		frameFormat = av.PixelFormatYUV420P
	}
	if _, ok := normalizePixelFormat(frameFormat); !ok {
		return filter.ErrUnsupportedFormat
	}

	g, err := f.resolveGeometry(inputWidth, inputHeight)
	if err != nil {
		return err
	}
	if !inputPlanesReady(frame.Planes, inputWidth, inputHeight) {
		return filter.ErrUnsupportedFormat
	}

	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	output := &out.Frames[index]
	output.Reset()
	if cap(output.Planes) < 3 {
		out.Frames = out.Frames[:index]
		return filter.ErrOutputBufferTooSmall
	}
	output.Planes = output.Planes[:3]
	if !prepareOutputPlanes(output.Planes, g.outputWidth, g.outputHeight) {
		out.Frames = out.Frames[:index]
		return filter.ErrOutputBufferTooSmall
	}

	scaleI420(output.Planes, frame.Planes, g)

	f.outputVideo.Width = g.outputWidth
	f.outputVideo.Height = g.outputHeight
	f.outputVideo.PixelFormat = f.outputFormat

	output.StreamID = frame.StreamID
	output.CodecEpoch = frame.CodecEpoch
	output.Type = av.MediaVideo
	output.PTS = frame.PTS
	output.Duration = frame.Duration
	output.Video = &f.outputVideo
	output.Metadata = frame.Metadata
	return nil
}

func (f *Filter) FlushInto(context.Context, *filter.Result) error {
	return nil
}

func (f *Filter) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (f *Filter) Close() error {
	return nil
}

func (f *Filter) frameDimensions(frame *av.Frame) (int, int) {
	width := f.inputWidth
	height := f.inputHeight
	if frame.Video != nil {
		if frame.Video.Width != 0 {
			width = frame.Video.Width
		}
		if frame.Video.Height != 0 {
			height = frame.Video.Height
		}
	}
	return width, height
}

func (f *Filter) framePixelFormat(frame *av.Frame) string {
	if frame.Video != nil && frame.Video.PixelFormat != "" {
		return frame.Video.PixelFormat
	}
	return f.inputFormat
}

func (f *Filter) resolveGeometry(inputWidth int, inputHeight int) (geometry, error) {
	if !validI420Dimension(inputWidth) || !validI420Dimension(inputHeight) {
		return geometry{}, filter.ErrUnsupportedFormat
	}
	switch f.mode {
	case filter.ResizePassthrough:
		return fullGeometry(inputWidth, inputHeight, inputWidth, inputHeight), nil
	case filter.ResizeExact:
		outputWidth := f.targetWidth
		outputHeight := f.targetHeight
		if outputWidth == 0 {
			outputWidth = inputWidth
		}
		if outputHeight == 0 {
			outputHeight = inputHeight
		}
		if !validI420Dimension(outputWidth) || !validI420Dimension(outputHeight) {
			return geometry{}, filter.ErrUnsupportedFormat
		}
		return fullGeometry(outputWidth, outputHeight, inputWidth, inputHeight), nil
	case filter.ResizeFit:
		outputWidth, outputHeight := fitDimensions(inputWidth, inputHeight, f.targetWidth, f.targetHeight)
		if !validI420Dimension(outputWidth) || !validI420Dimension(outputHeight) {
			return geometry{}, filter.ErrUnsupportedFormat
		}
		return fullGeometry(outputWidth, outputHeight, inputWidth, inputHeight), nil
	case filter.ResizeFill:
		if !validI420Dimension(f.targetWidth) || !validI420Dimension(f.targetHeight) {
			return geometry{}, filter.ErrUnsupportedFormat
		}
		g := fillGeometry(inputWidth, inputHeight, f.targetWidth, f.targetHeight)
		if !validI420Dimension(g.cropWidth) || !validI420Dimension(g.cropHeight) {
			return geometry{}, filter.ErrUnsupportedFormat
		}
		return g, nil
	default:
		return geometry{}, filter.ErrUnsupportedFormat
	}
}

func fullGeometry(outputWidth int, outputHeight int, inputWidth int, inputHeight int) geometry {
	return geometry{
		inputWidth:   inputWidth,
		outputWidth:  outputWidth,
		outputHeight: outputHeight,
		cropWidth:    inputWidth,
		cropHeight:   inputHeight,
	}
}

func fitDimensions(inputWidth int, inputHeight int, targetWidth int, targetHeight int) (int, int) {
	if inputWidth <= 0 || inputHeight <= 0 || targetWidth <= 0 || targetHeight <= 0 {
		return 0, 0
	}
	if targetWidth*inputHeight <= targetHeight*inputWidth {
		return evenFloor(targetWidth), evenFloor((inputHeight*targetWidth + inputWidth/2) / inputWidth)
	}
	return evenFloor((inputWidth*targetHeight + inputHeight/2) / inputHeight), evenFloor(targetHeight)
}

func fillGeometry(inputWidth int, inputHeight int, targetWidth int, targetHeight int) geometry {
	g := geometry{
		inputWidth:   inputWidth,
		outputWidth:  targetWidth,
		outputHeight: targetHeight,
		cropWidth:    inputWidth,
		cropHeight:   inputHeight,
	}
	if inputWidth*targetHeight > inputHeight*targetWidth {
		g.cropWidth = evenFloor((inputHeight * targetWidth) / targetHeight)
		g.cropX = evenFloor((inputWidth - g.cropWidth) / 2)
		return g
	}
	g.cropHeight = evenFloor((inputWidth * targetHeight) / targetWidth)
	g.cropY = evenFloor((inputHeight - g.cropHeight) / 2)
	return g
}

func evenFloor(value int) int {
	if value < 2 {
		return 0
	}
	return value &^ 1
}

func validI420Dimension(value int) bool {
	return value > 0 && value%2 == 0
}

func supportedMode(mode filter.ResizeMode) bool {
	return mode == filter.ResizeExact ||
		mode == filter.ResizeFit ||
		mode == filter.ResizeFill ||
		mode == filter.ResizePassthrough
}

func normalizePixelFormat(format string) (string, bool) {
	switch strings.ToLower(format) {
	case "":
		return "", true
	case av.PixelFormatI420:
		return av.PixelFormatI420, true
	case av.PixelFormatYUV420P:
		return av.PixelFormatYUV420P, true
	default:
		return "", false
	}
}

func inputPlanesReady(planes []av.Plane, width int, height int) bool {
	return planeReady(&planes[0], width, height) &&
		planeReady(&planes[1], width/2, height/2) &&
		planeReady(&planes[2], width/2, height/2)
}

func planeReady(plane *av.Plane, width int, height int) bool {
	stride := plane.Stride
	if stride == 0 {
		stride = width
	}
	if width <= 0 || height <= 0 || stride < width {
		return false
	}
	need := (height-1)*stride + width
	return need <= len(plane.Buffer.Bytes)
}

func prepareOutputPlanes(planes []av.Plane, width int, height int) bool {
	return prepareOutputPlane(&planes[0], width, height) &&
		prepareOutputPlane(&planes[1], width/2, height/2) &&
		prepareOutputPlane(&planes[2], width/2, height/2)
}

func prepareOutputPlane(plane *av.Plane, width int, height int) bool {
	size := width * height
	if cap(plane.Buffer.Bytes) < size {
		return false
	}
	plane.Buffer.Bytes = plane.Buffer.Bytes[:size]
	plane.Buffer.Ownership = av.BufferOwned
	plane.Buffer.Owner = nil
	plane.Stride = width
	plane.Offset = 0
	return true
}

func scaleI420(dst []av.Plane, src []av.Plane, g geometry) {
	scalePlaneNearest(dst[0].Buffer.Bytes, dst[0].Stride, src[0].Buffer.Bytes, inputStride(&src[0], g.inputWidth), g.cropX, g.cropY, g.cropWidth, g.cropHeight, g.outputWidth, g.outputHeight)
	scalePlaneNearest(dst[1].Buffer.Bytes, dst[1].Stride, src[1].Buffer.Bytes, inputStride(&src[1], g.inputWidth/2), g.cropX/2, g.cropY/2, g.cropWidth/2, g.cropHeight/2, g.outputWidth/2, g.outputHeight/2)
	scalePlaneNearest(dst[2].Buffer.Bytes, dst[2].Stride, src[2].Buffer.Bytes, inputStride(&src[2], g.inputWidth/2), g.cropX/2, g.cropY/2, g.cropWidth/2, g.cropHeight/2, g.outputWidth/2, g.outputHeight/2)
}

func inputStride(plane *av.Plane, width int) int {
	if plane.Stride != 0 {
		return plane.Stride
	}
	return width
}

func scalePlaneNearest(dst []byte, dstStride int, src []byte, srcStride int, srcX int, srcY int, srcWidth int, srcHeight int, dstWidth int, dstHeight int) {
	if srcX == 0 && srcY == 0 && srcWidth == dstWidth && srcHeight == dstHeight && copyPlaneRows(dst, dstStride, src, srcStride, dstWidth, dstHeight) {
		return
	}
	if srcWidth > 0 && srcHeight > 0 && dstWidth > 0 && dstHeight > 0 && srcWidth%dstWidth == 0 && srcHeight%dstHeight == 0 {
		scalePlaneNearestInteger(dst, dstStride, src, srcStride, srcX, srcY, srcWidth/dstWidth, srcHeight/dstHeight, dstWidth, dstHeight)
		return
	}
	for y := 0; y < dstHeight; y++ {
		sourceY := srcY + (y*srcHeight)/dstHeight
		sourceRow := sourceY * srcStride
		targetRow := y * dstStride
		for x := 0; x < dstWidth; x++ {
			sourceX := srcX + (x*srcWidth)/dstWidth
			dst[targetRow+x] = src[sourceRow+sourceX]
		}
	}
}

func copyPlaneRows(dst []byte, dstStride int, src []byte, srcStride int, width int, height int) bool {
	if width <= 0 || height <= 0 || srcStride < width || dstStride < width {
		return false
	}
	srcEnd := (height-1)*srcStride + width
	dstEnd := (height-1)*dstStride + width
	if srcEnd > len(src) || dstEnd > len(dst) {
		return false
	}
	if srcStride == width && dstStride == width {
		copy(dst[:width*height], src[:width*height])
		return true
	}
	for row := 0; row < height; row++ {
		copy(dst[row*dstStride:row*dstStride+width], src[row*srcStride:row*srcStride+width])
	}
	return true
}

func scalePlaneNearestInteger(dst []byte, dstStride int, src []byte, srcStride int, srcX int, srcY int, xScale int, yScale int, dstWidth int, dstHeight int) {
	for y := 0; y < dstHeight; y++ {
		sourceRow := (srcY + y*yScale) * srcStride
		targetRow := y * dstStride
		for x := 0; x < dstWidth; x++ {
			dst[targetRow+x] = src[sourceRow+srcX+x*xScale]
		}
	}
}
