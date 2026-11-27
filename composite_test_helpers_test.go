package goav

import "github.com/thesyncim/goav/av"

// compositeTestI420Frame builds a tightly-packed I420 frame whose Y/U/V planes are
// each filled with a constant value, so a copied pixel is easy to identify.
func compositeTestI420Frame(id av.StreamID, w, h int, y, u, v byte) *av.Frame {
	chromaW := (w + 1) / 2
	chromaH := (h + 1) / 2
	yPlane := make([]byte, w*h)
	uPlane := make([]byte, chromaW*chromaH)
	vPlane := make([]byte, chromaW*chromaH)
	for i := range yPlane {
		yPlane[i] = y
	}
	for i := range uPlane {
		uPlane[i] = u
	}
	for i := range vPlane {
		vPlane[i] = v
	}
	return &av.Frame{
		StreamID: id,
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: 7},
		Duration: av.Duration{Value: 3},
		Video:    &av.VideoFrame{Width: w, Height: h, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: yPlane}, Stride: w},
			{Buffer: av.Buffer{Bytes: uPlane}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: vPlane}, Stride: chromaW},
		},
	}
}

func compositeTestYAt(frame *av.Frame, x, y int) byte {
	return frame.Planes[0].Buffer.Bytes[y*frame.Video.Width+x]
}

// compositeTestChromaAt reads the U and V samples at chroma coordinates (x, y).
func compositeTestChromaAt(frame *av.Frame, x, y int) (byte, byte) {
	chromaW := (frame.Video.Width + 1) / 2
	return frame.Planes[1].Buffer.Bytes[y*chromaW+x], frame.Planes[2].Buffer.Bytes[y*chromaW+x]
}
