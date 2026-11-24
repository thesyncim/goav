package goav1

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

const (
	testOBUSequenceHeader byte = 1
	testOBUFrameHeader    byte = 3
	testOBUTileGroup      byte = 4
)

func TestDecoderFrameFormatFromBounds(t *testing.T) {
	format, err := DecoderFrameFormatFromBounds(codec.DecodeBounds{
		MaxWidth:  64,
		MaxHeight: 48,
	})
	if err != nil {
		t.Fatal(err)
	}
	if format.Width != 64 || format.Height != 48 || format.BitDepth != 8 ||
		!format.SubsamplingX || !format.SubsamplingY || format.Align != defaultAlign {
		t.Fatalf("format = %+v", format)
	}
}

func TestDecoderFrameFormatFromStreamPixelFormats(t *testing.T) {
	tests := []struct {
		name         string
		pixelFormat  string
		mono         bool
		subsamplingX bool
		subsamplingY bool
	}{
		{name: "default", subsamplingX: true, subsamplingY: true},
		{name: "i420", pixelFormat: av.PixelFormatI420, subsamplingX: true, subsamplingY: true},
		{name: "yuv420p", pixelFormat: av.PixelFormatYUV420P, subsamplingX: true, subsamplingY: true},
		{name: "i422", pixelFormat: av.PixelFormatI422, subsamplingX: true},
		{name: "yuv422p", pixelFormat: av.PixelFormatYUV422P, subsamplingX: true},
		{name: "i444", pixelFormat: av.PixelFormatI444},
		{name: "yuv444p", pixelFormat: av.PixelFormatYUV444P},
		{name: "gray8", pixelFormat: av.PixelFormatGray8, mono: true, subsamplingX: true, subsamplingY: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			format, err := DecoderFrameFormatFromStream(av.Stream{
				Codec: av.CodecParameters{
					Width:       64,
					Height:      48,
					PixelFormat: test.pixelFormat,
				},
			}, codec.DecodeBounds{})
			if err != nil {
				t.Fatal(err)
			}
			if format.Width != 64 || format.Height != 48 || format.BitDepth != 8 ||
				format.MonoChrome != test.mono ||
				format.SubsamplingX != test.subsamplingX ||
				format.SubsamplingY != test.subsamplingY {
				t.Fatalf("format = %+v", format)
			}
		})
	}
}

func TestDecoderFrameFormatFromStreamRejectsUnsupportedPixelFormat(t *testing.T) {
	_, err := DecoderFrameFormatFromStream(av.Stream{
		Codec: av.CodecParameters{
			Width:       64,
			Height:      48,
			PixelFormat: "nv12",
		},
	}, codec.DecodeBounds{})
	if !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestNewDecoderStateLifecycle(t *testing.T) {
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput: 2,
			MaxEventsPerInput: 3,
			MaxPayloadBytes:   100,
			MaxRetainedBytes:  200,
			MaxWidth:          64,
			MaxHeight:         64,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.HasSequenceHeader() {
		t.Fatal("expected HasSequenceHeader() == false before first decode")
	}
	if state.Bounds().MaxWidth != 64 || state.Format().Width != 64 {
		t.Fatalf("bounds=%+v format=%+v", state.Bounds(), state.Format())
	}
	state.Reset()
	state.Close()
}

func testTileData() []byte {
	return []byte{0x0a, 0x1b, 0x59, 0x03, 0xa2, 0x4c, 0x8f, 0x24, 0x87, 0xdc, 0xb4, 0x19, 0x46, 0x81, 0xa4, 0x82, 0x4f, 0x21}
}

func testLowOverheadStream() []byte {
	var payload []byte
	payload = appendTestLowOverheadOBU(payload, testOBUSequenceHeader, testSequenceHeaderPayload())
	payload = appendTestLowOverheadOBU(payload, testOBUFrameHeader, testFrameHeaderPayload())
	payload = appendTestLowOverheadOBU(payload, testOBUTileGroup, testTileData())
	return payload
}

func testRTPPayload() []byte {
	elements := [...]backend.RTPElement{
		{Data: testRTPElement(testOBUSequenceHeader, testSequenceHeaderPayload())},
		{Data: testRTPElement(testOBUFrameHeader, testFrameHeaderPayload())},
		{Data: testRTPElement(testOBUTileGroup, testTileData())},
	}
	var dst [128]byte
	n, err := backend.PutRTPPayload(dst[:], backend.RTPAggregationHeader{
		ElementCount:                3,
		StartsNewCodedVideoSequence: true,
	}, elements[:])
	if err != nil {
		panic(err)
	}
	return append([]byte(nil), dst[:n]...)
}

func testRTPFramePayload() []byte {
	elements := [...]backend.RTPElement{
		{Data: testRTPElement(testOBUFrameHeader, testFrameHeaderPayload())},
		{Data: testRTPElement(testOBUTileGroup, testTileData())},
	}
	var dst [128]byte
	n, err := backend.PutRTPPayload(dst[:], backend.RTPAggregationHeader{
		ElementCount: 2,
	}, elements[:])
	if err != nil {
		panic(err)
	}
	return append([]byte(nil), dst[:n]...)
}

func testFragmentedRTPPayloads() [][]byte {
	var payloads [][]byte
	var packet [128]byte

	seq := [...]backend.RTPElement{
		{Data: testRTPElement(testOBUSequenceHeader, testSequenceHeaderPayload())},
	}
	n, err := backend.PutRTPPayload(packet[:], backend.RTPAggregationHeader{
		ElementCount:                1,
		StartsNewCodedVideoSequence: true,
	}, seq[:])
	if err != nil {
		panic(err)
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))

	frameHeader := testRTPElement(testOBUFrameHeader, testFrameHeaderPayload())
	n, next, more, err := backend.PutRTPFragment(packet[:], frameHeader, 0, 2, false)
	if err != nil {
		panic(err)
	}
	if !more {
		panic("expected frame header RTP fragment")
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))
	n, _, more, err = backend.PutRTPFragment(packet[:], frameHeader, next, len(packet), false)
	if err != nil {
		panic(err)
	}
	if more {
		panic("unexpected third frame header RTP fragment")
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))

	tile := [...]backend.RTPElement{
		{Data: testRTPElement(testOBUTileGroup, testTileData())},
	}
	n, err = backend.PutRTPPayload(packet[:], backend.RTPAggregationHeader{ElementCount: 1}, tile[:])
	if err != nil {
		panic(err)
	}
	payloads = append(payloads, append([]byte(nil), packet[:n]...))
	return payloads
}

func testRTPElement(typ byte, payload []byte) []byte {
	element := make([]byte, 0, 1+len(payload))
	element = append(element, typ<<3)
	element = append(element, payload...)
	return element
}

func appendTestLowOverheadOBU(dst []byte, typ byte, payload []byte) []byte {
	if len(payload) > 0x7f {
		panic("test payload too large")
	}
	dst = append(dst, (typ<<3)|0x02)
	dst = append(dst, byte(len(payload)))
	dst = append(dst, payload...)
	return dst
}

func testSequenceHeaderPayload() []byte {
	var w testBitWriter
	w.writeBits(0, 3)  // seq_profile
	w.writeBool(true)  // still_picture
	w.writeBool(true)  // reduced_still_picture_header
	w.writeBits(5, 5)  // seq_level_idx[0]
	w.writeBits(7, 4)  // frame_width_bits_minus_1
	w.writeBits(7, 4)  // frame_height_bits_minus_1
	w.writeBits(15, 8) // max_frame_width_minus_1
	w.writeBits(15, 8) // max_frame_height_minus_1
	w.writeBool(false) // use_128x128_superblock
	w.writeBool(false) // enable_filter_intra
	w.writeBool(true)  // enable_intra_edge_filter
	w.writeBool(false) // enable_superres
	w.writeBool(true)  // enable_cdef
	w.writeBool(false) // enable_restoration
	w.writeBool(false) // high_bitdepth
	w.writeBool(true)  // mono_chrome
	w.writeBool(false) // color_description_present_flag
	w.writeBool(false) // color_range
	w.writeBool(false) // film_grain_params_present
	return w.trailingBits()
}

func testFrameHeaderPayload() []byte {
	var w testBitWriter
	w.writeBool(false) // disable_cdf_update
	w.writeBool(false) // allow_screen_content_tools
	w.writeBool(false) // render_and_frame_size_different
	w.writeBool(false) // uniform_tile_spacing_flag
	w.writeBits(0, 8)  // base_q_idx
	w.writeBool(false) // y_dc_delta_q
	w.writeBool(false) // using_qmatrix
	w.writeBool(false) // segmentation_enabled
	w.writeBool(false) // reduced_tx_set
	return w.trailingBits()
}

type testBitWriter struct {
	buf [128]byte
	bit int
}

func (w *testBitWriter) writeBits(value uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		if (value>>uint(i))&1 != 0 {
			w.buf[w.bit>>3] |= 1 << uint(7-(w.bit&7))
		}
		w.bit++
	}
}

func (w *testBitWriter) writeBool(value bool) {
	if value {
		w.writeBits(1, 1)
		return
	}
	w.writeBits(0, 1)
}

func (w *testBitWriter) bytes() []byte {
	return w.buf[:(w.bit+7)>>3]
}

func (w *testBitWriter) trailingBits() []byte {
	w.writeBits(1, 1)
	for w.bit&7 != 0 {
		w.writeBits(0, 1)
	}
	return w.buf[:w.bit>>3]
}
