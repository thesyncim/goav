package flv

import (
	"bytes"
	"context"
	"testing"

	"github.com/thesyncim/goav/container/flv/amf0"
	"github.com/thesyncim/goav/format"
)

// FuzzDemux feeds the demuxer arbitrary bytes: it must never panic or
// over-read, and what it reads back must mux.
func FuzzDemux(f *testing.F) {
	for _, name := range []string{"h264_aac", "hevc_aac", "av1_opus", "vp9_opus", "vp9_flac", "multi"} {
		data := fixture(f, name)
		f.Add(data[:min(len(data), 4096)])
	}
	f.Add(rawFile(flagVideo|flagAudio,
		rawTag(tagVideo, 0, []byte{0x90 | packetModEx, 0xff, 0, 0}),
		rawTag(tagAudio, 0, []byte{0x90 | audioMultitrack, manyTracksManyCodecs<<4 | packetCodedFrames, 'O', 'p', 'u', 's', 0, 0, 0, 9}),
		rawTag(tagScript, 0, []byte{2, 0, 10, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', 0x0a, 0xff, 0xff, 0xff, 0xff}),
	))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := demux(data)
		if err != nil || len(got.streams) == 0 {
			return
		}
		m := &Muxer{}
		if err := m.Open(context.Background(), format.Output{Writer: &bytes.Buffer{}}, got.streams, format.OpenOptions{}); err != nil {
			return
		}
		for i := range got.packets {
			if err := m.Write(context.Background(), &got.packets[i], nil); err != nil {
				return
			}
		}
		_ = m.Close()
	})
}

// FuzzScript feeds AMF0 decoding arbitrary script tag bodies.
func FuzzScript(f *testing.F) {
	meta, _ := amf0.Append(nil, "onMetaData", amf0.ECMAArray{{Name: "width", Value: 1280}, {Name: "videoTrackIdInfoMap", Value: amf0.Object{{Name: "1", Value: amf0.Object{}}}}})
	f.Add(meta)
	f.Fuzz(func(t *testing.T, body []byte) {
		(&Demuxer{}).script(body)
	})
}
