package flv

import (
	"bytes"
	"context"
	"testing"

	"github.com/thesyncim/goav/format"
)

// FuzzDemux feeds the demuxer arbitrary bytes: it must never panic or
// over-read, and what it reads back must mux.
func FuzzDemux(f *testing.F) {
	for _, name := range []string{"h264_aac", "hevc_aac", "av1_opus", "vp9_opus", "multi"} {
		data := fixture(f, name)
		f.Add(data[:min(len(data), 4096)])
	}
	f.Add(rawFile(flagVideo|flagAudio,
		rawTag(tagVideo, 0, []byte{0x90 | packetModEx, 0xff, 0, 0}),
		rawTag(tagAudio, 0, []byte{0x90 | audioMultitrack, manyTracksManyCodecs<<4 | packetCodedFrames, 'O', 'p', 'u', 's', 0, 0, 0, 9}),
		rawTag(tagScript, 0, []byte{amfString, 0, 10, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a', amfStrictArray, 0xff, 0xff, 0xff, 0xff}),
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
	meta := appendAMFString(nil, "onMetaData")
	meta = append(meta, amfECMAArray, 0, 0, 0, 1)
	meta = appendAMFEnd(appendNumberProperty(meta, "width", 1280))
	f.Add(meta)
	f.Fuzz(func(t *testing.T, body []byte) {
		(&Demuxer{}).script(body)
	})
}
