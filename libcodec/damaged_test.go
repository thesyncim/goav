package libcodec_test

import (
	"context"
	"slices"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/libcodec"
	"github.com/thesyncim/goav/runconfig"
)

// TestDamagedUnitDecodesOn damages one P-frame's slice data and pins that a
// library decoder drops what it cannot decode, asks for a keyframe when its
// library reports the damage, and goes on decoding the units after it.
func TestDamagedUnitDecodesOn(t *testing.T) {
	stream, packets := shortGOP(t)
	damaged := slices.IndexFunc(packets, func(p av.Packet) bool { return !p.Keyframe })
	heal := damaged + slices.IndexFunc(packets[damaged:], func(p av.Packet) bool { return p.Keyframe })
	if damaged < 0 || heal <= damaged {
		t.Fatalf("%d packets: the first P-frame at %d, the next keyframe at %d", len(packets), damaged, heal)
	}
	data := slices.Clone(packets[damaged].Payload.Bytes)
	for i := len(data) / 4; i < len(data); i++ {
		data[i] = 0xff // past the slice header: no start code can appear
	}
	packets[damaged].Payload = av.Buffer{Bytes: data}
	for name, option := range map[string]runconfig.Option{"openh264": libcodec.OpenH264(), "videotoolbox": libcodec.VideoToolbox()} {
		t.Run(name, func(t *testing.T) {
			config, err := runconfig.NewConfig(option)
			if err != nil {
				t.Fatal(err)
			}
			choice, err := config.Codecs.Select(codec.Need{ID: av.CodecH264, Mode: codec.ModeDecode, Backend: name})
			if err != nil {
				t.Skip(err)
			}
			decodeConfig := codec.DecodeConfig{Stream: stream}
			decodeConfig.Resilience.RequestKeyframes = true
			decoder, err := choice.Decoder.NewDecoder(context.Background(), decodeConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer decoder.Close()
			var result codec.DecodeResult
			frames, after, requests := 0, 0, 0
			for i := range packets {
				if err := decoder.DecodeInto(context.Background(), &packets[i], &result); err != nil {
					t.Fatalf("unit %d of %d (damaged %d): %v", i, len(packets), damaged, err)
				}
				frames += len(result.Frames)
				if i > damaged {
					after += len(result.Frames)
				}
				requests += len(result.Requests)
				release(&result)
			}
			if err := decoder.FlushInto(context.Background(), &result); err != nil {
				t.Fatal(err)
			}
			frames += len(result.Frames)
			release(&result)
			if after < len(packets)-heal || requests > 1 || (frames < len(packets) && requests != 1) {
				t.Fatalf("decoded %d of %d units, %d after the damaged one (want %d or more), with %d keyframe requests", frames, len(packets), after, len(packets)-heal, requests)
			}
			t.Logf("%d of %d units decoded, %d keyframe requests", frames, len(packets), requests)
		})
	}
}

// shortGOP encodes test video with a keyframe every five frames, with the
// library H.264 encoder this host runs.
func shortGOP(t *testing.T) (av.Stream, []av.Packet) {
	t.Helper()
	name, option := libH264(t)
	collector := goavtest.NewCollector()
	out := goavtest.Video(64, 48, 20).Video().Encode(codec.H264(codec.Backend(name), codec.KeyframeInterval(5))).To(collector.Sink())
	if err := bundle.MustNew(option).Run(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	var packets []av.Packet
	for _, p := range collector.Packets() {
		packet := *p
		packet.Payload = av.Buffer{Bytes: slices.Clone(p.Payload.Bytes)}
		packets = append(packets, packet)
	}
	if len(packets) == 0 {
		t.Fatal("no packets")
	}
	stream := av.Stream{ID: packets[0].StreamID, Type: av.MediaVideo, TimeBase: packets[0].PTS.Base, Codec: av.CodecParameters{ID: av.CodecH264, Width: 64, Height: 48}}
	return stream, packets
}

func release(result *codec.DecodeResult) {
	for i := range result.Frames {
		result.Frames[i].Release()
	}
	result.Reset()
}
