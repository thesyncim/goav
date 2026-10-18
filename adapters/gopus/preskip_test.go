package gopus

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// TestOpusRoundTripPresentsAtInputTime pins pre-skip end to end: the encoder
// declares its lookahead in the identification header and stamps packets at
// their first decoded sample, and the decoder drops the pre-skip, so a tone
// that starts 10 ms into the input decodes 10 ms into the output, at the
// input's own timestamps, and the flush drains the lookahead so the input's
// tail survives. Without either half the tone lands 6.5 ms late.
func TestOpusRoundTripPresentsAtInputTime(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewEncoderFactory().NewEncoder(ctx, opusEncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	head := encoder.(codec.ParameterReporter).EncodedParameters().ExtraData.Bytes
	if got := opusPreSkip(head); got != 312 {
		t.Fatalf("header pre-skip = %d, want the 312-sample lookahead", got)
	}

	const frames, frameSamples, onset = 10, 960, 480
	var packets []av.Packet
	for i := range frames {
		pcm := make([]byte, frameSamples*2)
		for s := range frameSamples {
			if n := i*frameSamples + s; n >= onset {
				binary.LittleEndian.PutUint16(pcm[s*2:], uint16(int16(12000*math.Sin(2*math.Pi*440*float64(n)/48000))))
			}
		}
		frame := opusTestFrame()
		frame.Planes[0].Buffer.Bytes = pcm
		frame.PTS = av.Timestamp{Value: int64(i * frameSamples), Base: av.TimeBase{Num: 1, Den: 48000}}
		result := opusEncodeResult(1, 4000)
		if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
			t.Fatal(err)
		}
		for _, packet := range result.Packets {
			packet.Payload.Bytes = append([]byte(nil), packet.Payload.Bytes...)
			packets = append(packets, packet)
		}
	}
	flush := opusEncodeResult(4, 4000)
	if err := encoder.FlushInto(ctx, &flush); err != nil {
		t.Fatal(err)
	}
	for _, packet := range flush.Packets {
		packet.Payload.Bytes = append([]byte(nil), packet.Payload.Bytes...)
		packets = append(packets, packet)
	}
	if first := packets[0].PTS; first.Value != -312 {
		t.Fatalf("first packet PTS = %+v, want -312: it presents at its first decoded sample", first)
	}

	stream := opusTestStream()
	stream.Codec.ExtraData = av.Buffer{Bytes: head}
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: stream})
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	var decoded []int16
	var firstPTS av.Timestamp
	for i := range packets {
		_, result := newTestDecoder(t)
		if err := decoder.DecodeInto(ctx, &packets[i], &result); err != nil {
			t.Fatal(err)
		}
		for _, frame := range result.Frames {
			if decoded == nil {
				firstPTS = frame.PTS
			}
			pcm := frame.Planes[0].Buffer.Bytes
			for s := 0; s+1 < len(pcm); s += 2 {
				decoded = append(decoded, int16(binary.LittleEndian.Uint16(pcm[s:])))
			}
		}
	}
	if firstPTS.Value != 0 {
		t.Fatalf("first frame PTS = %+v, want 0, the input's first sample", firstPTS)
	}
	// The flush drains the lookahead, so every input sample comes back.
	if len(decoded) < frames*frameSamples {
		t.Fatalf("decoded %d samples, want all %d input samples", len(decoded), frames*frameSamples)
	}
	start := -1
	for i, sample := range decoded {
		if sample > 2000 || sample < -2000 {
			start = i
			break
		}
	}
	// The first sine sample above the threshold is ~0.4 ms past the onset;
	// allow a couple of milliseconds for the codec's transient smearing.
	if start < onset-96 || start > onset+96 {
		t.Fatalf("tone starts at decoded sample %d, want ~%d (input onset)", start, onset)
	}
}
