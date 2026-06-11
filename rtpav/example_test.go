package rtpav_test

import (
	"context"
	"fmt"
	"io"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
)

// staticRTPReader is a minimal rtpav.PacketReader: one declared Opus stream
// and an immediately ended packet feed.
type staticRTPReader struct{}

func (staticRTPReader) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}}}, nil
}

func (staticRTPReader) PayloadMap() rtpav.PayloadMap { return nil }

func (staticRTPReader) ReadRTP(context.Context) (*rtp.Packet, error) { return nil, io.EOF }

func (staticRTPReader) Events() <-chan av.Event { return nil }

func (staticRTPReader) Close() error { return nil }

// ExampleInput opens an RTP receive provider as a goav recipe input: the
// provider seam carries the transport, goav carries the recipe.
func ExampleInput() {
	mic := goav.Input(rtpav.Receive(staticRTPReader{}, rtpav.WithName("mic"), rtpav.WithCodec(codec.Opus())))

	report, err := goav.From(mic).
		Audio().
		Copy().
		To(goav.File("mic.webm", io.Discard)).
		Explain(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	in := report.Inputs[0]
	fmt.Printf("input=%s codec=%s realtime=%t\n", in.Name, in.Codec, in.Realtime)
	// Output: input=mic codec=opus realtime=true
}
