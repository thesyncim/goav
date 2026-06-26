package integration

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/source"
)

// onStreamRTPReceiver feeds an opus stream, then announces a late VP8 stream
// on its event channel and pumps that stream's RTP packets until stopped.
type onStreamRTPReceiver struct {
	audio    av.Stream
	video    av.Stream
	payloads rtpav.PayloadMap
	events   chan av.Event

	announced bool
	sent      int
	stop      <-chan struct{}
}

func (r *onStreamRTPReceiver) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.audio}, nil
}

func (r *onStreamRTPReceiver) PayloadMap() rtpav.PayloadMap {
	return r.payloads
}

func (r *onStreamRTPReceiver) ReadRTP(ctx context.Context) (*rtp.Packet, error) {
	if !r.announced {
		r.announced = true
		announced := r.video
		r.events <- av.Event{Type: av.EventStreamAdded, StreamID: announced.ID, Stream: &announced}
		return &rtp.Packet{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}, nil
	}
	select {
	case <-r.stop:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Millisecond):
		r.sent++
		return &rtp.Packet{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: uint32(90 * r.sent), SequenceNumber: uint16(r.sent)},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}, nil
	}
}

func (r *onStreamRTPReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *onStreamRTPReceiver) Close() error {
	return nil
}

// TestOnStreamRTPLateStreamAttachesBranch wires the real producer end to end:
// an RTP receive provider announces a late VP8 stream mid-run; the rtpav
// source derives a depacketizer for it and forwards the announce, and the
// OnStream rule attaches a packet branch that receives the late stream's
// packets under its own stream id.
func TestOnStreamRTPLateStreamAttachesBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	audio := audioOpusTestStream()
	video := av.Stream{
		ID:       "cam",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
		},
	}
	stop := make(chan struct{})
	receiver := &onStreamRTPReceiver{
		audio: audio,
		video: video,
		payloads: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{
			{PayloadType: 111, Parameters: audio.Codec, MIMEType: av.MIMEOpus, ClockRate: 48000, Channels: codec.Stereo},
			{PayloadType: 96, Parameters: video.Codec, MIMEType: av.MIMEVP8, ClockRate: 90000},
		}),
		events: make(chan av.Event, 2),
		stop:   stop,
	}

	var lateVideo atomic.Int32
	monitor := goav.Sink(component.SinkFunc("monitor", func(_ context.Context, msg component.Message) error {
		if msg.Kind == pipeline.MessagePacket && msg.Packet.StreamID == video.ID {
			lateVideo.Add(1)
		}
		return nil
	}))

	task, err := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()))),
	).
		OnStream(source.MatchMedia(av.MediaVideo), goav.Branch("cam-watch").Copy().To(monitor)).
		Audio().Copy().To(goav.Sink(component.SinkFunc("main", func(context.Context, component.Message) error { return nil }))).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitForCount(t, "late video packets", 1, lateVideo.Load)
	waitForCondition(t, "rule branch attached", func() bool {
		return taskHasBranch(task, "cam-watch-cam")
	})
	close(stop)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}
