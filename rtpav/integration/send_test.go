package integration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/rtpav"
	runconfig "github.com/thesyncim/goav/runconfig"
)

const mp4Fixture = "../../container/mp4/testdata/h264_aac.mp4"

// captureWriter is an rtpav.PacketWriter that keeps a copy of every RTP
// packet and hears no feedback.
type captureWriter struct {
	mu      sync.Mutex
	packets []rtp.Packet
	at      []time.Time
	closed  chan struct{}
}

func (w *captureWriter) WriteRTP(_ context.Context, packet *rtp.Packet) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	clone := *packet
	clone.Payload = append([]byte(nil), packet.Payload...)
	w.packets = append(w.packets, clone)
	w.at = append(w.at, time.Now())
	return nil
}

func (w *captureWriter) WriteRTCP(context.Context, []rtcp.Packet) error { return nil }

func (w *captureWriter) ReadRTCP(ctx context.Context) ([]rtcp.Packet, error) {
	select {
	case <-w.closed:
		return nil, rtpav.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *captureWriter) Close() error {
	close(w.closed)
	return nil
}

// TestSendCopiesMP4VideoWithParameterSets sends an MP4's H264 video, copied,
// through rtpav.Send on an offline runtime: the file plays at its media
// time, since Send is live; the depacketized stream opens with SPS and PPS
// ahead of its IDR (from the avcC, since the samples carry none in band);
// and every packet keeps one SSRC and consecutive sequence numbers.
func TestSendCopiesMP4VideoWithParameterSets(t *testing.T) {
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	writer := &captureWriter{closed: make(chan struct{})}
	rt := bundle.MustNew(runconfig.WithRealtime(false))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := rt.Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).Video().To(goav.Sink(rtpav.Send(writer)))); err != nil {
		t.Fatal(err)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.packets) == 0 {
		t.Fatal("no packets")
	}
	media := time.Duration(writer.packets[len(writer.packets)-1].Timestamp-writer.packets[0].Timestamp) * time.Second / 90000
	if wall := writer.at[len(writer.at)-1].Sub(writer.at[0]); media < 200*time.Millisecond || wall < media*8/10 {
		t.Fatalf("%s of media sent in %s, want it paced at its media time", media, wall)
	}
	depacketizer := rtpav.NewH264Depacketizer(av.Stream{ID: "v", Type: av.MediaVideo})
	var units [][]byte
	for i, packet := range writer.packets {
		if packet.SSRC != writer.packets[0].SSRC || packet.SequenceNumber != writer.packets[0].SequenceNumber+uint16(i) || packet.PayloadType != 102 {
			t.Fatalf("packet %d: %v", i, packet.Header)
		}
		result := rtpav.DepacketizeResult{Packets: make([]av.Packet, 0, 1), Events: make([]av.Event, 0, 1)}
		if err := depacketizer.PushInto(ctx, &packet, rtpav.PayloadCodec{ClockRate: 90000}, &result); err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		for _, unit := range result.Packets {
			units = append(units, append([]byte(nil), unit.Payload.Bytes...))
		}
	}
	if len(units) == 0 {
		t.Fatal("no access units")
	}
	var types []byte
	for i := 0; i+4 < len(units[0]); i++ {
		if units[0][i] == 0 && units[0][i+1] == 0 && units[0][i+2] == 0 && units[0][i+3] == 1 {
			types = append(types, units[0][i+4]&0x1f)
		}
	}
	if len(types) < 3 || types[0] != 7 || types[1] != 8 || types[len(types)-1] != 5 {
		t.Fatalf("first access unit's NAL types = %v, want SPS, PPS, ..., IDR", types)
	}
}

// TestSendRefusesARemuxWithAAC pins the planner's refusal of an MP4 copied
// whole into rtpav.Send: AAC has no RTP packetizer, and the fix decodes it.
func TestSendRefusesARemuxWithAAC(t *testing.T) {
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	rt := bundle.MustNew(runconfig.WithRealtime(false))
	_, err = rt.Build(context.Background(), goav.FileInput("in.mp4", bytes.NewReader(data)).To(goav.Sink(rtpav.Send(&captureWriter{closed: make(chan struct{})}))))
	var refusal *goav.BuildError
	if !errors.As(err, &refusal) || refusal.Code != "destination_shape_mismatch" || !slices.Contains(refusal.FixLines(), "route the input's streams one by one, decoding the aac one: input.Audio().Decode().To(destination)") {
		t.Fatalf("err = %v, want destination_shape_mismatch with the fix", err)
	}
}
