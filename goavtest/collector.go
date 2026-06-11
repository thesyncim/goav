// The Collector: the recording destination every pipeline test points at.

package goavtest

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
)

// Collector is a recording destination: point a chain at Sink() and read back
// everything it received through the ordered, race-safe accessors. Collected
// frames, packets, and events are safe deep copies taken at delivery — buffer
// ownership is respected, so a producer recycling its buffers can never
// corrupt what a test asserts on. Treat the returned values as read-only.
//
// The zero Collector is not usable; construct with NewCollector.
type Collector struct {
	dest goav.Destination

	mu      sync.Mutex
	frames  []*av.Frame
	packets []*av.Packet
	events  []av.Event
}

// NewCollector returns a ready Collector with a distinct generated sink name,
// so several collectors can terminate different branches of one job.
func NewCollector() *Collector {
	c := &Collector{}
	c.dest = goav.Sink(goav.SinkFunc(nextName("collector"), c.collect))
	return c
}

// Sink returns the Collector's destination. It is a stable handle — every
// call returns the same value — so branches can share it like any other
// goav.Destination.
func (c *Collector) Sink() goav.Destination {
	return c.dest
}

func (c *Collector) collect(_ context.Context, msg goav.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case msg.Frame != nil:
		c.frames = append(c.frames, cloneFrame(msg.Frame))
	case msg.Packet != nil:
		c.packets = append(c.packets, clonePacket(msg.Packet))
	case msg.Event != nil:
		c.events = append(c.events, cloneEvent(*msg.Event))
	}
	return nil
}

// Frames returns the collected frames in delivery order.
func (c *Collector) Frames() []*av.Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*av.Frame(nil), c.frames...)
}

// Packets returns the collected packets in delivery order.
func (c *Collector) Packets() []*av.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*av.Packet(nil), c.packets...)
}

// Events returns the collected events in delivery order.
func (c *Collector) Events() []av.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]av.Event(nil), c.events...)
}

// S16 returns the interleaved samples of every collected S16 audio frame, in
// delivery order — the one accessor almost every audio assertion wants.
// Frames that are not S16 audio are skipped.
func (c *Collector) S16() [][]int16 {
	frames := c.Frames()
	out := make([][]int16, 0, len(frames))
	for _, frame := range frames {
		if frame.Audio == nil || frame.Audio.SampleFormat != av.SampleFormatS16 || len(frame.Planes) == 0 {
			continue
		}
		payload := frame.Planes[0].Buffer.Bytes
		samples := make([]int16, len(payload)/2)
		for i := range samples {
			samples[i] = int16(binary.LittleEndian.Uint16(payload[i*2:]))
		}
		out = append(out, samples)
	}
	return out
}

// Wait polls until cond reports true or ctx ends — the wait-until-observed
// pattern for live pipelines, where a control lands asynchronously and the
// test waits for its effect to reach the collector:
//
//	err := out.Wait(ctx, func(c *goavtest.Collector) bool {
//	    return len(c.Frames()) >= 3
//	})
//
// It returns nil once cond holds and ctx.Err() when the context ends first.
func (c *Collector) Wait(ctx context.Context, cond func(*Collector) bool) error {
	for {
		if cond(c) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// cloneFrame deep-copies a delivered frame: headers by value, plane bytes
// into fresh backing marked immutable (the collector never writes them again).
func cloneFrame(frame *av.Frame) *av.Frame {
	clone := *frame
	if frame.Audio != nil {
		audio := *frame.Audio
		clone.Audio = &audio
	}
	if frame.Video != nil {
		video := *frame.Video
		clone.Video = &video
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		clone.Planes[i].Buffer = cloneBuffer(frame.Planes[i].Buffer)
	}
	clone.Metadata = cloneMetadata(frame.Metadata)
	return &clone
}

// clonePacket deep-copies a delivered packet, payload bytes included.
func clonePacket(packet *av.Packet) *av.Packet {
	clone := *packet
	clone.Payload = cloneBuffer(packet.Payload)
	clone.Metadata = cloneMetadata(packet.Metadata)
	return &clone
}

// cloneEvent copies a delivered event, detaching its Stream and Codec
// pointees and Metadata from the producer.
func cloneEvent(event av.Event) av.Event {
	clone := event
	if event.Stream != nil {
		stream := *event.Stream
		stream.Metadata = cloneMetadata(stream.Metadata)
		stream.Codec = cloneCodecParameters(stream.Codec)
		clone.Stream = &stream
	}
	if event.Codec != nil {
		codecParams := cloneCodecParameters(*event.Codec)
		clone.Codec = &codecParams
	}
	clone.Metadata = cloneMetadata(event.Metadata)
	return clone
}

func cloneCodecParameters(params av.CodecParameters) av.CodecParameters {
	clone := params
	clone.ExtraData = cloneBuffer(params.ExtraData)
	clone.Attributes = cloneMetadata(params.Attributes)
	return clone
}

func cloneBuffer(buffer av.Buffer) av.Buffer {
	if buffer.Bytes == nil {
		return av.Buffer{Ownership: av.BufferImmutable}
	}
	return av.Buffer{
		Bytes:     append([]byte(nil), buffer.Bytes...),
		Ownership: av.BufferImmutable,
	}
}

func cloneMetadata(metadata av.Metadata) av.Metadata {
	if metadata == nil {
		return nil
	}
	clone := make(av.Metadata, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}
