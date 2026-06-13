package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
)

const (
	roomName       = "room.mix"
	roomSampleRate = 48000
	roomChannels   = 1
)

func main() {
	mixed, events, err := runDynamicRoomDemo(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("mixed:", mixed)
	fmt.Println("events:", summarizeRoomEvents(events))
}

func runDynamicRoomDemo(ctx context.Context) ([][]int16, []av.Event, error) {
	return runRoomScript(ctx, func(ctx context.Context, room *Room) error {
		if err := room.Join(ctx, "host"); err != nil {
			return err
		}
		if err := room.Push(ctx, map[string][]int16{
			"host": []int16{100, 100},
		}); err != nil {
			return err
		}
		if err := room.Join(ctx, "music"); err != nil {
			return err
		}
		if err := room.Push(ctx, map[string][]int16{
			"host":  []int16{100, 100},
			"music": []int16{25, -50},
		}); err != nil {
			return err
		}
		if err := room.Join(ctx, "guest"); err != nil {
			return err
		}
		if err := room.Push(ctx, map[string][]int16{
			"host":  []int16{100, 100},
			"music": []int16{25, -50},
			"guest": []int16{-10, 20},
		}); err != nil {
			return err
		}
		if err := room.Leave(ctx, "music"); err != nil {
			return err
		}
		if err := room.Push(ctx, map[string][]int16{
			"host":  []int16{100, 100},
			"guest": []int16{-10, 20},
		}); err != nil {
			return err
		}
		if err := room.Leave(ctx, "guest"); err != nil {
			return err
		}
		if err := room.Push(ctx, map[string][]int16{
			"host": []int16{100, 100},
		}); err != nil {
			return err
		}
		return room.Leave(ctx, "host")
	})
}

func runRoomScript(ctx context.Context, script func(context.Context, *Room) error) ([][]int16, []av.Event, error) {
	room := NewRoom(roomName, roomSampleRate, roomChannels)
	out := goavtest.NewCollector()
	task, err := goav.From(room.Input()).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		return nil, nil, err
	}

	watch := task.Watch()
	var eventMu sync.Mutex
	var events []av.Event
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		for event := range watch {
			eventMu.Lock()
			events = append(events, cloneEvent(event))
			eventMu.Unlock()
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errC := make(chan error, 1)
	go func() {
		errC <- task.Run(runCtx)
	}()

	scriptErr := script(runCtx, room)
	if scriptErr == nil {
		scriptErr = room.Close(runCtx)
	}
	if scriptErr != nil {
		cancel()
		_ = task.Close()
		<-watchDone
		return out.S16(), eventsSnapshot(&eventMu, &events), scriptErr
	}

	runErr := waitRoomTask(ctx, errC, cancel, task)
	_ = task.Close()
	<-watchDone
	return out.S16(), eventsSnapshot(&eventMu, &events), runErr
}

func waitRoomTask(ctx context.Context, errC <-chan error, cancel context.CancelFunc, task goav.Task) error {
	select {
	case err := <-errC:
		return err
	case <-ctx.Done():
		cancel()
		_ = task.Close()
		return ctx.Err()
	}
}

func eventsSnapshot(mu *sync.Mutex, events *[]av.Event) []av.Event {
	mu.Lock()
	defer mu.Unlock()
	out := make([]av.Event, len(*events))
	for i := range *events {
		out[i] = cloneEvent((*events)[i])
	}
	return out
}

// Room is an application-owned live audio room. It is intentionally outside
// the goav root API: the room owns dynamic participant membership, while goav
// sees a stable mixed S16 audio source plus stream lifecycle events.
type Room struct {
	name       string
	sampleRate int
	channels   int
	commands   chan roomCommand
}

func NewRoom(name string, sampleRate int, channels int) *Room {
	if name == "" {
		name = roomName
	}
	if sampleRate <= 0 {
		sampleRate = roomSampleRate
	}
	if channels <= 0 {
		channels = roomChannels
	}
	return &Room{
		name:       name,
		sampleRate: sampleRate,
		channels:   channels,
		commands:   make(chan roomCommand, 64),
	}
}

func (r *Room) Input() goav.InputSpec {
	return goav.Source(r.name,
		shape.Frame(av.MediaAudio,
			shape.Audio(r.sampleRate, r.channels, av.SampleFormatS16),
			shape.Stream(av.StreamID(r.name)),
			shape.Realtime(true),
		),
		func(ctx context.Context, push goav.SourcePush) error {
			active := make(map[string]struct{})
			var elapsed int64
			for {
				select {
				case <-ctx.Done():
					return nil
				case command := <-r.commands:
					err := r.handle(push, active, &elapsed, command)
					command.ack <- err
					if err != nil {
						return err
					}
					if command.kind == roomClose {
						return nil
					}
				}
			}
		})
}

func (r *Room) Join(ctx context.Context, participant string) error {
	return r.dispatch(ctx, roomCommand{kind: roomJoin, participant: strings.TrimSpace(participant)})
}

func (r *Room) Leave(ctx context.Context, participant string) error {
	return r.dispatch(ctx, roomCommand{kind: roomLeave, participant: strings.TrimSpace(participant)})
}

func (r *Room) Push(ctx context.Context, frames map[string][]int16) error {
	return r.dispatch(ctx, roomCommand{kind: roomPush, frames: cloneRoomFrames(frames)})
}

func (r *Room) Close(ctx context.Context) error {
	return r.dispatch(ctx, roomCommand{kind: roomClose})
}

func (r *Room) dispatch(ctx context.Context, command roomCommand) error {
	command.ack = make(chan error, 1)
	select {
	case r.commands <- command:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-command.ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type roomCommandKind int

const (
	roomJoin roomCommandKind = iota + 1
	roomLeave
	roomPush
	roomClose
)

type roomCommand struct {
	kind        roomCommandKind
	participant string
	frames      map[string][]int16
	ack         chan error
}

func (r *Room) handle(push goav.SourcePush, active map[string]struct{}, elapsed *int64, command roomCommand) error {
	switch command.kind {
	case roomJoin:
		return r.join(push, active, command.participant)
	case roomLeave:
		return r.leave(push, active, command.participant)
	case roomPush:
		frame, err := r.mix(active, elapsed, command.frames)
		if err != nil || frame == nil {
			return err
		}
		_, err = push.Frame(frame)
		return err
	case roomClose:
		return push.EOS(av.StreamID(r.name))
	default:
		return fmt.Errorf("unknown room command %d", command.kind)
	}
}

func (r *Room) join(push goav.SourcePush, active map[string]struct{}, participant string) error {
	if participant == "" {
		return fmt.Errorf("participant name is empty")
	}
	if _, ok := active[participant]; ok {
		return fmt.Errorf("participant %q already joined", participant)
	}
	active[participant] = struct{}{}
	_, err := push.Event(av.Event{
		Type:     av.EventStreamAdded,
		StreamID: av.StreamID(participant),
		Stream:   r.participantStream(participant),
		Reason:   "participant joined room mix",
		Metadata: av.Metadata{"room": r.name},
	})
	if err != nil {
		delete(active, participant)
	}
	return err
}

func (r *Room) leave(push goav.SourcePush, active map[string]struct{}, participant string) error {
	if participant == "" {
		return fmt.Errorf("participant name is empty")
	}
	if _, ok := active[participant]; !ok {
		return fmt.Errorf("participant %q is not active", participant)
	}
	delete(active, participant)
	_, err := push.Event(av.Event{
		Type:     av.EventStreamRemoved,
		StreamID: av.StreamID(participant),
		Reason:   "participant left room mix",
		Metadata: av.Metadata{"room": r.name},
	})
	if err != nil {
		active[participant] = struct{}{}
	}
	return err
}

func (r *Room) participantStream(participant string) *av.Stream {
	return &av.Stream{
		ID:   av.StreamID(participant),
		Name: participant,
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:         av.MediaAudio,
			SampleRate:   r.sampleRate,
			Channels:     r.channels,
			SampleFormat: av.SampleFormatS16,
			ClockRate:    uint32(r.sampleRate),
		},
		Metadata: av.Metadata{
			"room": r.name,
		},
	}
}

func (r *Room) mix(active map[string]struct{}, elapsed *int64, frames map[string][]int16) (*av.Frame, error) {
	for participant := range frames {
		if _, ok := active[participant]; !ok {
			return nil, fmt.Errorf("unknown participant frame %q", participant)
		}
	}
	names := activeParticipants(active)
	if len(names) == 0 {
		return nil, nil
	}
	samples := 0
	for _, participant := range names {
		input := frames[participant]
		if len(input)%r.channels != 0 {
			return nil, fmt.Errorf("participant %q frame has %d samples, not divisible by %d channels", participant, len(input), r.channels)
		}
		if len(input) > samples {
			samples = len(input)
		}
	}
	if samples == 0 {
		return nil, nil
	}

	mixed := make([]int16, samples)
	for i := 0; i < samples; i++ {
		var sum int32
		for _, participant := range names {
			input := frames[participant]
			if i >= len(input) {
				continue
			}
			sum += int32(input[i])
		}
		mixed[i] = clampS16(sum)
	}
	frame := s16Frame(r.name, r.sampleRate, r.channels, mixed, *elapsed)
	frame.Metadata = av.Metadata{"active_participants": strings.Join(names, ",")}
	*elapsed += frame.Duration.Value
	return frame, nil
}

func activeParticipants(active map[string]struct{}) []string {
	names := make([]string, 0, len(active))
	for participant := range active {
		names = append(names, participant)
	}
	sort.Strings(names)
	return names
}

func clampS16(value int32) int16 {
	switch {
	case value > 32767:
		return 32767
	case value < -32768:
		return -32768
	default:
		return int16(value)
	}
}

func s16Frame(stream string, sampleRate int, channels int, samples []int16, elapsed int64) *av.Frame {
	payload := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(sample))
	}
	perChannel := len(samples) / channels
	duration := av.SamplesDuration(perChannel, sampleRate)
	return &av.Frame{
		StreamID: av.StreamID(stream),
		Type:     av.MediaAudio,
		PTS:      av.Timestamp{Value: elapsed, Base: duration.Base},
		Duration: duration,
		Audio: &av.AudioFrame{
			SampleRate:   sampleRate,
			Channels:     channels,
			SampleFormat: av.SampleFormatS16,
			Samples:      perChannel,
		},
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
			Stride: channels * 2,
		}},
	}
}

func cloneRoomFrames(frames map[string][]int16) map[string][]int16 {
	if len(frames) == 0 {
		return nil
	}
	out := make(map[string][]int16, len(frames))
	for participant, samples := range frames {
		out[participant] = append([]int16(nil), samples...)
	}
	return out
}

func summarizeRoomEvents(events []av.Event) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		switch event.Type {
		case av.EventStreamAdded, av.EventStreamRemoved, av.EventEndOfStream:
			label := string(event.Type) + ":" + string(event.StreamID)
			if len(out) == 0 || out[len(out)-1] != label {
				out = append(out, label)
			}
		}
	}
	return out
}

func cloneEvent(event av.Event) av.Event {
	clone := event
	if event.Stream != nil {
		stream := *event.Stream
		stream.Metadata = cloneMetadata(stream.Metadata)
		clone.Stream = &stream
	}
	if event.Codec != nil {
		codec := *event.Codec
		clone.Codec = &codec
	}
	clone.Metadata = cloneMetadata(event.Metadata)
	return clone
}

func cloneMetadata(metadata av.Metadata) av.Metadata {
	if len(metadata) == 0 {
		return nil
	}
	out := make(av.Metadata, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}
