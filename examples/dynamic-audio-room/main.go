package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
)

const (
	roomName       = "room"
	roomSampleRate = 48000
	roomChannels   = 1
)

func main() {
	result, err := runDynamicRoomDemo(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("per_track:", result.PerTrackSummary())
	fmt.Println("mixed:", result.Mixed)
	fmt.Println("events:", result.Events)
}

type DemoResult struct {
	PerTrack map[string][][]int16
	Meter    map[string]int
	Mixed    [][]int16
	Events   []string
}

func (r DemoResult) PerTrackSummary() []string {
	names := make([]string, 0, len(r.PerTrack))
	for name := range r.PerTrack {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, fmt.Sprintf("%s:%v", name, r.PerTrack[name]))
	}
	return out
}

func runDynamicRoomDemo(ctx context.Context) (DemoResult, error) {
	return runRoomScript(ctx, func(ctx context.Context, room *RoomPipeline) error {
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

		if err := room.Leave(ctx, "host"); err != nil {
			return err
		}
		return nil
	})
}

func runRoomScript(ctx context.Context, script func(context.Context, *RoomPipeline) error) (DemoResult, error) {
	room := NewRoom(roomName, roomSampleRate, roomChannels)
	input := room.Input()
	meter := NewTrackMeter()
	roomSync := goav.Sync("room", goav.SyncTolerance(20*time.Millisecond), goav.SyncDropLate())
	meterStage := goav.FrameFunc("track-meter", func(_ context.Context, frame *av.Frame, emit goav.Emit) error {
		meter.Observe(frame)
		return emit.Frame(frame)
	})
	tracks := NewTrackRecorder()
	mixer := NewOutputMixer()
	main := goav.Sink(goav.SinkFunc("room-anchor", func(context.Context, goav.Message) error { return nil }))

	task, err := goav.From(input).
		Audio().
		To(main).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		return DemoResult{}, err
	}
	roomPipeline := &RoomPipeline{
		room:        room,
		task:        task,
		attachments: make(map[string]goav.Attachment),
		attach: func(ctx context.Context, participant string) (goav.Attachment, error) {
			stream := *room.participantStream(participant)
			anchor := input.Stream(stream)
			return task.Attach(ctx,
				goav.Branch("track-"+participant).
					From(anchor).
					Sync(roomSync).
					Do(meterStage).
					To(tracks.Sink()),
				goav.Branch("mix-"+participant).
					From(anchor).
					Sync(roomSync).
					To(mixer.Sink()),
			)
		},
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
	if err := waitRoomReady(ctx, room, errC, cancel, task); err != nil {
		_ = task.Close()
		<-watchDone
		return roomResult(tracks, meter, mixer, &eventMu, &events), err
	}

	scriptErr := script(runCtx, roomPipeline)
	if scriptErr == nil {
		scriptErr = roomPipeline.Close(runCtx)
	}
	if scriptErr != nil {
		cancel()
		_ = task.Close()
		<-watchDone
		diagnostics := roomEventDiagnostics(eventsSnapshot(&eventMu, &events))
		result := roomResult(tracks, meter, mixer, &eventMu, &events)
		return result, withRoomDiagnostics(scriptErr, diagnostics)
	}

	runErr := waitRoomTask(ctx, errC, cancel, task)
	_ = task.Close()
	<-watchDone
	return roomResult(tracks, meter, mixer, &eventMu, &events), runErr
}

type RoomPipeline struct {
	room        *Room
	task        goav.Task
	attachments map[string]goav.Attachment
	attach      func(context.Context, string) (goav.Attachment, error)
}

func (p *RoomPipeline) Join(ctx context.Context, participant string) error {
	participant = strings.TrimSpace(participant)
	if participant == "" {
		return fmt.Errorf("participant name is empty")
	}
	if _, ok := p.attachments[participant]; ok {
		return fmt.Errorf("participant %q already joined", participant)
	}
	attachment, err := p.attach(ctx, participant)
	if err != nil {
		return err
	}
	if err := p.room.Join(ctx, participant); err != nil {
		_ = p.task.Detach(ctx, attachment)
		return err
	}
	p.attachments[participant] = attachment
	return nil
}

func (p *RoomPipeline) Leave(ctx context.Context, participant string) error {
	participant = strings.TrimSpace(participant)
	if participant == "" {
		return fmt.Errorf("participant name is empty")
	}
	attachment, ok := p.attachments[participant]
	if !ok {
		return fmt.Errorf("participant %q is not active", participant)
	}
	if err := p.room.Leave(ctx, participant); err != nil {
		return err
	}
	if err := p.task.Detach(ctx, attachment); err != nil {
		return err
	}
	delete(p.attachments, participant)
	return nil
}

func (p *RoomPipeline) Push(ctx context.Context, frames map[string][]int16) error {
	return p.room.Push(ctx, frames)
}

func (p *RoomPipeline) Close(ctx context.Context) error {
	return p.room.Close(ctx)
}

func waitRoomReady(ctx context.Context, room *Room, errC <-chan error, cancel context.CancelFunc, task goav.Task) error {
	select {
	case <-room.Ready():
		return nil
	case err := <-errC:
		if err == nil {
			return fmt.Errorf("room task stopped before source ready")
		}
		return fmt.Errorf("room task stopped before source ready: %w", err)
	case <-ctx.Done():
		cancel()
		_ = task.Close()
		return fmt.Errorf("room source did not start: %w", ctx.Err())
	}
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

func roomResult(tracks *TrackRecorder, meter *TrackMeter, mixer *OutputMixer, eventMu *sync.Mutex, events *[]av.Event) DemoResult {
	return DemoResult{
		PerTrack: tracks.Frames(),
		Meter:    meter.Counts(),
		Mixed:    mixer.Mixed(),
		Events:   summarizeRoomEvents(eventsSnapshot(eventMu, events)),
	}
}

func withRoomDiagnostics(err error, events []string) error {
	if err == nil || len(events) == 0 {
		return err
	}
	return fmt.Errorf("%w; room events: %v", err, events)
}

func roomEventDiagnostics(events []av.Event) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		label := string(event.Type)
		if event.StreamID != "" {
			label += ":" + string(event.StreamID)
		}
		if event.Reason != "" {
			label += " reason=" + event.Reason
		}
		if event.Cause != nil {
			label += " cause=" + event.Cause.Error()
		}
		if len(out) == 0 || out[len(out)-1] != label {
			out = append(out, label)
		}
	}
	return out
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

// Room is an application-owned source of dynamic tracks. It does not mix at
// input time: every participant is announced as its own stream, and runtime
// branches decide whether to process the track independently or mix it later.
type Room struct {
	name       string
	sampleRate int
	channels   int
	commands   chan roomCommand
	ready      chan struct{}
	readyOnce  sync.Once
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
		ready:      make(chan struct{}),
	}
}

func (r *Room) Input() goav.InputSpec {
	return goav.Source(r.name,
		shape.Frame(av.MediaAudio,
			shape.Audio(r.sampleRate, r.channels, av.SampleFormatS16),
			shape.Stream(av.StreamID(r.controlStream())),
			shape.Realtime(true),
		),
		func(ctx context.Context, push goav.SourcePush) error {
			r.markReady()
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

func (r *Room) Ready() <-chan struct{} {
	return r.ready
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
		return fmt.Errorf("queue %s command: %w", command.kind, ctx.Err())
	}
	select {
	case err := <-command.ack:
		return err
	case <-ctx.Done():
		return fmt.Errorf("room source did not acknowledge %s command: %w", command.kind, ctx.Err())
	}
}

func (r *Room) markReady() {
	r.readyOnce.Do(func() { close(r.ready) })
}

func (r *Room) controlStream() string {
	return r.name + ".control"
}

type roomCommandKind int

const (
	roomJoin roomCommandKind = iota + 1
	roomLeave
	roomPush
	roomClose
)

func (k roomCommandKind) String() string {
	switch k {
	case roomJoin:
		return "join"
	case roomLeave:
		return "leave"
	case roomPush:
		return "push"
	case roomClose:
		return "close"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}

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
		return r.pushTrackFrames(push, active, elapsed, command.frames)
	case roomClose:
		return push.EOS(av.StreamID(r.controlStream()))
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
		Reason:   "participant joined room",
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
		Reason:   "participant left room",
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
		TimeBase: av.TimeBase{Num: 1, Den: int64(r.sampleRate)},
		Metadata: av.Metadata{
			"room": r.name,
		},
	}
}

func (r *Room) pushTrackFrames(push goav.SourcePush, active map[string]struct{}, elapsed *int64, frames map[string][]int16) error {
	for participant := range frames {
		if _, ok := active[participant]; !ok {
			return fmt.Errorf("unknown participant frame %q", participant)
		}
	}
	names := activeParticipants(active)
	if len(names) == 0 {
		return nil
	}
	samples := 0
	for _, participant := range names {
		input := frames[participant]
		if len(input)%r.channels != 0 {
			return fmt.Errorf("participant %q frame has %d samples, not divisible by %d channels", participant, len(input), r.channels)
		}
		if len(input) > samples {
			samples = len(input)
		}
	}
	if samples == 0 {
		return nil
	}

	activeList := strings.Join(names, ",")
	for _, participant := range names {
		samplesForParticipant := padS16(frames[participant], samples)
		frame := s16Frame(participant, r.sampleRate, r.channels, samplesForParticipant, *elapsed)
		frame.Metadata = av.Metadata{
			"active_participants": activeList,
			"room":                r.name,
		}
		if _, err := push.Frame(frame); err != nil {
			return err
		}
	}
	duration := av.SamplesDuration(samples/r.channels, r.sampleRate)
	*elapsed += duration.Value
	return nil
}

func activeParticipants(active map[string]struct{}) []string {
	names := make([]string, 0, len(active))
	for participant := range active {
		names = append(names, participant)
	}
	sort.Strings(names)
	return names
}

type TrackMeter struct {
	mu     sync.Mutex
	counts map[string]int
}

func NewTrackMeter() *TrackMeter {
	return &TrackMeter{counts: make(map[string]int)}
}

func (m *TrackMeter) Observe(frame *av.Frame) {
	m.mu.Lock()
	m.counts[string(frame.StreamID)] += len(readS16(frame))
	m.mu.Unlock()
}

func (m *TrackMeter) Counts() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.counts))
	for stream, count := range m.counts {
		out[stream] = count
	}
	return out
}

type TrackRecorder struct {
	dest goav.Destination

	mu     sync.Mutex
	frames map[string][][]int16
}

func NewTrackRecorder() *TrackRecorder {
	r := &TrackRecorder{frames: make(map[string][][]int16)}
	r.dest = goav.Sink(goav.SinkFunc("per-track-output", r.handle))
	return r
}

func (r *TrackRecorder) Sink() goav.Destination {
	return r.dest
}

func (r *TrackRecorder) handle(_ context.Context, msg goav.Message) error {
	if msg.Frame == nil {
		return nil
	}
	stream := string(msg.Frame.StreamID)
	samples := readS16(msg.Frame)
	r.mu.Lock()
	r.frames[stream] = append(r.frames[stream], samples)
	r.mu.Unlock()
	return nil
}

func (r *TrackRecorder) Frames() map[string][][]int16 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][][]int16, len(r.frames))
	for stream, frames := range r.frames {
		out[stream] = cloneS16Frames(frames)
	}
	return out
}

type OutputMixer struct {
	dest goav.Destination

	mu        sync.Mutex
	pending   map[int64]*mixBucket
	completed map[int64][]int16
}

type mixBucket struct {
	expected map[string]struct{}
	frames   map[string][]int16
}

func NewOutputMixer() *OutputMixer {
	m := &OutputMixer{
		pending:   make(map[int64]*mixBucket),
		completed: make(map[int64][]int16),
	}
	m.dest = goav.Sink(goav.SinkFunc("room-mix-output", m.handle))
	return m
}

func (m *OutputMixer) Sink() goav.Destination {
	return m.dest
}

func (m *OutputMixer) handle(_ context.Context, msg goav.Message) error {
	if msg.Frame == nil {
		return nil
	}
	expected := participantsFromMetadata(msg.Frame.Metadata)
	if len(expected) == 0 {
		expected = []string{string(msg.Frame.StreamID)}
	}
	samples := readS16(msg.Frame)
	pts := msg.Frame.PTS.Value

	m.mu.Lock()
	defer m.mu.Unlock()
	bucket := m.pending[pts]
	if bucket == nil {
		bucket = &mixBucket{expected: make(map[string]struct{}, len(expected)), frames: make(map[string][]int16)}
		m.pending[pts] = bucket
	}
	for _, participant := range expected {
		bucket.expected[participant] = struct{}{}
	}
	bucket.frames[string(msg.Frame.StreamID)] = samples
	if len(bucket.frames) < len(bucket.expected) {
		return nil
	}
	m.completed[pts] = mixFrames(bucket.expected, bucket.frames)
	delete(m.pending, pts)
	return nil
}

func (m *OutputMixer) Mixed() [][]int16 {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]int64, 0, len(m.completed))
	for pts := range m.completed {
		keys = append(keys, pts)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([][]int16, 0, len(keys))
	for _, pts := range keys {
		out = append(out, append([]int16(nil), m.completed[pts]...))
	}
	return out
}

func participantsFromMetadata(metadata av.Metadata) []string {
	if len(metadata) == 0 || metadata["active_participants"] == "" {
		return nil
	}
	parts := strings.Split(metadata["active_participants"], ",")
	out := parts[:0]
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	sort.Strings(out)
	return out
}

func mixFrames(expected map[string]struct{}, frames map[string][]int16) []int16 {
	length := 0
	for participant := range expected {
		if len(frames[participant]) > length {
			length = len(frames[participant])
		}
	}
	mixed := make([]int16, length)
	for i := 0; i < length; i++ {
		var sum int32
		for participant := range expected {
			input := frames[participant]
			if i < len(input) {
				sum += int32(input[i])
			}
		}
		mixed[i] = clampS16(sum)
	}
	return mixed
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

func padS16(samples []int16, length int) []int16 {
	out := make([]int16, length)
	copy(out, samples)
	return out
}

func readS16(frame *av.Frame) []int16 {
	if frame == nil || frame.Audio == nil || frame.Audio.SampleFormat != av.SampleFormatS16 || len(frame.Planes) == 0 {
		return nil
	}
	payload := frame.Planes[0].Buffer.Bytes
	samples := make([]int16, len(payload)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(payload[i*2:]))
	}
	return samples
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

func cloneS16Frames(frames [][]int16) [][]int16 {
	out := make([][]int16, len(frames))
	for i := range frames {
		out[i] = append([]int16(nil), frames[i]...)
	}
	return out
}

func summarizeRoomEvents(events []av.Event) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		switch event.Type {
		case av.EventStreamAdded, av.EventStreamRemoved, av.EventEndOfStream:
			if event.Type == av.EventEndOfStream && event.StreamID == "" {
				continue
			}
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
