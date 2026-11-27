package stage

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

// audioMixStage sums N synchronized audio inputs into one output stream — the
// runtime heart of a convergent Mix. Each input arm is identified by its frame
// StreamID. It is a normal pipeline.Stage: the buffered runner calls Handle
// serially per node, so the per-input queues need no locking (lock-free by
// design — the hot path takes no mutex).
type AudioMix struct {
	joinStageBase
	inputs [][]byte
	audio  av.AudioFrame
}

func NewAudioMix(name string, inputs []av.StreamID, out av.StreamID, mode compile.JoinSyncMode) *AudioMix {
	return NewAudioMixWithPrealloc(name, inputs, out, mode, 1)
}

func NewAudioMixWithPrealloc(name string, inputs []av.StreamID, out av.StreamID, mode compile.JoinSyncMode, preallocDepth int) *AudioMix {
	return NewAudioMixWithPreallocAndPools(name, inputs, out, mode, preallocDepth, nil)
}

func NewAudioMixWithPreallocAndPools(name string, inputs []av.StreamID, out av.StreamID, mode compile.JoinSyncMode, preallocDepth int, pools *MediaPools) *AudioMix {
	if preallocDepth < 1 {
		preallocDepth = 1
	}
	stage := &AudioMix{
		joinStageBase: joinStageBase{
			name:  name,
			out:   out,
			sync:  NewJoinSyncStateWithPendingCap(mode, inputs, preallocDepth),
			free:  make([][]*av.Frame, len(inputs)),
			pools: pools,
		},
		inputs: make([][]byte, 0, len(inputs)),
	}
	stage.output.Planes = takeMediaPlanes(pools, 1)
	stage.sync.recycle = stage.recycleArmFrame
	for i := range inputs {
		free := make([]*av.Frame, 0, preallocDepth)
		for range preallocDepth {
			free = append(free, &av.Frame{
				Audio:  &av.AudioFrame{},
				Planes: takeMediaPlanes(pools, 1),
			})
		}
		stage.free[i] = free
	}
	return stage
}

func (s *AudioMix) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if s.closed {
		return pipeline.ErrClosed
	}
	var err error
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame != nil && msg.Frame.Audio != nil {
			if armIdx := s.sync.armIndex(msg.Frame.StreamID); armIdx >= 0 {
				if !s.sync.eosEmitted && !s.sync.armStates[armIdx].eos {
					s.sync.bufferAt(armIdx, s.cloneMixFrame(armIdx, msg.Frame))
				}
			}
			err = s.drain(ctx, emitter)
		}
	case pipeline.MessageEvent:
		err = s.handleEvent(ctx, msg, emitter, s.drain)
	}
	s.sync.publishDepths()
	return err
}

func (s *AudioMix) drain(ctx context.Context, emitter pipeline.Emitter) error {
	for {
		frames, ref, ok := s.sync.step()
		if !ok {
			return nil
		}
		mixed, err := mixS16FramesInto(frames, s.out, &s.output, &s.audio, &s.inputs)
		if err != nil {
			s.recycleStepFrames(frames)
			return err
		}
		// The step's timing reference stamps the output (in arrival mode this
		// is the first arm, exactly as before).
		mixed.PTS = frames[ref].PTS
		mixed.Duration = frames[ref].Duration
		s.recycleStepFrames(frames)
		s.message.Kind = pipeline.MessageFrame
		s.message.Packet = nil
		s.message.Frame = mixed
		s.message.Event = nil
		err = emitter.Emit(ctx, &s.message)
		// The mixed buffer is the stage's own; downstream keeps it by reference.
		mixed.Release()
		if err != nil {
			return err
		}
	}
}

// mixS16FramesInto sums S16-interleaved audio frames sample-by-sample with
// clamping into a new pooled plane in dst, which the caller releases.
func mixS16FramesInto(frames []*av.Frame, out av.StreamID, dst *av.Frame, audio *av.AudioFrame, inputsBuf *[][]byte) (*av.Frame, error) {
	base := firstMixFrame(frames)
	if base == nil {
		return nil, fmt.Errorf("goav: audio mix has no participating frames")
	}
	if base.Audio == nil || len(base.Planes) == 0 {
		return nil, fmt.Errorf("goav: mix arm %q delivered a frame with no audio plane", base.StreamID)
	}
	n := -1
	inputs := *inputsBuf
	if cap(inputs) < len(frames) {
		inputs = make([][]byte, 0, len(frames))
	} else {
		inputs = inputs[:0]
	}
	for i := range frames {
		f := frames[i]
		if f == nil {
			continue
		}
		if f.Audio == nil || len(f.Planes) == 0 {
			return nil, fmt.Errorf("goav: mix arm %q delivered a frame with no audio plane", f.StreamID)
		}
		if f.Audio.SampleFormat != av.SampleFormatS16 {
			return nil, fmt.Errorf("goav: audio mix requires %s, got %s on arm %q", av.SampleFormatS16, f.Audio.SampleFormat, f.StreamID)
		}
		if f.Audio.Channels != base.Audio.Channels || f.Audio.SampleRate != base.Audio.SampleRate {
			return nil, fmt.Errorf("goav: audio mix arms differ (arm %q is %d ch/%d Hz, arm %q is %d ch/%d Hz)",
				f.StreamID, f.Audio.Channels, f.Audio.SampleRate, base.StreamID, base.Audio.Channels, base.Audio.SampleRate)
		}
		raw := f.Planes[0].Buffer.Bytes
		off := f.Planes[0].Offset
		if off < 0 || off > len(raw) {
			return nil, fmt.Errorf("goav: mix arm %q audio plane offset %d out of range [0,%d]", f.StreamID, off, len(raw))
		}
		pcm := raw[off:]
		if n < 0 || len(pcm) < n {
			n = len(pcm)
		}
		inputs = append(inputs, pcm)
	}
	*inputsBuf = inputs
	if n < 0 {
		n = 0
	}
	channels := max(base.Audio.Channels, 1)
	frameBytes := channels * 2
	n -= n % frameBytes // whole multi-channel int16 frames
	if cap(dst.Planes) < 1 {
		dst.Planes = make([]av.Plane, 1)
	}
	dst.Planes = dst.Planes[:1]
	mixed := av.NewBuffer(n)
	MixS16Into(mixed.Bytes, inputs, n)
	*audio = *base.Audio
	audio.Samples = n / frameBytes
	dst.StreamID = out
	dst.Type = av.MediaAudio
	dst.PTS = base.PTS
	dst.Duration = base.Duration
	dst.Audio = audio
	dst.Video = nil
	dst.Metadata = nil
	dst.Planes[0] = av.Plane{
		Buffer: mixed,
		Stride: frameBytes,
		Offset: 0,
	}
	return dst, nil
}

func firstMixFrame(frames []*av.Frame) *av.Frame {
	for i := range frames {
		if frames[i] != nil {
			return frames[i]
		}
	}
	return nil
}

// cloneMixFrame keeps an arm frame past the call that lent it: a pooled
// header with the frame's fields and its own reference to each plane.
func (s *AudioMix) cloneMixFrame(armIdx int, frame *av.Frame) *av.Frame {
	clone := s.takeMixFrame(armIdx)
	planes := clone.Planes
	audio := clone.Audio
	*clone = *frame
	if frame.Audio != nil {
		if audio == nil {
			audio = &av.AudioFrame{}
		}
		*audio = *frame.Audio
	}
	clone.Audio = audio
	clone.Video = nil
	clone.Planes = append(planes[:0], frame.Planes...)
	for i := range clone.Planes {
		clone.Planes[i].Buffer.Retain()
	}
	return clone
}

func (s *AudioMix) takeMixFrame(armIdx int) *av.Frame {
	free := s.free[armIdx]
	if len(free) == 0 {
		return &av.Frame{
			Audio:  &av.AudioFrame{},
			Planes: takeMediaPlanes(s.pools, 1),
		}
	}
	index := len(free) - 1
	frame := free[index]
	free[index] = nil
	s.free[armIdx] = free[:index]
	return frame
}
