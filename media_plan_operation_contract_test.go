package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestPlanOperationSpecsContracts(t *testing.T) {
	tests := []struct {
		name            string
		input           inputIntent
		stream          streamIntent
		branch          string
		initial         shape.Spec
		selectComponent string
		wantKinds       []plan.OperationKind
		wantComponents  []string
		wantCodes       []string
	}{
		{
			name:   "custom frame source selects without input reader",
			input:  inputIntent{Name: "virtual", Protocol: av.ProtocolCustom, Codec: codec.Opus()},
			stream: streamIntent{Select: plan.StreamSelect{Type: av.MediaAudio}},
			branch: "voice",
			initial: shape.Frame(av.MediaAudio,
				shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
			),
			selectComponent: "declared-select",
			wantKinds:       []plan.OperationKind{plan.OpSelect},
			wantComponents:  []string{"declared-select"},
			wantCodes:       []string{string(errcode.FrameSource)},
		},
		{
			name: "packet file source keeps packet copy when no frame work is requested",
			input: inputIntent{
				Name:     "movie",
				URI:      "file.webm",
				Protocol: av.ProtocolFile,
				Codec:    codec.VP8(),
			},
			stream:         streamIntent{Select: plan.StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8}},
			branch:         "video",
			initial:        shape.Packet(av.MediaVideo, av.CodecVP8),
			wantKinds:      []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpCopy},
			wantComponents: []string{"container", "video", "packet-copy"},
			wantCodes:      []string{string(errcode.PacketCopy)},
		},
		{
			name: "realtime source depacketizes before packet copy",
			input: inputIntent{
				Name:     "mic",
				Protocol: av.ProtocolRTP,
				Codec:    codec.Opus(),
				Realtime: true,
			},
			stream:         streamIntent{Select: plan.StreamSelect{Type: av.MediaAudio, Name: "floor"}},
			branch:         "live-voice",
			initial:        shape.Packet(av.MediaAudio, av.CodecOpus, shape.Realtime(true)),
			wantKinds:      []plan.OperationKind{plan.OpDepacketize, plan.OpSelect, plan.OpCopy},
			wantComponents: []string{"opus", "floor", "packet-copy"},
			wantCodes:      []string{string(errcode.PacketCopy)},
		},
		{
			name: "declared decode transform encode operations report frame and packet requirements",
			input: inputIntent{
				Name:     "voice",
				Protocol: av.ProtocolFile,
				Codec:    codec.Opus(),
			},
			stream: streamIntent{
				Name:   "voice",
				Select: plan.StreamSelect{Type: av.MediaAudio},
				Operations: []operationSpec{
					operationSpecForDecode(codec.Opus(), "custom-opus-decoder"),
					operationSpecForTransform(Resample(16_000, codec.Mono)),
					operationSpecForEncode(codec.Opus(codec.Bitrate(64_000))),
				},
			},
			branch:          "voice",
			initial:         shape.Packet(av.MediaAudio, av.CodecOpus),
			selectComponent: "select-voice",
			wantKinds: []plan.OperationKind{
				plan.OpDemux,
				plan.OpSelect,
				plan.OpDecode,
				plan.OpTransform,
				plan.OpEncode,
			},
			wantComponents: []string{"container", "select-voice", "opus", filter.FactoryResample, "opus"},
			wantCodes:      []string{string(errcode.DecodeRequired), string(errcode.EncodeRequired)},
		},
		{
			name: "declared copy operation reports packet preservation",
			input: inputIntent{
				Name:     "music",
				Protocol: av.ProtocolFile,
				Codec:    codec.Opus(),
			},
			stream: streamIntent{
				Select:     plan.StreamSelect{Type: av.MediaAudio},
				Operations: []operationSpec{operationSpecForCopy(codec.Copy())},
			},
			branch:         "audio-copy",
			initial:        shape.Packet(av.MediaAudio, av.CodecOpus),
			wantKinds:      []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpCopy},
			wantComponents: []string{"container", "audio", "packet-copy"},
			wantCodes:      []string{string(errcode.PacketCopy)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operations, decisions := planOperationSpecs(tt.input, tt.stream, tt.branch, tt.initial, tt.selectComponent)
			if got := planOperationKinds(operations); !reflect.DeepEqual(got, tt.wantKinds) {
				t.Fatalf("operation kinds = %#v, want %#v", got, tt.wantKinds)
			}
			if got := planOperationComponents(operations); !reflect.DeepEqual(got, tt.wantComponents) {
				t.Fatalf("operation components = %#v, want %#v", got, tt.wantComponents)
			}
			if got := planDecisionCodes(decisions); !reflect.DeepEqual(got, tt.wantCodes) {
				t.Fatalf("decision codes = %#v, want %#v", got, tt.wantCodes)
			}
			for _, decision := range decisions {
				if decision.Branch != tt.branch {
					t.Fatalf("decision branch = %q, want %q", decision.Branch, tt.branch)
				}
				if decision.Message == "" {
					t.Fatalf("decision %q has empty message", decision.Code)
				}
			}
		})
	}
}

func TestPlanOperationSpecsInputShapes(t *testing.T) {
	fileInput := inputIntent{
		Name:     "camera-file",
		Protocol: av.ProtocolFile,
		Codec:    codec.VP8(),
	}
	fileOps, _ := planOperationSpecs(
		fileInput,
		streamIntent{Select: plan.StreamSelect{Type: av.MediaVideo}},
		"camera",
		shape.Packet(av.MediaVideo, av.CodecVP8),
		"",
	)
	if got := fileOps[0].Shape; got.Domain != shape.DomainPacket ||
		got.MediaKind != av.MediaVideo ||
		got.StreamID != av.StreamID(fileInput.Name) ||
		got.Codec != av.CodecVP8 {
		t.Fatalf("file input shape = %#v", got)
	}

	liveInput := inputIntent{
		Name:     "live-mic",
		Protocol: av.ProtocolRTP,
		Codec:    codec.Opus(),
		Realtime: true,
	}
	liveOps, _ := planOperationSpecs(
		liveInput,
		streamIntent{Select: plan.StreamSelect{Type: av.MediaAudio}},
		"live",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Realtime(true)),
		"",
	)
	if got := liveOps[0].Shape; got.Domain != shape.DomainPacket ||
		got.MediaKind != av.MediaAudio ||
		got.StreamID != av.StreamID(liveInput.Name) ||
		got.Codec != av.CodecOpus ||
		!got.Realtime {
		t.Fatalf("live input shape = %#v", got)
	}
}

func planOperationKinds(operations []planOperation) []plan.OperationKind {
	kinds := make([]plan.OperationKind, 0, len(operations))
	for i := range operations {
		kinds = append(kinds, operations[i].Kind)
	}
	return kinds
}

func planOperationComponents(operations []planOperation) []string {
	components := make([]string, 0, len(operations))
	for i := range operations {
		components = append(components, operations[i].Component)
	}
	return components
}

func planDecisionCodes(decisions []planDecision) []string {
	codes := make([]string, 0, len(decisions))
	for i := range decisions {
		codes = append(codes, decisions[i].Code)
	}
	return codes
}
