package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestPlanShapeForOperationContracts(t *testing.T) {
	current := shape.Packet(av.MediaAudio, av.CodecOpus, shape.Stream("voice"))
	explicit := shape.Frame(av.MediaAudio, shape.Audio(16_000, 1, av.SampleFormatF32))
	if got := planShapeForOperation(current, planBranch{Name: "voice"}, planOperation{
		Kind:  plan.OpTransform,
		Shape: explicit,
	}); !reflect.DeepEqual(got, explicit) {
		t.Fatalf("explicit operation shape = %#v, want %#v", got, explicit)
	}

	decoded := planShapeForOperation(current, planBranch{Name: "voice"}, planOperation{Kind: plan.OpDecode})
	if decoded.Domain != shape.DomainFrame ||
		decoded.MediaKind != av.MediaAudio ||
		decoded.StreamID != av.StreamID("voice") ||
		decoded.Codec != av.CodecOpus {
		t.Fatalf("decoded shape = %#v", decoded)
	}

	encoded := planShapeForOperation(
		shape.Frame(av.MediaVideo, shape.Stream("camera")),
		planBranch{Name: "preview"},
		planOperation{Kind: plan.OpEncode, Component: string(av.CodecVP8)},
	)
	if encoded.Domain != shape.DomainPacket ||
		encoded.MediaKind != av.MediaVideo ||
		encoded.StreamID != av.StreamID("preview") ||
		encoded.Codec != av.CodecVP8 {
		t.Fatalf("encoded shape = %#v", encoded)
	}
}

func TestPlanBranchDestinationsContracts(t *testing.T) {
	outputs := []planOutput{{Name: "web"}, {Name: "archive"}}
	fromOutputs := planBranchDestinations(nil, outputs)
	if !reflect.DeepEqual(fromOutputs, []string{"web", "archive"}) {
		t.Fatalf("destinations from outputs = %#v", fromOutputs)
	}
	fromOutputs[0] = "mutated"
	if outputs[0].Name != "web" {
		t.Fatalf("planBranchDestinations mutated output name to %q", outputs[0].Name)
	}

	refs := []string{"preview", "mobile"}
	fromRefs := planBranchDestinations(refs, outputs)
	if !reflect.DeepEqual(fromRefs, refs) {
		t.Fatalf("destinations from refs = %#v, want %#v", fromRefs, refs)
	}
	fromRefs[0] = "mutated"
	if refs[0] != "preview" {
		t.Fatalf("planBranchDestinations reused target ref backing array: %#v", refs)
	}
}

func TestFirstInputContracts(t *testing.T) {
	if got := firstInput(nil); !reflect.DeepEqual(got, inputIntent{}) {
		t.Fatalf("firstInput(nil) = %#v, want zero input", got)
	}
	inputs := []inputIntent{
		{Name: "camera", URI: "file:///camera.webm", Protocol: av.ProtocolFile},
		{Name: "mic", Protocol: av.ProtocolRTP, Realtime: true},
	}
	if got := firstInput(inputs); !reflect.DeepEqual(got, inputs[0]) {
		t.Fatalf("firstInput = %#v, want %#v", got, inputs[0])
	}

	nameTests := []struct {
		name   string
		inputs []inputIntent
		want   string
	}{
		{name: "nil", want: "input"},
		{name: "explicit name", inputs: []inputIntent{{Name: "camera", URI: "file:///camera.webm"}}, want: "camera"},
		{name: "uri fallback", inputs: []inputIntent{{URI: "file:///camera.webm"}}, want: "file:///camera.webm"},
		{name: "default fallback", inputs: []inputIntent{{}}, want: "input"},
	}
	for _, tt := range nameTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstInputName(tt.inputs); got != tt.want {
				t.Fatalf("firstInputName = %q, want %q", got, tt.want)
			}
		})
	}
}
