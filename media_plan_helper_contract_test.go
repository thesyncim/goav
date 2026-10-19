package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestPlanShapeForOperationContracts(t *testing.T) {
	current := shape.Packet(av.MediaAudio, av.CodecOpus, shape.Stream("voice"))
	explicit := shape.Frame(av.MediaAudio, shape.Audio(16_000, 1, av.SampleFormatF32))
	if got := compile.PlanShapeForOperation(current, compile.PlanBranch{Name: "voice"}, compile.PlanOperation{
		Kind:  plan.OpTransform,
		Shape: explicit,
	}); !reflect.DeepEqual(got, explicit) {
		t.Fatalf("explicit operation shape = %#v, want %#v", got, explicit)
	}

	decoded := compile.PlanShapeForOperation(current, compile.PlanBranch{Name: "voice"}, compile.PlanOperation{Kind: plan.OpDecode})
	if decoded.Domain != shape.DomainFrame ||
		decoded.MediaKind != av.MediaAudio ||
		decoded.StreamID != av.StreamID("voice") ||
		decoded.Codec != av.CodecOpus {
		t.Fatalf("decoded shape = %#v", decoded)
	}

	encoded := compile.PlanShapeForOperation(
		shape.Frame(av.MediaVideo, shape.Stream("camera")),
		compile.PlanBranch{Name: "preview"},
		compile.PlanOperation{Kind: plan.OpEncode, Component: string(av.CodecVP8)},
	)
	if encoded.Domain != shape.DomainPacket ||
		encoded.MediaKind != av.MediaVideo ||
		encoded.StreamID != av.StreamID("preview") ||
		encoded.Codec != av.CodecVP8 {
		t.Fatalf("encoded shape = %#v", encoded)
	}
}

func TestPlanBranchDestinationsContracts(t *testing.T) {
	outputs := []compile.PlanOutput{{Name: "web"}, {Name: "archive"}}
	fromOutputs := compile.PlanBranchDestinations(nil, outputs)
	if !reflect.DeepEqual(fromOutputs, []string{"web", "archive"}) {
		t.Fatalf("destinations from outputs = %#v", fromOutputs)
	}
	fromOutputs[0] = "mutated"
	if outputs[0].Name != "web" {
		t.Fatalf("planBranchDestinations mutated output name to %q", outputs[0].Name)
	}

	refs := []string{"preview", "mobile"}
	fromRefs := compile.PlanBranchDestinations(refs, outputs)
	if !reflect.DeepEqual(fromRefs, refs) {
		t.Fatalf("destinations from refs = %#v, want %#v", fromRefs, refs)
	}
	fromRefs[0] = "mutated"
	if refs[0] != "preview" {
		t.Fatalf("planBranchDestinations reused target ref backing array: %#v", refs)
	}
}
