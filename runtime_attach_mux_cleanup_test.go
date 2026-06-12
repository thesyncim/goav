package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type runtimeAttachMuxCleanupMuxer struct {
	closed int
	failed bool
}

func (m *runtimeAttachMuxCleanupMuxer) Format() av.FormatID { return av.FormatID("cleanup") }
func (m *runtimeAttachMuxCleanupMuxer) Open(context.Context, format.Output, []av.Stream, format.OpenOptions) error {
	return nil
}
func (m *runtimeAttachMuxCleanupMuxer) Write(context.Context, *av.Packet, *format.WriteResult) error {
	return nil
}
func (m *runtimeAttachMuxCleanupMuxer) Close() error {
	m.closed++
	return nil
}
func (m *runtimeAttachMuxCleanupMuxer) MarkFailed() {
	m.failed = true
}

func runtimeAttachMuxCleanupStage(t *testing.T, name string, muxer *runtimeAttachMuxCleanupMuxer) *format.MuxStage {
	t.Helper()
	stage, err := format.NewMuxStage(format.MuxStageConfig{Name: name, Muxer: muxer})
	if err != nil {
		t.Fatal(err)
	}
	return stage
}

func TestRuntimeAttachGroupSharedMuxCleanupContracts(t *testing.T) {
	var nilGroup *runtimeAttachGroup
	nilGroup.failSharedMuxStages()
	nilGroup.closeSharedMuxStages()

	openMuxer := &runtimeAttachMuxCleanupMuxer{}
	attachedMuxer := &runtimeAttachMuxCleanupMuxer{}
	group := &runtimeAttachGroup{
		muxOrder: []string{"nil-target", "open", "attached", "empty"},
		sharedMuxes: map[string]*runtimeSharedMuxDestination{
			"nil-target": nil,
			"open": {
				name:  "open",
				stage: runtimeAttachMuxCleanupStage(t, "open", openMuxer),
			},
			"attached": {
				name:  "attached",
				stage: runtimeAttachMuxCleanupStage(t, "attached", attachedMuxer),
				ref:   pipeline.NodeRef("attached"),
			},
			"empty": {name: "empty"},
		},
	}

	group.failSharedMuxStages()
	if !openMuxer.failed || !attachedMuxer.failed {
		t.Fatalf("failed markers: open=%v attached=%v, want both", openMuxer.failed, attachedMuxer.failed)
	}

	group.closeSharedMuxStages()
	if openMuxer.closed != 1 {
		t.Fatalf("open muxer closed %d time(s), want 1", openMuxer.closed)
	}
	if group.sharedMuxes["open"].stage != nil {
		t.Fatal("unattached shared mux stage was not cleared")
	}
	if attachedMuxer.closed != 0 {
		t.Fatalf("attached muxer closed %d time(s), want 0", attachedMuxer.closed)
	}
	if group.sharedMuxes["attached"].stage == nil {
		t.Fatal("attached shared mux stage was cleared")
	}
}
