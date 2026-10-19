package goav

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/shape"
)

func TestDuplicateInputNameErrorContract(t *testing.T) {
	err := duplicateInputNameError("mic", 0, 2)
	assertBuildErrorCode(t, err, inputDuplicateCode)
	if !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want errUnsupportedBuild cause", err)
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	if buildErr.Node != "mic" {
		t.Fatalf("node = %q, want mic", buildErr.Node)
	}
	if !reflect.DeepEqual(buildErr.DetailLines(), []string{"first input index: 0", "second input index: 2"}) {
		t.Fatalf("details = %#v", buildErr.DetailLines())
	}

	source := func(context.Context, SourcePush) error { return nil }
	err = validateInputNames([]InputSpec{
		Source("mic", shape.Frame(av.MediaAudio), source),
		Source("camera", shape.Frame(av.MediaVideo), source),
		Source("mic", shape.Frame(av.MediaAudio), source),
	})
	assertBuildErrorCode(t, err, inputDuplicateCode)
	if !strings.Contains(err.Error(), "input name \"mic\" is defined more than once") {
		t.Fatalf("err = %v, want duplicate name reason", err)
	}
}
