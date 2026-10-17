//go:build !goav_goh264

package goav_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
)

// TestH264FileDecodeNamesBuildTag pins the open-time decode refusal in the
// default build: an MP4's H.264 stream is known only once the file opens,
// and the bundled H.264 decoder is opt-in (goav_goh264). The refusal must be
// a typed BuildError naming the exact build tag, not a bare codec sentinel.
func TestH264FileDecodeNamesBuildTag(t *testing.T) {
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	err = bundle.Run(context.Background(), goav.From(goav.FileInput("in.mp4", bytes.NewReader(data))).
		Video().Decode().Encode(codec.VP8()).
		To(goav.Write("out.webm", &bytes.Buffer{})))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want a *goav.BuildError", err)
	}
	if buildErr.Code != errcode.Code("decode_adapter_unavailable") {
		t.Fatalf("code = %q, want decode_adapter_unavailable\nerr = %v", buildErr.Code, err)
	}
	if !strings.Contains(err.Error(), "build with -tags goav_goh264") {
		t.Fatalf("refusal does not name the build tag:\n%v", err)
	}
}
