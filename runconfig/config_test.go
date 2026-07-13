package runconfig

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func TestDefaultConfigIsValidAndBare(t *testing.T) {
	config := DefaultConfig()
	if err := Validate(config); err != nil {
		t.Fatalf("DefaultConfig does not validate: %v", err)
	}
	if !config.Realtime {
		t.Fatal("DefaultConfig must default to realtime pacing")
	}
	if config.Codecs == nil || config.Filters == nil || config.Formats == nil {
		t.Fatal("DefaultConfig must construct per-runtime registries")
	}
	if config.MediaPools {
		t.Fatal("media pools are opt-in and must default off")
	}
}

func TestNewConfigAppliesOptionsInOrder(t *testing.T) {
	var order []string
	tag := func(name string) Option {
		return func(*Config) error {
			order = append(order, name)
			return nil
		}
	}
	if _, err := NewConfig(tag("first"), tag("second"), tag("third")); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "first,second,third" {
		t.Fatalf("options applied as %s, want first,second,third", got)
	}
}

func TestNewConfigRejectsNilAndFailingOptions(t *testing.T) {
	if _, err := NewConfig(nil); err == nil || !strings.Contains(err.Error(), "option 0") {
		t.Fatalf("nil option error = %v, want indexed refusal", err)
	}
	boom := errors.New("boom")
	_, err := NewConfig(WithRealtime(false), func(*Config) error { return boom })
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "option 1") {
		t.Fatalf("failing option error = %v, want indexed wrap of cause", err)
	}
}

func TestOptionValidationRefusals(t *testing.T) {
	cases := []struct {
		name   string
		option Option
		want   string
	}{
		{"negative event capacity", WithEventCapacity(-1), "event capacity"},
		{"negative close wait", WithCloseWaitTimeout(-time.Second), "close wait timeout"},
		{"nil shape delta", WithShapeDelta(nil), "shape delta contributor is nil"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewConfig(tc.option); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want mention of %q", err, tc.want)
			}
		})
	}
}

func TestValidateRefusesBrokenConfigs(t *testing.T) {
	if err := Validate(nil); err == nil {
		t.Fatal("nil config must refuse")
	}
	broken := DefaultConfig()
	broken.Codecs = nil
	if err := Validate(broken); err == nil || !strings.Contains(err.Error(), "codec registry") {
		t.Fatalf("nil codec registry error = %v", err)
	}
	negative := DefaultConfig()
	negative.EventCapacity = -1
	if err := Validate(negative); err == nil || !strings.Contains(err.Error(), "event capacity") {
		t.Fatalf("negative event capacity error = %v", err)
	}
}

func TestOptionsSetTheirFields(t *testing.T) {
	clock := av.MonotonicClock()
	contributor := ShapeDeltaFunc(func(ShapeDeltaRequest) (ShapeDeltaPlan, bool, error) {
		return ShapeDeltaPlan{}, false, nil
	})
	config, err := NewConfig(
		WithRealtime(false),
		WithMediaPools(true),
		WithEventCapacity(7),
		WithCloseWaitTimeout(3*time.Second),
		WithBufferPolicy(pipeline.BufferPolicy{Capacity: 9}),
		WithClock(clock),
		WithShapeDelta(contributor),
	)
	if err != nil {
		t.Fatal(err)
	}
	if config.Realtime {
		t.Fatal("WithRealtime(false) not applied")
	}
	if !config.MediaPools {
		t.Fatal("WithMediaPools(true) not applied")
	}
	if config.EventCapacity != 7 {
		t.Fatalf("EventCapacity = %d, want 7", config.EventCapacity)
	}
	if config.CloseWaitTimeout != 3*time.Second {
		t.Fatalf("CloseWaitTimeout = %s, want 3s", config.CloseWaitTimeout)
	}
	if config.Buffer.Capacity != 9 {
		t.Fatalf("Buffer.Capacity = %d, want 9", config.Buffer.Capacity)
	}
	if config.Clock == nil {
		t.Fatal("WithClock not applied")
	}
	if len(config.ShapeDeltas) != 1 {
		t.Fatalf("ShapeDeltas length = %d, want 1", len(config.ShapeDeltas))
	}
}

func TestShapeDeltaFuncAdaptsFunction(t *testing.T) {
	called := false
	contributor := ShapeDeltaFunc(func(request ShapeDeltaRequest) (ShapeDeltaPlan, bool, error) {
		called = true
		if !request.Realtime {
			t.Fatal("request did not carry the realtime fact")
		}
		return ShapeDeltaPlan{}, true, nil
	})
	request := ShapeDeltaRequest{Actual: shape.Spec{}, Expected: shape.Spec{}, Realtime: true}
	if _, ok, err := contributor.ShapeDelta(request); err != nil || !ok {
		t.Fatalf("ShapeDelta = ok=%v err=%v, want ok true", ok, err)
	}
	if !called {
		t.Fatal("adapter did not call the wrapped function")
	}
}
