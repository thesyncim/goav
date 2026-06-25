package control_test

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav/control"
)

func TestControlHasNoExportedFields(t *testing.T) {
	typ := reflect.TypeOf(control.Control{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.IsExported() {
			t.Fatalf("control.Control field %s is exported; use typed constructors and accessors", field.Name)
		}
	}
}

func TestValidatedControlConstructorsRejectInvalidPayloads(t *testing.T) {
	for _, rate := range []float64{0, -1, math.Inf(1), math.NaN()} {
		_, err := control.Rate(rate)
		if !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("control.Rate(%v) err = %v, want ErrInvalid", rate, err)
		}
	}
	for _, bitrate := range []int{0, -1} {
		_, err := control.SetBitrate("video", bitrate)
		if !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("control.SetBitrate(%d) err = %v, want ErrInvalid", bitrate, err)
		}
	}
	for _, window := range []struct {
		start time.Duration
		end   time.Duration
	}{
		{start: 2 * time.Second, end: time.Second},
		{start: time.Second, end: time.Second},
		{start: -time.Nanosecond, end: time.Second},
	} {
		_, err := control.Segment(window.start, window.end)
		if !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("control.Segment(%v, %v) err = %v, want ErrInvalid", window.start, window.end, err)
		}
	}
}

func TestMustPanicsOnInvalidControl(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("control.Must did not panic")
		}
	}()
	_ = control.Must(control.Rate(0))
}
