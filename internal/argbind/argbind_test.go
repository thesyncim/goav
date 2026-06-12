package argbind

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func TestBindReflectsTypedFieldsAndUnknownMetadata(t *testing.T) {
	type settings struct {
		Name     string        `goavctl:"name,required" usage:"name=<value>" help:"name"`
		Bitrate  int           `goavctl:"bitrate,rate" usage:"[bitrate=<rate>]" help:"bitrate"`
		FPS      av.Duration   `goavctl:"fps,fps" usage:"[fps=<n|n/d>]" help:"frame rate"`
		Delay    time.Duration `goavctl:"delay,duration" usage:"[delay=<duration>]" help:"delay"`
		Custom   av.Metadata   `goavctl:"custom,unknown" usage:"[native_key=value...]" help:"custom"`
		Internal bool          `goavctl:"-"`
	}

	result, err := Bind(Context{
		Name:                 "settings",
		Operation:            "test bind",
		ArgsType:             reflect.TypeOf(settings{}),
		Usage:                "settings name=<value>",
		UnknownMetadataField: "custom",
	}, []string{"name=encoder", "bitrate=1.5M", "fps=30000/1001", "delay=12.5s", "lookahead=deep"})
	if err != nil {
		t.Fatal(err)
	}
	got := result.Value.(settings)
	if got.Name != "encoder" ||
		got.Bitrate != 1_500_000 ||
		got.FPS != (av.Duration{Value: 1001, Base: av.TimeBase{Num: 1, Den: 30000}}) ||
		got.Delay != 12500*time.Millisecond ||
		got.Custom["lookahead"] != "deep" ||
		got.Internal {
		t.Fatalf("settings = %+v", got)
	}
	if _, ok := result.Seen["custom"]; !ok {
		t.Fatalf("seen = %+v, want custom field marked", result.Seen)
	}
}

func TestBindRejectsDuplicateAndUnknownFields(t *testing.T) {
	type settings struct {
		Name string `goavctl:"name,required"`
	}
	ctx := Context{Name: "settings", Operation: "test bind", ArgsType: reflect.TypeOf(settings{})}
	for _, tc := range []struct {
		name string
		args []string
		code string
		node string
	}{
		{name: "duplicate", args: []string{"name=a", "name=b"}, code: "invalid_argument", node: "name"},
		{name: "unknown", args: []string{"title=a"}, code: "unknown_field", node: "title"},
		{name: "missing", args: nil, code: "missing_required", node: "name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Bind(ctx, tc.args)
			var bindErr *Error
			if !errors.As(err, &bindErr) ||
				bindErr.Code != tc.code ||
				bindErr.Node != tc.node {
				t.Fatalf("err = %+v, want %s node=%s", err, tc.code, tc.node)
			}
		})
	}
}

func TestArgsUsageSkipsUnknownFieldWithoutUsage(t *testing.T) {
	type settings struct {
		Name   string      `goavctl:"name,required"`
		Custom av.Metadata `goavctl:"custom,unknown"`
	}
	if got, want := ArgsUsage(reflect.TypeOf(settings{})), "name=<value>"; got != want {
		t.Fatalf("ArgsUsage = %q, want %q", got, want)
	}
}
