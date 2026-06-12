package cliargs

import (
	"errors"
	"reflect"
	"testing"
)

func TestSplitPipelineAndFields(t *testing.T) {
	steps, err := SplitPipeline(`meter label="left ! right" note='two words' raw="a=b" ! filesink location="/tmp/a b=1.ogg" title="say \"hi\""`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := steps, []string{
		`meter label="left ! right" note='two words' raw="a=b"`,
		`filesink location="/tmp/a b=1.ogg" title="say \"hi\""`,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("steps = %#v, want %#v", got, want)
	}

	fields, err := PipelineFields(steps[0])
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fields, []string{"meter", "label=left ! right", "note=two words", "raw=a=b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fields = %#v, want %#v", got, want)
	}

	fields, err = PipelineFields(steps[1])
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fields, []string{"filesink", "location=/tmp/a b=1.ogg", `title=say "hi"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fields = %#v, want %#v", got, want)
	}
}

func TestPipelineFieldsPreservesEmptyQuotedValue(t *testing.T) {
	fields, err := PipelineFields(`meter label=""`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fields, []string{"meter", "label="}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fields = %#v, want %#v", got, want)
	}
}

func TestPipelineSyntaxErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func() error
		want string
	}{
		{name: "empty step", run: func() error {
			_, err := SplitPipeline(`copy ! ! filesink location=out.ivf`)
			return err
		}, want: "empty pipeline step"},
		{name: "split quote", run: func() error {
			_, err := SplitPipeline(`copy ! filesink location="unterminated`)
			return err
		}, want: "unterminated quoted value in pipeline"},
		{name: "field quote", run: func() error {
			_, err := PipelineFields(`meter label="unterminated`)
			return err
		}, want: "unterminated quoted value in pipeline step"},
		{name: "field escape", run: func() error {
			_, err := PipelineFields(`meter label="dangling\`)
			return err
		}, want: "unterminated escape sequence in pipeline step"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var syntax *PipelineSyntaxError
			if !errors.As(err, &syntax) || syntax.Message != tc.want || syntax.Offset <= 0 {
				t.Fatalf("err = %#v, want PipelineSyntaxError %q with offset", err, tc.want)
			}
		})
	}
}
