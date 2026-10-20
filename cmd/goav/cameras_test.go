package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCameraDescribeDoesNotOpenDevice(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"describe", `uri "camera://default?width=640&height=480&fps=30" ! video ! encode vp8 ! file preview.webm`}, &stdout, &stderr); code != 0 {
		t.Fatalf("describe: %s", stderr.String())
	}
	if !strings.Contains(stdout.String(), "camera") || !strings.Contains(stdout.String(), "vp8") {
		t.Fatal(stdout.String())
	}
}
func TestCamerasHelpAndInvalidFlag(t *testing.T) {
	var out, err bytes.Buffer
	if run([]string{"cameras", "--help"}, &out, &err) != 0 || !strings.Contains(out.String(), "--request-access") {
		t.Fatal(out.String(), err.String())
	}
	if run([]string{"cameras", "--no-such-flag"}, &out, &err) != 2 {
		t.Fatal("accepted bad flag")
	}
}
