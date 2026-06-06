package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
)

func TestReadmeRecordRecipeIsSmall(t *testing.T) {
	job := goav.Record(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("recording.ogg", io.Discard),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(spec.String(), "input.ogg -> recording.ogg") {
		t.Fatalf("spec:\n%s", spec.String())
	}

	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(report.Text(), "record") || !strings.Contains(report.Mermaid(), "recording.ogg") {
		t.Fatalf("report:\n%s\nmermaid:\n%s", report.Text(), report.Mermaid())
	}
}

func TestDefaultRecordIVFRecipeRuns(t *testing.T) {
	var out bytes.Buffer
	task, err := goav.Record(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.FileOutput("preview.ivf", &out),
	).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		t.Fatal("empty output")
	}
}

func TestReadmeTranscodeLadderRecipeIsSmall(t *testing.T) {
	job := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Resize(1280, 720).VP9(2_000_000).To("web").
		Video("360p").Resize(640, 360).VP9(600_000).To("preview").
		Output("web", goav.FileOutput("web.webm", io.Discard)).
		Output("preview", goav.FileOutput("preview.webm", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := spec.String()
	if !strings.Contains(text, "resize, 1280x720") ||
		!strings.Contains(text, "codec=vp9") ||
		!strings.Contains(text, "web.webm") ||
		!strings.Contains(text, "preview.webm") {
		t.Fatalf("spec:\n%s", text)
	}
	if strings.Contains(text, "encode-720p -> preview.webm") ||
		strings.Contains(text, "encode-360p -> web.webm") {
		t.Fatalf("branch labels leaked:\n%s", text)
	}
}

func tinyIVF() []byte {
	var data bytes.Buffer
	var header [32]byte
	copy(header[:4], "DKIF")
	binary.LittleEndian.PutUint16(header[6:8], 32)
	copy(header[8:12], "VP80")
	binary.LittleEndian.PutUint16(header[12:14], 16)
	binary.LittleEndian.PutUint16(header[14:16], 16)
	binary.LittleEndian.PutUint32(header[16:20], 1000)
	binary.LittleEndian.PutUint32(header[20:24], 1)
	data.Write(header[:])

	payload := []byte{0x10, 0x20, 0x30}
	var frame [12]byte
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	data.Write(frame[:])
	data.Write(payload)
	return data.Bytes()
}
