package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestEncodeCustomPCM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	packets, err := encodeCustomPCM(ctx, customCodecRuntime())
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(packets))
	}
	if got, want := packets[0].Payload.Bytes, samplesToBytes(5, 6); !bytes.Equal(got, want) {
		t.Fatalf("payload = %v, want %v", got, want)
	}
}

func TestRunCustomCodec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := runCustomCodec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]int16{{5, 6}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded = %v, want %v", got, want)
	}
	if output := fmt.Sprintln("decoded:", got); output != expectedOutput(t) {
		t.Fatalf("output = %q, want %q", output, expectedOutput(t))
	}
}

func expectedOutput(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("testdata/expected.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
