package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav/goavtest/expect"
)

func TestEncodeCustomPCM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	packets, err := encodeCustomPCM(ctx, customCodecRuntime())
	expect.NoError(t, err)
	expect.Equal(t, "packets", len(packets), 1)
	expect.DeepEqual(t, "payload", packets[0].Payload.Bytes, samplesToBytes(5, 6))
}

func TestRunCustomCodec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := runCustomCodec(ctx)
	expect.NoError(t, err)
	expect.DeepEqual(t, "decoded", got, [][]int16{{5, 6}})
	expect.GoldenString(t, "testdata/expected.txt", fmt.Sprintln("decoded:", got))
}
