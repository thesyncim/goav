package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestRunCustomFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := runCustomFilter(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]int16{{1, 1, 2, 2}, {3, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("frames = %v, want %v", got, want)
	}
	if output := fmt.Sprintln("frames:", got); output != expectedOutput(t) {
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
