package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/thesyncim/goav/cameraav"
	"io"
	"os"
	"os/signal"
)

func camerasCommand(argv []string, stdout, stderr io.Writer) int {
	request, jsonOutput := false, false
	for _, arg := range argv {
		switch arg {
		case "--request-access":
			request = true
		case "--json":
			jsonOutput = true
		case "--help", "-h":
			fmt.Fprintln(stdout, "goav cameras [--json] [--request-access]")
			return 0
		default:
			fmt.Fprintf(stderr, "goav cameras: unknown option %s\n", arg)
			return 2
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if request {
		if err := cameraav.RequestAccess(ctx); err != nil {
			printErr(stderr, err)
			return 1
		}
	}
	devices, err := cameraav.Devices(ctx)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(devices); err != nil {
			printErr(stderr, err)
			return 1
		}
		return 0
	}
	for _, device := range devices {
		fmt.Fprintf(stdout, "%s\t%s\n", device.ID, device.Name)
	}
	if len(devices) == 0 {
		fmt.Fprintln(stdout, "No cameras available.")
	}
	return 0
}
