// Command goav provides the goav command-line entry point.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/thesyncim/goav/ctl"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(argv []string, stdout io.Writer, stderr io.Writer) int {
	if len(argv) < 1 {
		fmt.Fprintln(stderr, "usage: goav <ctl|run> ...")
		return 2
	}
	switch argv[0] {
	case "ctl":
		return runCtlCommand(argv[1:], stdout, stderr)
	case "run":
		return runPipelineCommand(argv[1:], stdout, stderr)
	default:
		fmt.Fprintln(stderr, "usage: goav <ctl|run> ...")
		return 2
	}
}

func runCtlCommand(argv []string, stdout io.Writer, stderr io.Writer) int {
	control, args, err := parseCtlArgs(argv)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if len(args) == 0 || (args[0] == "help" && control == "") {
		topic := args
		if len(topic) != 0 {
			topic = topic[1:]
		}
		text, err := ctl.Help(topic)
		if err != nil {
			printErr(stderr, err)
			return 2
		}
		fmt.Fprint(stdout, text)
		return 0
	}
	request, err := ctl.RequestFromCLI(args)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if control == "" {
		printErr(stderr, fmt.Errorf("missing --control unix://PATH"))
		return 2
	}
	if err := sendWithOutput(control, request, stdout); err != nil {
		printErr(stderr, err)
		return 1
	}
	return 0
}

func parseCtlArgs(argv []string) (string, []string, error) {
	var control string
	var args []string
	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		switch {
		case arg == "--control":
			if i+1 >= len(argv) {
				return "", nil, fmt.Errorf("--control needs unix://PATH")
			}
			control = argv[i+1]
			i++
		case strings.HasPrefix(arg, "--control="):
			control = strings.TrimPrefix(arg, "--control=")
		default:
			args = append(args, arg)
		}
	}
	return control, args, nil
}

func send(address string, request ctl.Request) error {
	return sendWithOutput(address, request, os.Stdout)
}

func sendWithOutput(address string, request ctl.Request, stdout io.Writer) error {
	path, ok := strings.CutPrefix(address, "unix://")
	if !ok || path == "" {
		return fmt.Errorf("unsupported control address %q: expected unix://PATH", address)
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		return err
	}
	defer conn.Close()
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(request); err != nil {
		return err
	}
	var response ctl.Response
	if follows(request) {
		decoder := json.NewDecoder(conn)
		for {
			err := decoder.Decode(&response)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if !response.OK {
				if response.Error != nil {
					return response.Error
				}
				return fmt.Errorf("control request failed")
			}
			if err := json.NewEncoder(stdout).Encode(response.Result); err != nil {
				return err
			}
		}
	}
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return err
	}
	if !response.OK {
		if response.Error != nil {
			return response.Error
		}
		return fmt.Errorf("control request failed")
	}
	if rawText(request) {
		text, ok := response.Result.(string)
		if !ok {
			return fmt.Errorf("control request returned %T, want text", response.Result)
		}
		fmt.Fprint(stdout, text)
		return nil
	}
	return json.NewEncoder(stdout).Encode(response.Result)
}

func follows(request ctl.Request) bool {
	switch request.Op {
	case "events", "watch":
		return request.Args["follow"] == "true"
	default:
		return false
	}
}

func rawText(request ctl.Request) bool {
	switch request.Op {
	case "help", "graph", "flowchart":
		return true
	default:
		return false
	}
}

func printErr(stderr io.Writer, err error) {
	fmt.Fprintln(stderr, err)
}
