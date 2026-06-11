// Command goav provides the goav command-line entry point.
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/thesyncim/goav/internal/launchctl"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "ctl" {
		fmt.Fprintln(os.Stderr, "usage: goav ctl [--control unix://PATH] <command>")
		os.Exit(2)
	}
	control, args, err := parseCtlArgs(os.Args[2:])
	if err != nil {
		printErr(err)
		os.Exit(2)
	}
	if len(args) == 0 || args[0] == "help" {
		topic := args
		if len(topic) != 0 {
			topic = topic[1:]
		}
		text, err := launchctl.Help(topic)
		if err != nil {
			printErr(err)
			os.Exit(2)
		}
		fmt.Print(text)
		return
	}
	request, err := launchctl.RequestFromCLI(args)
	if err != nil {
		printErr(err)
		os.Exit(2)
	}
	if control == "" {
		printErr(fmt.Errorf("missing --control unix://PATH"))
		os.Exit(2)
	}
	if err := send(control, request); err != nil {
		printErr(err)
		os.Exit(1)
	}
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

func send(address string, request launchctl.Request) error {
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
	var response launchctl.Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return err
	}
	if !response.OK {
		if response.Error != nil {
			return response.Error
		}
		return fmt.Errorf("control request failed")
	}
	return json.NewEncoder(os.Stdout).Encode(response.Result)
}

func printErr(err error) {
	fmt.Fprintln(os.Stderr, err)
}
