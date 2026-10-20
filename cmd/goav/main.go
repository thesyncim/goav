// Command goav provides the goav command-line entry point.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/thesyncim/goav/ctlserver"
)

const usage = `usage: goav <command> ...

  run       run a program
  describe  print the graph a program builds
  explain   print the plan a program builds, as JSON
  play      play a file or a program in a window
  elements  list the elements a program can use
  ctl       inspect and steer a running task
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(argv []string, stdout io.Writer, stderr io.Writer) int {
	if len(argv) < 1 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch argv[0] {
	case "run":
		return runCommand(argv[1:], stdout, stderr)
	case "play":
		return playCommand(argv[1:], stdout, stderr)
	case "describe":
		return describeCommand(argv[1:], stdout, stderr)
	case "explain":
		return explainCommand(argv[1:], stdout, stderr)
	case "elements":
		return elementsCommand(stdout)
	case "ctl":
		return runCtlCommand(argv[1:], stdout, stderr)
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
}

const ctlUsage = `usage: goav ctl --control unix://PATH|tcp://HOST:PORT [--control-token FILE] <method> [key=value...]

  goav ctl ... capabilities                    the server's methods and elements
  goav ctl ... describe format=text            the graph (json, text, dot, mermaid)
  goav ctl ... stats | snapshot | taps | explain
  goav ctl ... watch [types=a,b] [stream=id]   events as JSON lines, until Ctrl-C
  goav ctl ... attach '<program>'              e.g. 'tap live ! encode vp8 ! as preview ! file p.webm'
  goav ctl ... rebranch NAME '<program>' [switch=next-keyframe]
  goav ctl ... detach NAME [outcome=drain]
  goav ctl ... bitrate stream=hd bitrate=1M | keyframe stream=hd | seek position=2s
  goav ctl ... rate value=0.5 | pause | resume | stop

--control takes a socket path (unix:// is the default) or tcp://host:port, a trusted-network interface. Values are JSON-free: 1M, 2s, a,b.
`

// positional names the params a method takes without key=: attach's
// program, detach's attachment.
var positional = map[string][]string{
	"attach":   {"program"},
	"detach":   {"attachment"},
	"rebranch": {"attachment", "program"},
	"unwatch":  {"subscription"},
}

func runCtlCommand(argv []string, stdout io.Writer, stderr io.Writer) int {
	control, args, err := parseCtlArgs(argv)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, ctlUsage)
		return 0
	}
	if control == "" {
		printErr(stderr, errors.New("goav ctl: missing --control unix://PATH"))
		return 2
	}
	method := args[0]
	params, err := ctlParams(method, args[1:])
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client, err := ctlserver.Dial(ctx, control)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	defer client.Close()
	if method == "watch" {
		events, err := client.Watch(ctx, params)
		if err != nil {
			printErr(stderr, err)
			return 1
		}
		enc := json.NewEncoder(stdout)
		for e := range events {
			if err := enc.Encode(e); err != nil {
				return 1
			}
		}
		return 0
	}
	result, err := client.Call[json.RawMessage](ctx, method, params)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	var text string
	if json.Unmarshal(result, &text) == nil {
		fmt.Fprint(stdout, text)
		if !strings.HasSuffix(text, "\n") {
			fmt.Fprintln(stdout)
		}
		return 0
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, result, "", "  ") != nil {
		pretty.Reset()
		pretty.Write(result)
	}
	fmt.Fprintln(stdout, pretty.String())
	return 0
}

// ctlParams turns key=value arguments, and a method's positional ones, into
// params; the server parses each value for its param's type.
func ctlParams(method string, args []string) (map[string]string, error) {
	params := map[string]string{}
	names := positional[method]
	for _, arg := range args {
		key, value, ok := strings.Cut(arg, "=")
		if ok && !strings.ContainsAny(key, " !'\"") {
			params[key] = value
			continue
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("goav ctl %s: %q is not key=value", method, arg)
		}
		params[names[0]] = arg
		names = names[1:]
	}
	return params, nil
}

func parseCtlArgs(argv []string) (string, []string, error) {
	var control, tokenFile string
	var args []string
	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		switch {
		case arg == "--control-token" && i+1 < len(argv):
			tokenFile = argv[i+1]
			i++
		case strings.HasPrefix(arg, "--control-token="):
			tokenFile = strings.TrimPrefix(arg, "--control-token=")
		case arg == "--control":
			if i+1 >= len(argv) {
				return "", nil, errors.New("goav ctl: --control needs unix://PATH or tcp://host:port")
			}
			control = controlAddress(argv[i+1])
			i++
		case strings.HasPrefix(arg, "--control="):
			control = controlAddress(strings.TrimPrefix(arg, "--control="))
		default:
			args = append(args, arg)
		}
	}
	if control == "" {
		return control, args, nil
	}
	control, err := withToken(control, tokenFile)
	return control, args, err
}

func printErr(stderr io.Writer, err error) {
	fmt.Fprintln(stderr, err)
}

// controlAddress defaults --control to a unix socket: a bare path means
// unix://path. Anything with a scheme (unix://, tcp://) passes through.
func controlAddress(s string) string {
	if s == "" || strings.Contains(s, "://") {
		return s
	}
	return "unix://" + s
}
