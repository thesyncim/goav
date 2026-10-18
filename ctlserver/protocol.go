package ctlserver

import (
	"encoding/json"
	"errors"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/launch"
)

// The wire format is JSON-RPC 2.0, one JSON value per message: a request
// {"jsonrpc":"2.0","id":1,"method":"stats","params":{...}}, its response
// {"jsonrpc":"2.0","id":1,"result":...} or {...,"error":{...}}, and, for a
// watch, notifications {"jsonrpc":"2.0","method":"event","params":
// {"subscription":"w1","event":{...}}} pushed on the same connection.

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
	// Method and Params are set on a notification instead.
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
}

type notification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type eventParams struct {
	Subscription string `json:"subscription"`
	Event        any    `json:"event"`
}

// protocolVersion is the wire protocol capabilities reports.
const protocolVersion = 1

// JSON-RPC error codes; Refused carries goav's own refusal in Data.
const (
	CodeParse          = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeRefused        = -32000
)

// Error is a refused call: a JSON-RPC code and message, and for goav's own
// refusals the structured detail — a *goav.BuildError's code, node, details,
// and fixes, or a program's line, column, and hint.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string { return e.Message }

// refusal maps an error to the wire, keeping goav's structure.
func refusal(err error) *Error {
	var rpc *Error
	if errors.As(err, &rpc) {
		return rpc
	}
	out := &Error{Code: CodeRefused, Message: err.Error()}
	var buildErr *goav.BuildError
	var programErr *launch.Error
	hasBuild := errors.As(err, &buildErr)
	switch {
	case errors.As(err, &programErr):
		// A program refusal locates the mistake; a BuildError behind it,
		// such as an element's, keeps its code and fixes under "build".
		detail := struct {
			Kind   string           `json:"kind"`
			Line   int              `json:"line"`
			Column int              `json:"column"`
			Msg    string           `json:"message"`
			Hint   string           `json:"hint,omitempty"`
			Build  *goav.BuildError `json:"build,omitempty"`
		}{Kind: "program", Line: programErr.Line, Column: programErr.Column, Msg: programErr.Msg, Hint: programErr.Hint}
		if hasBuild {
			detail.Build = buildErr
		}
		out.Data, _ = json.Marshal(detail)
	case hasBuild:
		out.Data, _ = json.Marshal(buildErr)
	}
	return out
}

func invalidParams(msg string) *Error { return &Error{Code: CodeInvalidParams, Message: msg} }
