package kit

import (
	"errors"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/codec"
)

// ErrUnavailable reports a backend that cannot run here: its library or
// framework did not load, a symbol is missing, the version is unsupported,
// or the platform has none. errors.Is also matches codec.ErrUnavailable.
var ErrUnavailable = errors.New("libcodec backend unavailable")

// Error is a libcodec backend failure with the exact fix: what failed (Op:
// load, symbol, version, platform, open, encode, decode, control), on which
// library, symbol, or setting (Name), and what to do about it.
type Error struct {
	Backend string
	Op      string
	Name    string
	Reason  string
	Fix     string
	Err     error
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("libcodec ")
	b.WriteString(e.Backend)
	b.WriteString(": ")
	if e.Name != "" {
		b.WriteString(e.Name)
		b.WriteString(" ")
	}
	b.WriteString(e.Reason)
	if e.Err != nil && !errors.Is(e.Err, ErrUnavailable) {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	if e.Fix != "" {
		b.WriteString("; fix: ")
		b.WriteString(e.Fix)
	}
	return b.String()
}

func (e *Error) Unwrap() []error {
	switch e.Op {
	case "load", "symbol", "version", "platform":
		return []error{ErrUnavailable, codec.ErrUnavailable, e.Err}
	}
	return []error{e.Err}
}

func unavailable(backend, op, name, reason, fix string, err error) *Error {
	return &Error{Backend: backend, Op: op, Name: name, Reason: reason, Fix: fix, Err: err}
}

// Fail is an Error for a call the backend made into its library: op is
// open, encode, decode, or control, and status the library's own code.
func Fail(backend, op, call string, status int64) error {
	return &Error{Backend: backend, Op: op, Name: call, Reason: "failed with status " + strconv.FormatInt(status, 10)}
}
