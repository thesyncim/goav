package launch

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/internal/spelling"
)

// Error is a refusal of a program, located in its source: what is wrong,
// where (1-based line and column), and how to fix it. Error() renders the
// offending line with the span underlined.
type Error struct {
	Line, Column int
	Msg          string
	// Hint is the fix: a corrected spelling, the expected form, or the
	// option that enables an element.
	Hint string
	// Pos and End are byte offsets of the offending span in the program.
	Pos, End int
	src      string
	cause    error
}

func (e *Error) Error() string {
	var b strings.Builder
	if e.Line > 0 {
		fmt.Fprintf(&b, "launch: %d:%d: %s", e.Line, e.Column, e.Msg)
	} else {
		fmt.Fprintf(&b, "launch: %s", e.Msg)
	}
	if e.src != "" && e.Line > 0 {
		line := sourceLine(e.src, e.Pos)
		width := max(1, min(e.End, e.Pos+len(line)-(e.Column-1))-e.Pos)
		fmt.Fprintf(&b, "\n    %s\n    %s%s", line, strings.Repeat(" ", visualWidth(line[:e.Column-1])), strings.Repeat("^", width))
	}
	if e.Hint != "" {
		fmt.Fprintf(&b, "\n    %s", e.Hint)
	}
	return b.String()
}

// Unwrap returns the error that caused the refusal, such as an element's
// constructor error or a codec setting refusal.
func (e *Error) Unwrap() error { return e.cause }

// locate fills an error's line and column from its byte offsets.
func (e *Error) locate(src string) *Error {
	e.src = src
	if e.Pos < 0 {
		return e
	}
	pos := min(e.Pos, len(src))
	e.Line = strings.Count(src[:pos], "\n") + 1
	e.Column = pos - (strings.LastIndexByte(src[:pos], '\n') + 1) + 1
	if e.End < e.Pos {
		e.End = e.Pos
	}
	return e
}

func sourceLine(src string, pos int) string {
	pos = min(pos, len(src))
	start := strings.LastIndexByte(src[:pos], '\n') + 1
	end := strings.IndexByte(src[pos:], '\n')
	if end < 0 {
		return src[start:]
	}
	return src[start : pos+end]
}

// visualWidth counts the columns a prefix occupies, keeping tabs as tabs so
// the caret lines up under them.
func visualWidth(prefix string) int {
	return len([]rune(strings.ReplaceAll(prefix, "\t", " ")))
}

func errorAt(t token, msg, hint string) *Error {
	return &Error{Pos: t.pos, End: t.end, Msg: msg, Hint: hint}
}

// suggest returns a did-you-mean hint for name among choices, or "".
func suggest(name string, choices []string) string {
	best := spelling.Nearest(name, choices)
	if best == "" {
		return ""
	}
	return fmt.Sprintf("did you mean %q?", best)
}
