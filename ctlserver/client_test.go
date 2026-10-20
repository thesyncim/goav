package ctlserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

// The reader can queue a reply and observe EOF before the calling goroutine
// resumes. The completed reply must win even when both channels are ready.
func TestClientKeepsReplyBeforeEOF(t *testing.T) {
	for _, refusal := range []bool{false, true} {
		for range 100 {
			c := &Client{pending: make(map[string]chan response), done: make(chan struct{})}
			c.enc = json.NewEncoder(clientWriteFunc(func(data []byte) (int, error) {
				var req request
				if err := json.Unmarshal(data, &req); err != nil {
					return 0, err
				}
				resp := response{ID: req.ID, Result: json.RawMessage(`{"ok":true}`)}
				if refusal {
					resp.Error = &Error{Code: CodeUnauthorized, Message: "unauthorized"}
				}
				c.pending[string(req.ID)] <- resp
				c.err = io.EOF
				close(c.done)
				return len(data), nil
			}))
			result, err := c.call(context.Background(), "taps", nil)
			if refusal {
				var got *Error
				if !errors.As(err, &got) || got.Code != CodeUnauthorized {
					t.Fatalf("completed refusal lost to EOF: %v", err)
				}
			} else if err != nil || string(result) != `{"ok":true}` {
				t.Fatalf("completed reply lost to EOF: result=%s err=%v", result, err)
			}
		}
	}
}

type clientWriteFunc func([]byte) (int, error)

func (f clientWriteFunc) Write(data []byte) (int, error) { return f(data) }
