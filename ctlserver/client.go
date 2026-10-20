package ctlserver

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client calls a ctl server over one connection; calls may run
// concurrently, and watches share the connection.
type Client struct {
	conn      net.Conn
	writeInit sync.Once
	writeGate chan struct{}
	enc       *json.Encoder

	mu      sync.Mutex
	next    int64
	pending map[string]chan response
	watches map[string]chan Event
	// early holds events that arrive while a watch call is in flight,
	// before its response names their subscription.
	early    map[string][]Event
	watching int
	err      error
	done     chan struct{}
}

// earlyEvents bounds the events kept for a watch whose response is in flight.
const earlyEvents = 64

const clientWriteTimeout = 5 * time.Second
const clientCleanupTimeout = time.Second

// Dial connects to a server at unix:///path or tcp://host:port, and
// authenticates with the token a tcp://TOKEN@host:port address carries
// (see Token).
func Dial(ctx context.Context, address string) (*Client, error) {
	token, address := splitToken(address)
	network, addr, err := splitAddress(address)
	if err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn: conn, enc: json.NewEncoder(conn),
		pending: map[string]chan response{}, watches: map[string]chan Event{}, early: map[string][]Event{},
		done: make(chan struct{}),
	}
	go c.read()
	if token != "" {
		if _, err := c.call(ctx, authMethod, map[string]string{"token": token}); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

// splitToken takes the token out of a tcp://TOKEN@host:port address.
func splitToken(address string) (string, string) {
	rest, ok := strings.CutPrefix(address, "tcp://")
	token, hostport, found := strings.Cut(rest, "@")
	if !ok || !found {
		return "", address
	}
	return token, "tcp://" + hostport
}

// Close closes the connection; calls in flight fail.
func (c *Client) Close() error { return c.conn.Close() }

// Call calls method with params (a struct or map, or nil) and returns its
// result as a T; json.RawMessage keeps it raw:
//
//	stats, err := client.Call[pipeline.GraphStats](ctx, "stats", nil)
//	_, err = client.Call[ctlserver.Attached](ctx, "attach", map[string]string{"program": "tap live ! null"})
//
// A refusal is a *Error, its Data carrying goav's structured detail.
func (c *Client) Call[T any](ctx context.Context, method string, params any) (T, error) {
	var zero T
	raw, err := c.call(ctx, method, params)
	if err != nil {
		return zero, err
	}
	var out T
	if len(raw) != 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return zero, err
		}
	}
	return out, nil
}

func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var rawParams json.RawMessage
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		rawParams = data
	}
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	c.next++
	id := strconv.FormatInt(c.next, 10)
	reply := make(chan response, 1)
	c.pending[id] = reply
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	err := c.write(ctx, request{JSONRPC: "2.0", ID: json.RawMessage(id), Method: method, Params: rawParams})
	if err != nil {
		return nil, err
	}
	var resp response
	select {
	case resp = <-reply:
	case <-c.done:
		// The reader queues responses before closing done. A peer may
		// send its last reply (including an auth refusal) and then close;
		// preserve that reply even when select observes EOF first.
		select {
		case resp = <-reply:
		default:
			return nil, c.closedErr()
		}
	case <-ctx.Done():
		// Returning to the caller never waits for the peer to read a cancel.
		go func() {
			cleanup, cancel := context.WithTimeout(context.Background(), clientCleanupTimeout)
			defer cancel()
			_ = c.write(cleanup, request{JSONRPC: "2.0", Method: cancelMethod, Params: json.RawMessage(`{"id":` + id + `}`)})
		}()
		return nil, ctx.Err()
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Result, nil
}

// write owns both serialization and the transport's write deadline. A
// canceled waiter cannot change the active writer's deadline. Once writing
// starts, a failure closes the connection: a partial JSON record cannot be
// safely followed by another request.
func (c *Client) write(ctx context.Context, req request) error {
	ctx, cancel := context.WithTimeout(ctx, clientWriteTimeout)
	defer cancel()
	c.writeInit.Do(func() { c.writeGate = make(chan struct{}, 1) })
	select {
	case c.writeGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.closedErr()
	}
	defer func() { <-c.writeGate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		_ = c.conn.Close()
		return err
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.conn.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	err := c.enc.Encode(req)
	if !stop() {
		<-interrupted
	}
	if err != nil {
		_ = c.conn.Close()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return err
	}
	return c.conn.SetWriteDeadline(time.Time{})
}

// Watch streams the task's events — filtered by params, such as
// {"types": ["stream_added"]} — until ctx ends or the connection closes.
// A slow reader loses events, never the task's media.
func (c *Client) Watch(ctx context.Context, params any) (<-chan Event, error) {
	c.mu.Lock()
	c.watching++
	c.mu.Unlock()
	sub, err := c.Call[struct {
		Subscription string `json:"subscription"`
	}](ctx, "watch", params)
	events := make(chan Event, earlyEvents)
	c.mu.Lock()
	c.watching--
	if err == nil {
		for _, e := range c.early[sub.Subscription] {
			events <- e
		}
		c.watches[sub.Subscription] = events
	}
	delete(c.early, sub.Subscription)
	if c.watching == 0 {
		clear(c.early)
	}
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-c.done:
		}
		c.mu.Lock()
		if ch, ok := c.watches[sub.Subscription]; ok {
			delete(c.watches, sub.Subscription)
			close(ch)
		}
		c.mu.Unlock()
		if ctx.Err() != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), clientCleanupTimeout)
			defer cancel()
			_, _ = c.call(cleanup, "unwatch", map[string]string{"subscription": sub.Subscription})
		}
	}()
	return events, nil
}

func (c *Client) read() {
	dec := json.NewDecoder(c.conn)
	var err error
	for {
		var resp response
		if err = dec.Decode(&resp); err != nil {
			break
		}
		if resp.Method == "event" {
			var p struct {
				Subscription string `json:"subscription"`
				Event        Event  `json:"event"`
			}
			if json.Unmarshal(resp.Params, &p) == nil {
				c.deliver(p.Subscription, p.Event)
			}
			continue
		}
		c.mu.Lock()
		reply := c.pending[string(resp.ID)]
		c.mu.Unlock()
		if reply != nil {
			select {
			case reply <- resp:
			default: // A duplicate response must not stop all other calls.
			}
		}
	}
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	close(c.done)
}

func (c *Client) deliver(subscription string, e Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.watches[subscription]; ok {
		select {
		case ch <- e:
		default: // the reader fell behind: shed, like the server does
		}
		return
	}
	if q := c.early[subscription]; c.watching > 0 && len(q) < earlyEvents {
		c.early[subscription] = append(q, e)
	}
}

func (c *Client) closedErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil && !errors.Is(c.err, net.ErrClosed) {
		return c.err
	}
	return errors.New("ctlserver: connection closed")
}
