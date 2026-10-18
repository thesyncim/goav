package ctlserver

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
)

// Client calls a ctl server over one connection; calls may run
// concurrently, and watches share the connection.
type Client struct {
	conn net.Conn
	wmu  sync.Mutex
	enc  *json.Encoder

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

// Dial connects to a server at unix:///path or tcp://host:port.
func Dial(ctx context.Context, address string) (*Client, error) {
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
	return c, nil
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

	c.wmu.Lock()
	err := c.enc.Encode(request{JSONRPC: "2.0", ID: json.RawMessage(id), Method: method, Params: rawParams})
	c.wmu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case resp := <-reply:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	case <-c.done:
		return nil, c.closedErr()
	case <-ctx.Done():
		// Tell the server, so the call stops there too.
		c.wmu.Lock()
		_ = c.enc.Encode(request{JSONRPC: "2.0", Method: cancelMethod, Params: json.RawMessage(`{"id":` + id + `}`)})
		c.wmu.Unlock()
		return nil, ctx.Err()
	}
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
			_, _ = c.call(context.Background(), "unwatch", map[string]string{"subscription": sub.Subscription})
		case <-c.done:
		}
		c.mu.Lock()
		if ch, ok := c.watches[sub.Subscription]; ok {
			delete(c.watches, sub.Subscription)
			close(ch)
		}
		c.mu.Unlock()
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
			reply <- resp
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
