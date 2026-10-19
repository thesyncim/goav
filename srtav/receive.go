package srtav

import "sync"

// receiveQueue bounds the packets a receiver holds unread: about 6 MiB, a
// few seconds at broadcast bitrates. A reader that falls further behind
// stalls the session, and SRT drops what turns too late.
const receiveQueue = 4096

// receiver reads a session as fast as it delivers, into a bounded queue
// that Read drains: gosrt drops what a session still holds when the peer's
// shutdown arrives, so the media sent before the peer closed is read in
// full, then the session's end.
type receiver struct {
	conn    *conn
	chunks  chan []byte
	free    chan []byte
	done    chan struct{}
	stop    sync.Once
	err     error // the session's end, set before chunks closes
	chunk   []byte
	pending []byte
}

func receive(c *conn) *receiver {
	r := &receiver{
		conn:   c,
		chunks: make(chan []byte, receiveQueue),
		free:   make(chan []byte, receiveQueue),
		done:   make(chan struct{}),
	}
	go r.pump()
	return r
}

func (r *receiver) pump() {
	defer close(r.chunks)
	for {
		var buf []byte
		select {
		case buf = <-r.free:
		default:
			buf = make([]byte, 1500) // an SRT payload fits
		}
		n, err := r.conn.Read(buf[:cap(buf)])
		if n > 0 {
			select {
			case r.chunks <- buf[:n]:
			case <-r.done:
				return
			}
		}
		if err != nil {
			// Release the session now, its listener's port with it, so a
			// reconnect listens again before goav closes this reader.
			r.err = err
			r.conn.Close()
			return
		}
	}
}

func (r *receiver) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.chunk != nil {
			select {
			case r.free <- r.chunk:
			default:
			}
			r.chunk = nil
		}
		chunk, ok := <-r.chunks
		if !ok {
			return 0, r.err
		}
		r.chunk, r.pending = chunk, chunk
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

// Live reports the input live: the peer sends on its own clock.
func (*receiver) Live() bool { return true }

func (r *receiver) Close() error {
	r.stop.Do(func() { close(r.done) })
	return r.conn.Close()
}
