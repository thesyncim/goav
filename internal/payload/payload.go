// Package payload turns what a codec, filter, or demuxer wrote into its
// caller-owned result into payload a stage may publish: the stage's own
// header over buffers it holds a reference to.
//
// An adapter result is scratch the adapter may rewrite on its next call.
// A buffer in it that has an Owner is pooled, and its one reference passes to
// the stage as is. A buffer without one aliases adapter memory (reused
// output capacity, a decoder's reference pictures), so it is copied once into
// a pooled buffer. Either way the published bytes are never written again,
// which is what lets every downstream branch share them by reference.
package payload

import "github.com/thesyncim/goav/av"

// TakeFrame fills dst with src's header and plane references dst holds,
// leaving src without them. planes is dst's reusable plane storage; the
// storage dst ends up using is returned for the next call.
func TakeFrame(dst *av.Frame, planes []av.Plane, src *av.Frame) []av.Plane {
	*dst = *src
	planes = append(planes[:0], src.Planes...)
	for i := range planes {
		take(&planes[i].Buffer, &src.Planes[i].Buffer)
	}
	dst.Planes = planes
	return planes
}

// TakePacket fills dst with src's header and a payload reference dst holds,
// leaving src without it.
func TakePacket(dst *av.Packet, src *av.Packet) {
	*dst = *src
	take(&dst.Payload, &src.Payload)
}

// take moves a pooled reference from src to dst, or copies scratch bytes into
// a new pooled buffer.
func take(dst *av.Buffer, src *av.Buffer) {
	if src.Owner != nil {
		*dst = *src
		*src = av.Buffer{}
		return
	}
	if len(src.Bytes) == 0 {
		*dst = av.Buffer{}
		return
	}
	*dst = av.NewBuffer(len(src.Bytes))
	copy(dst.Bytes, src.Bytes)
}

// DropFrames releases the pooled references still left in result frames a
// stage did not publish (an emit failed midway), keeping scratch bytes.
func DropFrames(frames []av.Frame) {
	for i := range frames {
		for j := range frames[i].Planes {
			drop(&frames[i].Planes[j].Buffer)
		}
	}
}

// DropPackets is DropFrames for result packets.
func DropPackets(packets []av.Packet) {
	for i := range packets {
		drop(&packets[i].Payload)
	}
}

// DropPacket is DropFrames for one result packet.
func DropPacket(packet *av.Packet) {
	if packet != nil {
		drop(&packet.Payload)
	}
}

func drop(b *av.Buffer) {
	if b.Owner != nil {
		b.Release()
	}
}
