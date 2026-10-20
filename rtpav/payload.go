package rtpav

import (
	"github.com/thesyncim/goav/av"
	"maps"
)

// StaticPayloadMap is a fixed PayloadMap over a known codec list — the right
// shape when the payload mapping was negotiated once, out of band.
type StaticPayloadMap struct {
	epoch  av.Epoch
	codecs []PayloadCodec
}

// NewStaticPayloadMap builds a fixed map at the given epoch.
func NewStaticPayloadMap(epoch av.Epoch, codecs []PayloadCodec) StaticPayloadMap {
	return StaticPayloadMap{epoch: epoch, codecs: clonePayloadCodecs(codecs)}
}

// Epoch reports the mapping generation it was built with.
func (m StaticPayloadMap) Epoch() av.Epoch {
	return m.epoch
}

// Lookup resolves one payload type to its codec. Reference fields are borrowed
// immutable values; use Codecs for an independently owned copy.
func (m StaticPayloadMap) Lookup(payloadType uint8) (PayloadCodec, bool) {
	for i := range m.codecs {
		if m.codecs[i].PayloadType == payloadType {
			return m.codecs[i], true
		}
	}
	return PayloadCodec{}, false
}

// Codecs lists every mapped payload codec.
func (m StaticPayloadMap) Codecs() []PayloadCodec {
	return clonePayloadCodecs(m.codecs)
}

func clonePayloadCodec(c PayloadCodec) PayloadCodec {
	c.Parameters.ExtraData = av.Buffer{Bytes: append([]byte(nil), c.Parameters.ExtraData.Bytes...)}
	c.Parameters.Attributes = maps.Clone(c.Parameters.Attributes)
	c.Attributes = maps.Clone(c.Attributes)
	return c
}
func clonePayloadCodecs(in []PayloadCodec) []PayloadCodec {
	out := make([]PayloadCodec, len(in))
	for i, c := range in {
		out[i] = clonePayloadCodec(c)
	}
	return out
}
