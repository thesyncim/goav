package rtpav

import "github.com/thesyncim/goav/av"

type StaticPayloadMap struct {
	epoch  av.Epoch
	codecs []PayloadCodec
}

func NewStaticPayloadMap(epoch av.Epoch, codecs []PayloadCodec) StaticPayloadMap {
	return StaticPayloadMap{epoch: epoch, codecs: codecs}
}

func (m StaticPayloadMap) Epoch() av.Epoch {
	return m.epoch
}

func (m StaticPayloadMap) Lookup(payloadType uint8) (PayloadCodec, bool) {
	for i := range m.codecs {
		if m.codecs[i].PayloadType == payloadType {
			return m.codecs[i], true
		}
	}
	return PayloadCodec{}, false
}

func (m StaticPayloadMap) Codecs() []PayloadCodec {
	return m.codecs
}
