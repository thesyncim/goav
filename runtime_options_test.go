package goav

import goavruntime "github.com/thesyncim/goav/runtime"

type Config = goavruntime.Config
type Option = goavruntime.Option

var (
	WithBufferPolicy    = goavruntime.WithBufferPolicy
	WithClock           = goavruntime.WithClock
	WithCodecAdapter    = goavruntime.WithCodecAdapter
	WithCodecDescriptor = goavruntime.WithCodecDescriptor
	WithDecoder         = goavruntime.WithDecoder
	WithDemuxer         = goavruntime.WithDemuxer
	WithEncoder         = goavruntime.WithEncoder
	WithEventCapacity   = goavruntime.WithEventCapacity
	WithFilter          = goavruntime.WithFilter
	WithFilterAdapter   = goavruntime.WithFilterAdapter
	WithFormatAdapter   = goavruntime.WithFormatAdapter
	WithMuxer           = goavruntime.WithMuxer
	WithProber          = goavruntime.WithProber
	WithRealtime        = goavruntime.WithRealtime
)
