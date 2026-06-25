package goav

import runconfig "github.com/thesyncim/goav/runconfig"

type Config = runconfig.Config
type Option = runconfig.Option

var (
	WithBufferPolicy    = runconfig.WithBufferPolicy
	WithClock           = runconfig.WithClock
	WithCodecAdapter    = runconfig.WithCodecAdapter
	WithCodecDescriptor = runconfig.WithCodecDescriptor
	WithDecoder         = runconfig.WithDecoder
	WithDemuxer         = runconfig.WithDemuxer
	WithEncoder         = runconfig.WithEncoder
	WithEventCapacity   = runconfig.WithEventCapacity
	WithFilter          = runconfig.WithFilter
	WithFilterAdapter   = runconfig.WithFilterAdapter
	WithFormatAdapter   = runconfig.WithFormatAdapter
	WithMuxer           = runconfig.WithMuxer
	WithProber          = runconfig.WithProber
	WithRealtime        = runconfig.WithRealtime
)
