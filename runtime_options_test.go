package goav

import runconfig "github.com/thesyncim/goav/runconfig"

type Config = runconfig.Config
type Option = runconfig.Option

var (
	WithBufferPolicy     = runconfig.WithBufferPolicy
	WithClock            = runconfig.WithClock
	WithCodecAdapter     = runconfig.WithCodecAdapter
	WithCodecDescriptor  = runconfig.WithCodecDescriptor
	WithCloseWaitTimeout = runconfig.WithCloseWaitTimeout
	WithDecoder          = runconfig.WithDecoder
	WithDemuxer          = runconfig.WithDemuxer
	WithEncoder          = runconfig.WithEncoder
	WithEventCapacity    = runconfig.WithEventCapacity
	WithFilter           = runconfig.WithFilter
	WithFilterAdapter    = runconfig.WithFilterAdapter
	WithFormatAdapter    = runconfig.WithFormatAdapter
	WithMediaPools       = runconfig.WithMediaPools
	WithMuxer            = runconfig.WithMuxer
	WithProber           = runconfig.WithProber
	WithQoSPolicy        = runconfig.WithQoSPolicy
	WithRealtime         = runconfig.WithRealtime
	WithShapeDelta       = runconfig.WithShapeDelta
)
