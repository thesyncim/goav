//go:build !darwin

package player

import "time"

// hardwareLatency is the output latency past oto's device queue. oto does not
// expose its WASAPI client or PulseAudio stream, so none is reported here.
func hardwareLatency() time.Duration { return 0 }
