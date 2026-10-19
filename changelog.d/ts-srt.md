- Added MPEG-TS (`container/ts`, standard library only, in `bundle`): `.ts`
  files and `video/mp2t` mux and demux H.264, HEVC, AAC, and Opus. The muxer
  writes PAT/PMT every 100ms and before keyframes, PCR at most 40ms apart,
  AUD and parameter sets where missing, and constant-bitrate padding
  (`ts.MuxerOptions.MuxRate`); the demuxer follows PMT changes, unwraps
  33-bit timestamps, resyncs after garbage, and reports continuity loss and
  discontinuities. Packetizing allocates nothing.
- Added the `udp` scheme (`scheme.UDP`, in `bundle.Schemes`): MPEG-TS over
  UDP, unicast or multicast, in and out.
- Added the `srtav` nested module: `srtav.SRT()` is MPEG-TS over SRT through
  datarhei/gosrt, caller or listener, with `streamid`, `passphrase`,
  `pbkeylen`, and `latency`; a refused passphrase is `srtav.ErrPassphrase`,
  a dropped session `srtav.ErrDisconnected`, which `goav.Reconnect`
  recovers. `goav run` grants `srt` and `udp`.
- A byte destination whose contract declares `Realtime` (srt, udp) now plays
  a file at its media time, as a live media sink does.
- `av.FormatTS` joins the vocabulary. A transport stream of HEVC remuxes
  into MP4: the demuxer reads hvcC and the picture size from the stream.
