Removed graph nodes remain owned after a drain timeout and are closed exactly
once when their work finishes. Shutdown also waits for pending removals, and
`GraphStats.PendingCleanup` exposes resources still retiring. Attach rollback
transfers ownership on successful installation and never directly closes an
installed sink during handling.

Control-client deadlines now cover queued and active writes. Cancellation and
watch cleanup return without depending on a blocked peer. WebRTC local-track
attachments share one sender-lifetime RTCP reader and unsubscribe on close;
remote-track replacement interrupts and retries an idle old read.

Native video decoders discard stale pictures and backend state on seek or
configuration changes, adopt the new epoch and configuration, and continue to
drain valid pictures at EOS. Shared native conformance now tests these transitions.

External-consumer gates test resolved behavior and player/bundle dependency
isolation. Release archives distinguish headless and desktop CLIs. Opt-in
720p/1080p file-ladder and real WebRTC mutation workloads record evidence, with
an hour-scale manual and weekly extended CI lane.
