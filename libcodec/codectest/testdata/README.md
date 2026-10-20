# Audio transition fixture

`aac_44100_mono.mp4` is a synthetic 660 Hz tone encoded independently with
FFmpeg's AAC encoder. Shared native conformance alternates it with the 48 kHz
stereo audio in `h264_high_aac.mp4` to verify session reconfiguration.

```sh
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'sine=frequency=660:sample_rate=44100' -t 0.25 -ac 1 \
  -c:a aac -b:a 64k -map_metadata -1 -fflags +bitexact -flags:a +bitexact \
  aac_44100_mono.mp4
```

SHA-256: `73201147d5ad5ef1cb61a79b72260a016fb85d511d7f2604c825fdee3434cebe`.
