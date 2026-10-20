- Break: the default `bundle` no longer links GPL code. The pure-Go AAC-LC
  decoder (goaac, a FAAD2 translation under GPL-2.0-or-later) moved to the new
  opt-in nested module `github.com/thesyncim/goav/gplcodec`:
  `bundle.New(gplcodec.AAC())`. Without it, AAC decodes through
  `libcodec.AudioToolbox()` on macOS, and a refused AAC decode names both
  options. The root module now requires only permissively licensed backends
  (and LGPL goh264, linked only with `-tags goav_goh264`), pinned with their
  licenses and the import graph by `TestRootModuleDependencyPurity`; the
  modernc runtime left the root module graph. The CLI no longer decodes AAC.
- The README's codec table is true on every platform it claims, pinned by
  `TestReadmeCodecTableMatchesBundle` against the bundle's active backends,
  and `docs/ADAPTERS.md#licenses` states every backend's license and what
  linking it means (LGPL goh264's relinking duty, GPL gplcodec).
- The release workflow can pass: tags are verified through GitHub's signature
  check (`scripts/verify-release-signature.sh`, shared by `ci.yml` and
  `release.yml`) instead of `git verify-tag` against an empty runner keyring;
  `workflow_dispatch` with `dry_run` rehearses every step on any branch without
  a tag or release; a nested tag pinning a goav module at a pseudo-version is
  refused; `gplcodec` and `goavtest/expect` tags are recognized.
- Removed a committed 3.7 MB benchmark helper binary; `TestNoCommittedBinaries`
  refuses any tracked ELF, Mach-O, or PE file.
- Attaching `task.Tap(name)` for a tap the task does not have is refused as
  `runtime_branch_tap_missing` even when the branch encodes or resamples,
  instead of a misleading packet-domain shape error.
- A seek or segment control on a source that cannot reposition is refused when
  it is asked, wrapping `format.ErrNotSeekable` with the fix, and the task plays
  on; it used to return nil and then stop the task. A demuxer whose seeking
  depends on its input reports it through `CanSeek() bool` (Matroska and WebM
  do, and probe the reader, so a pipe behind an `*os.File` counts as
  unseekable). CLI `file` inputs open when the run starts and keep the file's
  capabilities, so `goav ctl seek` works on them; a `provider.ByteSource` may
  also implement `Protocol() av.ProtocolID`.
- Input formats are detected from content: an input's first bytes are probed
  when it opens (MP4, WebM/Matroska, IVF, Annex B, Ogg, FLV, and ADTS, MP3,
  FLAC, WAV, which are named in the refusal when no demuxer reads them), and
  the name, URI, and MIME type are hints, so a misnamed or extensionless file
  works. Probing keeps the reader's capabilities (a file still seeks); an HTTP
  input re-reads its first 4 KiB without a second request.
- A muxer is never handed a stream it would drop: when a destination opens,
  the streams an input turned out to have are checked against the container
  like planned ones, so a whole two-stream file into IVF is refused as
  `destination_mux_incompatible` naming the stream to route, instead of
  silently losing the audio.
- An input its demuxer cannot open is refused as `input_invalid` (phase
  `open`) naming the input, the detected format, and what is wrong (empty,
  truncated, not that format) instead of a bare `EOF` or `matroska: invalid
  data`; a read failure mid-run names its source node.
- The control plane authenticates: `ctlserver.Token(t)` makes every
  connection authenticate first (`$/authenticate`; `Dial` sends it from
  `tcp://TOKEN@host:port`) and every HTTP request carry `Authorization: Bearer`,
  answering others `CodeUnauthorized` (-32001); `ListenAndServe` refuses tcp
  without a Token. `goav run --control tcp://127.0.0.1:PORT` writes a random
  token owner-only to `--control-token FILE` (default: per address, in the
  config directory) that `goav ctl` reads, refuses a non-loopback address
  without `--control-remote`, and confines attached programs to
  `--control-dir` (default `.`). Program output files are created when their
  destination opens, so a refused path fails the build or attach.
- Picture-in-picture works: a Composite (or Mix) arm may `.Resize` (or
  `.Resample`) itself after `.Decode()`, so
  `goav.Composite(cam, goav.At(screen.Video().Decode().Resize(480, 270), 1420, 790))`
  insets the screen on a camera-sized canvas, from frame sources and from
  files whose size is known only when they open (`composite(a, b ! resize
  480x270 ! at 1420 790)` in the CLI). Arm transforms are planned, checked
  against the registered filters, and shown in Describe; work an arm cannot
  lower is refused with the fix.
- `docs/ERROR_CATALOG.md` lists every code a goav refusal can carry (90, by
  family), generated from the source and pinned complete by
  `TestErrorCatalogDocMatchesErrcodeCatalog`, instead of 13 of them.
- Opus and AV1 encode are allocation-pinned (`TestOpusEncodeAllocs`,
  `TestAV1EncodeAllocs`: 0 per frame), as PERFORMANCE.md already claimed.
- Docs say what the code does: the default bundle's concrete VP8/VP9/AV1
  codecs (not "descriptor-only"), `goav.Write` (not `File`), no `From`, 13
  benchmark workloads, the real buffer options, a WebM custom-destination
  example the bundle can run, live provider transcode as working, RTP and
  WebRTC as in-and-out modules, and every nested module in the README.
- CI runs the suite on Windows, fuzz-smokes the CLI language (`FuzzCompile`),
  the Matroska demuxer, the EBML reader, and the RTP packetizers too, and pins
  govulncheck; the repository gains `.gitattributes` (LF), Dependabot,
  CODEOWNERS, and a bug-report form.

Control token files are replaced atomically with owner-only permissions, including when an existing path is world-readable or a symlink. A failed launch on an occupied control port preserves the running server's token.
