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
