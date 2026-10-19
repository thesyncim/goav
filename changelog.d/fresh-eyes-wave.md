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
