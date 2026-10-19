- Break: the default `bundle` no longer links GPL code. The pure-Go AAC-LC
  decoder (goaac, a FAAD2 translation under GPL-2.0-or-later) moved to the new
  opt-in nested module `github.com/thesyncim/goav/gplcodec`:
  `bundle.New(gplcodec.AAC())`. Without it, AAC decodes through
  `libcodec.AudioToolbox()` on macOS, and a refused AAC decode names both
  options. The root module now requires only permissively licensed backends
  (and LGPL goh264, linked only with `-tags goav_goh264`), pinned with their
  licenses and the import graph by `TestRootModuleDependencyPurity`; the
  modernc runtime left the root module graph. The CLI no longer decodes AAC.
