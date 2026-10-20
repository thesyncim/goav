// Package gplcodec registers goav's copyleft codec backends. The root module
// and package bundle leave them out, so importing goav links only
// permissively licensed code; this module is the explicit opt-in.
//
// AAC links goaac, a pure-Go translation of FAAD2 under GPL-2.0-or-later. A
// binary that registers it is a derivative work of FAAD2: distribute it under
// GPL-3.0-or-later, the GPL version compatible with goav's Apache-2.0.
// Without this module, AAC decodes through libcodec.AudioToolbox() on macOS.
//
//	rt, err := bundle.New(gplcodec.AAC())
package gplcodec

import (
	"github.com/thesyncim/goav/gplcodec/internal/goaac"
	"github.com/thesyncim/goav/runconfig"
)

// AAC registers the pure-Go AAC-LC decoder (aac/goaac, go). The backend
// decodes on darwin/arm64 and linux/arm64; elsewhere it registers as
// unsupported-target and selection passes it over.
func AAC() runconfig.Option {
	return runconfig.WithCodecAdapter(goaac.Register)
}
