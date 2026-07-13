// Package playoutav adapts scheduled packets, frames, and events into a goav
// source provider. The module owns the playout vocabulary and pacing loop; the
// root goav module only sees a provider.Source passed to goav.Input.
package playoutav
