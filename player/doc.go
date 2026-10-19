// Package player plays goav media in a window, with pure-Go windowing and
// audio output (Ebitengine and oto: no cgo on macOS, Windows, or Linux).
//
// The speaker is the master clock. Its played position is the clock the
// whole task runs on (runconfig.WithClock), so the realtime pacer delivers
// media exactly as fast as it is heard; video is presented against the heard
// audio position, late frames are dropped, early ones waited for. Pause,
// seek, and rate are the task's own controls.
//
//	w := player.New(player.Title("demo"))
//	rt := bundle.MustNew(runconfig.WithRealtime(true), runconfig.WithClock(w.Clock()))
//	in := goav.FileInput("in.webm", f)
//	task, err := rt.Build(ctx,
//		in.Video().Decode().To(w.Destination()),
//		in.Audio().Decode().Resample(48000, 2).To(w.Destination()))
//	...
//	err = w.Run(ctx, task) // on the main goroutine
//
// In a launch program the window is the play element (Window.Element):
// goav run 'file in.webm ! video ! decode ! play; file in.webm ! audio ! decode ! play'.
//
// On Linux the window and the speaker load libX11/libGL and PulseAudio or
// libasound at run time; without an audio device playback continues silently
// on a free-running clock.
package player
