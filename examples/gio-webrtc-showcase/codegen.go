package main

import (
	"fmt"
	"strings"
)

// codeBundle holds the live-generated Go recipe and CLI launch equivalent
// matching the exact single-task goav.Select pipeline running in the studio.
type codeBundle struct {
	GoCode    string `json:"goCode"`
	CLIRecipe string `json:"cliRecipe"`
	Mermaid   string `json:"mermaid"`
	LineCount int    `json:"lineCount"`
}

func generateCode(cfg studioConfig, branches []branchView, rec recordingStatus, hlsSt hlsStatus, whipSt whipStatus, mermaid string) codeBundle {
	pipW, pipH, pipX, pipY := pipGeometry(cfg.Width, cfg.Height, cfg.PiPCorner)
	halfW := (cfg.Width / 2) &^ 1

	var b strings.Builder
	b.WriteString("// Single persistent *goav.Task — zero restarts when switching scenes!\n")
	b.WriteString(fmt.Sprintf("pattern := goav.From(newTestPatternInput(%d, %d, %d)).Video().As(\"pattern\")\n", cfg.Width, cfg.Height, cfg.FPS))
	b.WriteString(fmt.Sprintf("cam     := goav.From(cameraav.New(cameraav.Config{Width: %d, Height: %d, FPS: %d})).Video()\n", cfg.CameraWidth, cfg.CameraHeight, cfg.FPS))
	b.WriteString(fmt.Sprintf("screen  := goav.From(screenav.New(screenav.Config{Width: %d, Height: %d, FPS: %d})).Video()\n", cfg.Width, cfg.Height, cfg.FPS))
	b.WriteString(fmt.Sprintf("pip     := goav.Composite(screen, goav.At(cam.Resize(%d, %d), %d, %d)).As(\"pip\")\n", pipW, pipH, pipX, pipY))
	b.WriteString(fmt.Sprintf("sbs     := goav.Composite(screen.Resize(%d, %d), goav.At(cam.Resize(%d, %d), %d, 0)).As(\"sidebyside\")\n\n", halfW, cfg.Height, halfW, cfg.Height, halfW))

	b.WriteString(fmt.Sprintf("program := goav.Select(pattern, cam.Resize(%d, %d).As(\"camera\"), screen.As(\"screen\"), pip, sbs).Tap(\"program\")\n", cfg.Width, cfg.Height))
	b.WriteString("task, _ := rt.Build(ctx, program.Buffer(flow.DropOldest(8)).To(goav.Sink(monitor)))\n")
	b.WriteString("go task.Run(ctx)\n\n")
	b.WriteString(fmt.Sprintf("// Instant 0ms scene switch (active = %q, recording & WebRTC stay live):\n", cfg.Scene))
	b.WriteString(fmt.Sprintf("task.Control(ctx, control.SelectActive(%q).At(\"select\"))\n", cfg.Scene))

	if len(branches) > 0 {
		b.WriteString("\n// Hot-attached WebRTC fan-out branches (zero restart):\n")
		for _, br := range branches {
			codecFn := codecConstructor(br.Codec, br.Bitrate, br.FPS)
			stateComment := ""
			if br.Paused {
				stateComment = " // paused"
			}
			b.WriteString(fmt.Sprintf("task.Attach(ctx, task.Tap(\"program\").\n\tBuffer(flow.DropOldest(8)).\n\tResize(%d, %d).\n\tEncode(%s).\n\tAs(%q).To(goav.Sink(webrtcTrack)))%s\n",
				br.Width, br.Height, codecFn, br.ID, stateComment))
		}
	}

	if rec.Active {
		codecFn := codecConstructor(rec.Codec, rec.Bitrate, cfg.FPS)
		b.WriteString(fmt.Sprintf("\n// Simultaneous local recording (survives scene cuts!):\ntask.Attach(ctx, task.Tap(\"program\").\n\tBuffer(flow.DropOldest(8)).\n\tEncode(%s).\n\tTo(goav.Write(%q, recFile)))\n",
			codecFn, rec.Filename))
	}

	if hlsSt.Active {
		b.WriteString(fmt.Sprintf("\n// Live LL-HLS origin (250ms parts, pure-Go VP9 fMP4):\ntask.Attach(ctx, task.Tap(\"program\").\n\tBuffer(flow.DropOldest(8)).\n\tEncode(codec.VP9(codec.Bitrate(%d))).\n\tTo(goav.Custom(hls.New(\"live.m3u8\", hlsSrv, hls.Parts(250*time.Millisecond), hls.Window(6)))))\n",
			hlsSt.Bitrate))
	}

	if whipSt.Active {
		codecFn := codecConstructor(whipSt.Codec, whipSt.Bitrate, cfg.FPS)
		b.WriteString(fmt.Sprintf("\n// WHIP live publish:\ntask.Attach(ctx, task.Tap(\"program\").\n\tBuffer(flow.DropOldest(8)).\n\tEncode(%s).\n\tTo(goav.ToURI(%q)))\n",
			codecFn, whipSt.URL))
	}

	goCode := strings.TrimSpace(b.String())

	var cli strings.Builder
	cli.WriteString("GOEXPERIMENT=simd goav run '\n")
	cli.WriteString(fmt.Sprintf("  p := testsrc video size=%dx%d fps=%d\n", cfg.Width, cfg.Height, cfg.FPS))
	for _, br := range branches {
		cli.WriteString(fmt.Sprintf("  p ! resize %dx%d ! encode %s bitrate=%d ! as %s ! file %s.webm\n",
			br.Width, br.Height, br.Codec, br.Bitrate, br.ID, br.ID))
	}
	if rec.Active {
		cli.WriteString(fmt.Sprintf("  p ! encode %s bitrate=%d ! file %s\n", rec.Codec, rec.Bitrate, rec.Filename))
	}
	if hlsSt.Active {
		cli.WriteString(fmt.Sprintf("  p ! encode vp9 bitrate=%d ! hls live.m3u8 parts=250ms window=6\n", hlsSt.Bitrate))
	}
	if whipSt.Active {
		cli.WriteString(fmt.Sprintf("  p ! encode %s bitrate=%d ! uri %q\n", whipSt.Codec, whipSt.Bitrate, whipSt.URL))
	}
	cli.WriteString("'")

	return codeBundle{
		GoCode:    goCode,
		CLIRecipe: cli.String(),
		Mermaid:   mermaid,
		LineCount: strings.Count(goCode, "\n") + 1,
	}
}

func codecConstructor(name string, bitrate, fps int) string {
	switch name {
	case "vp9":
		return fmt.Sprintf("codec.VP9(codec.Bitrate(%d), codec.FPS(%d))", bitrate, fps)
	case "av1":
		return fmt.Sprintf("codec.AV1(codec.Bitrate(%d), codec.FPS(%d))", bitrate, fps)
	default:
		return fmt.Sprintf("codec.VP8(codec.Bitrate(%d), codec.FPS(%d))", bitrate, fps)
	}
}

func pipGeometry(w, h int, corner string) (pipW, pipH, x, y int) {
	pipW = (w / 3) &^ 1
	pipH = (h / 3) &^ 1
	margin := 16
	switch corner {
	case "tl":
		return pipW, pipH, margin, margin
	case "tr":
		return pipW, pipH, w - pipW - margin, margin
	case "bl":
		return pipW, pipH, margin, h - pipH - margin
	default: // "br"
		return pipW, pipH, w - pipW - margin, h - pipH - margin
	}
}
