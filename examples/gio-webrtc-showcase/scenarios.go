package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func runPlannerScenarios(ctx context.Context) []scenarioResult {
	now := time.Now()
	scenarios := []struct {
		id   string
		name string
		run  func(context.Context) scenarioResult
	}{
		{id: "opus-ladder", name: "Live Opus ladder", run: scenarioOpusLadder},
		{id: "mix-resample", name: "Mix lab with auto-resample", run: scenarioMixResample},
		{id: "invalid-shape", name: "Invalid audio/video shape", run: scenarioInvalidShape},
	}
	results := make([]scenarioResult, 0, len(scenarios))
	for _, scenario := range scenarios {
		result := scenario.run(ctx)
		result.ID = scenario.id
		result.Name = scenario.name
		result.GeneratedAt = now
		results = append(results, result)
	}
	return results
}

func scenarioOpusLadder(ctx context.Context) scenarioResult {
	out := goavtest.NewCollector()
	report, err := goav.From(goavtest.Audio(48_000, codec.Mono, toneFrame(220), toneFrame(330))).
		Audio().
		Auto(shape.AllowResample()).
		Resample(48_000, codec.Stereo).
		Encode(codec.Opus(codec.Bitrate(96_000), codec.Channels(codec.Stereo), codec.SampleRate(48_000))).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Explain(ctx)
	if err != nil {
		return scenarioError(err)
	}
	return scenarioOK(report, "decode PCM, resample to 48 kHz stereo, encode Opus at 96 kbps")
}

func scenarioMixResample(ctx context.Context) scenarioResult {
	out := goavtest.NewCollector()
	report, err := goav.Mix(
		goav.From(goavtest.Audio(48_000, codec.Mono, toneFrame(140), toneFrame(180))).Audio(),
		goav.From(goavtest.Audio(24_000, codec.Mono, toneFrame(90), toneFrame(120))).Audio(),
	).
		SyncByPTS().
		Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16))).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Explain(ctx)
	if err != nil {
		return scenarioError(err)
	}
	return scenarioOK(report, "mix two S16 arms while the planner inserts the needed 24 kHz to 48 kHz conversion")
}

func scenarioInvalidShape(ctx context.Context) scenarioResult {
	_, err := goav.From(goavtest.Video(320, 180, 1)).
		Video().
		Resample(48_000, codec.Stereo).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Explain(ctx)
	if err == nil {
		return scenarioResult{Status: "error", Summary: "invalid video resample scenario unexpectedly succeeded"}
	}
	result := scenarioError(err)
	result.Status = "expected-error"
	result.Summary = "planner rejected audio resample on a video stream with structured fixes"
	return result
}

func scenarioOK(report plan.Report, summary string) scenarioResult {
	result := scenarioResult{
		Status:  "ok",
		Summary: summary,
		Nodes:   len(report.Graph.Nodes),
		Edges:   len(report.Graph.Edges),
	}
	for _, warning := range report.Warnings {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%s: %s", warning.Code, warning.Message))
	}
	for _, decision := range report.Decisions {
		if decision.Message != "" {
			result.Warnings = append(result.Warnings, decision.Message)
		}
	}
	return result
}

func scenarioError(err error) scenarioResult {
	result := scenarioResult{
		Status: "error",
		Error:  err.Error(),
	}
	var buildErr *goav.BuildError
	if errors.As(err, &buildErr) {
		result.Summary = string(buildErr.Code)
		result.Fixes = append([]string(nil), buildErr.Suggestions...)
	}
	return result
}

func toneFrame(amplitude int16) []int16 {
	frame := make([]int16, 960)
	for i := range frame {
		if i%2 == 0 {
			frame[i] = amplitude
		} else {
			frame[i] = -amplitude
		}
	}
	return frame
}
