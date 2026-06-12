package main

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"math"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	gioapp "gioui.org/app"
	"gioui.org/f32"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

var (
	uiBg       = color.NRGBA{R: 17, G: 19, B: 21, A: 255}
	uiPanel    = color.NRGBA{R: 25, G: 30, B: 34, A: 255}
	uiPanelAlt = color.NRGBA{R: 31, G: 38, B: 43, A: 255}
	uiLine     = color.NRGBA{R: 61, G: 72, B: 80, A: 255}
	uiText     = color.NRGBA{R: 235, G: 241, B: 242, A: 255}
	uiMuted    = color.NRGBA{R: 154, G: 168, B: 173, A: 255}
	uiTeal     = color.NRGBA{R: 52, G: 208, B: 182, A: 255}
	uiAmber    = color.NRGBA{R: 233, G: 180, B: 76, A: 255}
	uiRose     = color.NRGBA{R: 255, G: 107, B: 107, A: 255}
	uiBlue     = color.NRGBA{R: 99, G: 164, B: 255, A: 255}
	uiInk      = color.NRGBA{R: 5, G: 11, B: 12, A: 255}
)

type controlRoom struct {
	server     *server
	browserURL string
	theme      *material.Theme
	list       widget.List

	openBrowser   widget.Clickable
	refresh       widget.Clickable
	addVP8        widget.Clickable
	addVP9        widget.Clickable
	addOpusHi     widget.Clickable
	addOpusLow    widget.Clickable
	keyframeVideo widget.Clickable
	keyframeAudio widget.Clickable
	runScenarios  widget.Clickable

	branchButtons  map[string]*branchButtons
	localScenarios []scenarioResult
	message        string
}

type branchButtons struct {
	pause    widget.Clickable
	resume   widget.Clickable
	retuneUp widget.Clickable
	retuneDn widget.Clickable
	rebranch widget.Clickable
	remove   widget.Clickable
}

func runControlRoom(server *server, browserURL string) error {
	ui := newControlRoom(server, browserURL)
	window := new(gioapp.Window)
	window.Option(
		gioapp.Title("goav Gio WebRTC Showcase"),
		gioapp.Size(unit.Dp(1280), unit.Dp(880)),
		gioapp.MinSize(unit.Dp(860), unit.Dp(640)),
	)
	var ops op.Ops
	for {
		switch event := window.Event().(type) {
		case gioapp.DestroyEvent:
			return event.Err
		case gioapp.FrameEvent:
			gtx := gioapp.NewContext(&ops, event)
			ui.Layout(gtx)
			event.Frame(gtx.Ops)
		}
	}
}

func newControlRoom(server *server, browserURL string) *controlRoom {
	th := material.NewTheme()
	th.Palette.Bg = uiBg
	th.Palette.Fg = uiText
	th.Palette.ContrastBg = uiTeal
	th.Palette.ContrastFg = uiInk
	return &controlRoom{
		server:         server,
		browserURL:     browserURL,
		theme:          th,
		list:           widget.List{List: layout.List{Axis: layout.Vertical}},
		branchButtons:  make(map[string]*branchButtons),
		localScenarios: runPlannerScenarios(context.Background()),
	}
}

func (ui *controlRoom) Layout(gtx layout.Context) layout.Dimensions {
	state := ui.snapshot()
	ui.handleGlobalClicks(gtx, state)
	gtx.Execute(op.InvalidateCmd{At: gtx.Now.Add(250 * time.Millisecond)})
	paint.FillShape(gtx.Ops, uiBg, clip.Rect{Max: gtx.Constraints.Max}.Op())

	return layout.UniformInset(unit.Dp(14)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(12)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.header(gtx, state)
			}),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return material.List(ui.theme, &ui.list).Layout(gtx, 7, func(gtx layout.Context, index int) layout.Dimensions {
					switch index {
					case 0:
						return ui.quickActions(gtx, state)
					case 1:
						return ui.videoPanel(gtx, state)
					case 2:
						return ui.audioPanel(gtx, state)
					case 3:
						return ui.branchPanel(gtx, state)
					case 4:
						return ui.graphPanel(gtx, state)
					case 5:
						return ui.scenarioPanel(gtx, state)
					default:
						return ui.eventPanel(gtx, state)
					}
				})
			}),
		)
	})
}

func (ui *controlRoom) snapshot() stateResponse {
	if session := ui.server.latestSession(); session != nil {
		return session.State()
	}
	return stateResponse{
		BrowserURL:  ui.browserURL,
		NativeAudio: nativeAudioStatus{Message: "waiting for a WebRTC session"},
		Scenarios:   append([]scenarioResult(nil), ui.localScenarios...),
	}
}

func (ui *controlRoom) handleGlobalClicks(gtx layout.Context, state stateResponse) {
	for ui.openBrowser.Clicked(gtx) {
		if err := openURL(ui.browserURL); err != nil {
			ui.message = err.Error()
		} else {
			ui.message = "browser peer opened"
		}
	}
	for ui.refresh.Clicked(gtx) {
		ui.message = "state refreshed"
	}
	for ui.addVP8.Clicked(gtx) {
		ui.addBranch(branchSpec{Kind: "video", Codec: "vp8", Width: 960, Height: 540, Bitrate: 1_200_000})
	}
	for ui.addVP9.Clicked(gtx) {
		ui.addBranch(branchSpec{Kind: "video", Codec: "vp9", Width: 640, Height: 360, Bitrate: 700_000})
	}
	for ui.addOpusHi.Clicked(gtx) {
		ui.addBranch(branchSpec{Kind: "audio", Codec: "opus", Bitrate: 128_000})
	}
	for ui.addOpusLow.Clicked(gtx) {
		ui.addBranch(branchSpec{Kind: "audio", Codec: "opus", Bitrate: 32_000})
	}
	for ui.keyframeVideo.Clicked(gtx) {
		ui.requestKeyframe("video")
	}
	for ui.keyframeAudio.Clicked(gtx) {
		ui.requestKeyframe("audio")
	}
	for ui.runScenarios.Clicked(gtx) {
		results := runPlannerScenarios(context.Background())
		if session := ui.server.latestSession(); session != nil {
			session.setScenarios(results)
		} else {
			ui.localScenarios = results
		}
		ui.message = fmt.Sprintf("ran %d planner scenarios", len(results))
	}
	_ = state
}

func (ui *controlRoom) header(gtx layout.Context, state stateResponse) layout.Dimensions {
	status := "waiting for browser peer"
	statusColor := uiAmber
	if state.ID != "" {
		status = "session " + state.ID[:minInt(len(state.ID), 8)]
		statusColor = uiTeal
	}
	if state.LastError != "" {
		status = state.LastError
		statusColor = uiRose
	}
	return ui.panel(gtx, "", "", func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle, Gap: gtx.Dp(12)}.Layout(gtx,
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(4)}.Layout(gtx,
					layout.Rigid(ui.label("goav Gio WebRTC Showcase", 22, uiText).Layout),
					layout.Rigid(ui.label("Native Gio control room for live WebRTC media, pure-Go codecs, runtime branches, and audio diagnostics.", 13, uiMuted).Layout),
				)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.pill(gtx, status, statusColor)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.button(gtx, &ui.openBrowser, "Open Peer", uiTeal, true)
			}),
		)
	})
}

func (ui *controlRoom) quickActions(gtx layout.Context, state stateResponse) layout.Dimensions {
	return ui.panel(gtx, "Live Controls", state.BrowserURL, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(10)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Horizontal, Gap: gtx.Dp(8)}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.button(gtx, &ui.addVP8, "Add VP8 540p", uiBlue, state.ID != "")
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.button(gtx, &ui.addVP9, "Add VP9 360p", uiBlue, state.ID != "")
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.button(gtx, &ui.addOpusHi, "Add Opus 128k", uiTeal, state.ID != "")
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.button(gtx, &ui.addOpusLow, "Add Opus 32k", uiTeal, state.ID != "")
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.button(gtx, &ui.keyframeVideo, "Video Keyframe", uiAmber, state.VideoCodec != "")
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.button(gtx, &ui.runScenarios, "Run Scenarios", uiPanelAlt, true)
					}),
				)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				text := "Open the browser peer, start camera or synthetic A/V, then control branches here."
				if ui.message != "" {
					text = ui.message
				}
				return ui.label(text, 13, uiMuted).Layout(gtx)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.metricRow(gtx, []metric{
					{"Inbound video", firstNonEmpty(state.VideoCodec, "waiting")},
					{"Inbound audio", firstNonEmpty(state.AudioCodec, "waiting")},
					{"Received FPS", formatFPS(state.Video.FPS, state.Video.Status)},
					{"Branches", strconv.Itoa(len(state.Branches))},
					{"Revision", strconv.FormatUint(state.Revision, 10)},
					{"Native audio", state.NativeAudio.Message},
				})
			}),
		)
	})
}

func (ui *controlRoom) videoPanel(gtx layout.Context, state stateResponse) layout.Dimensions {
	video := state.Video
	status := firstNonEmpty(video.Status, "waiting")
	if video.Warning != "" {
		status += ": " + video.Warning
	}
	format := "waiting"
	if video.Width > 0 && video.Height > 0 {
		format = fmt.Sprintf("%dx%d, %s", video.Width, video.Height, firstNonEmpty(video.PixelFormat, "video"))
	}
	return ui.panel(gtx, "Video Receive", status, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(10)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.metricRow(gtx, []metric{
					{"Format", format},
					{"Frames", formatCount(video.Frames)},
					{"FPS", formatFPS(video.FPS, video.Status)},
					{"Validation", firstNonEmpty(video.Status, "waiting")},
					{"Last frame", formatFrameAge(video.LastFrameMS)},
					{"Last PTS", firstNonEmpty(video.LastPTS, "waiting")},
				})
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.surface(gtx, videoStatusColor(video), func(gtx layout.Context) layout.Dimensions {
					text := "decoded video frame-rate validation waiting for input"
					if video.Valid {
						text = fmt.Sprintf("decoded video is live at %.1f FPS", video.FPS)
					} else if video.Warning != "" {
						text = video.Warning
					}
					return ui.label(text, 13, uiText).Layout(gtx)
				})
			}),
		)
	})
}

func (ui *controlRoom) audioPanel(gtx layout.Context, state stateResponse) layout.Dimensions {
	audio := state.Audio
	level := fmt.Sprintf("rms %.2f / peak %.2f", audio.RMS, audio.Peak)
	format := "waiting"
	if audio.SampleRate > 0 {
		format = fmt.Sprintf("%d Hz, %d ch, %s", audio.SampleRate, audio.Channels, firstNonEmpty(audio.SampleFormat, "pcm"))
	}
	return ui.panel(gtx, "Audio Lab", level, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(10)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.metricRow(gtx, []metric{
					{"Format", format},
					{"Frames", formatCount(audio.Frames)},
					{"Packets", formatCount(audio.Packets)},
					{"Packet loss", formatCount(audio.LossEvents)},
					{"PLC", formatCount(audio.PLCFrames)},
					{"Last PTS", firstNonEmpty(audio.LastPTS, "waiting")},
				})
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.waveform(gtx, audio.Waveform, audio.RMS, audio.Peak)
			}),
		)
	})
}

func (ui *controlRoom) branchPanel(gtx layout.Context, state stateResponse) layout.Dimensions {
	return ui.panel(gtx, "Runtime Branch Matrix", fmt.Sprintf("%d branches", len(state.Branches)), func(gtx layout.Context) layout.Dimensions {
		if len(state.Branches) == 0 {
			return ui.empty(gtx, "waiting for browser session")
		}
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(8)}.Layout(gtx, ui.branchRows(state.Branches)...)
	})
}

func (ui *controlRoom) branchRows(branches []branchView) []layout.FlexChild {
	rows := make([]layout.FlexChild, 0, len(branches))
	for _, branch := range branches {
		branch := branch
		rows = append(rows, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return ui.branchRow(gtx, branch)
		}))
	}
	return rows
}

func (ui *controlRoom) branchRow(gtx layout.Context, branch branchView) layout.Dimensions {
	buttons := ui.buttonsForBranch(branch.ID)
	ui.handleBranchClicks(gtx, branch, buttons)
	bg := uiPanelAlt
	if !branch.Bound {
		bg = color.NRGBA{R: 39, G: 35, B: 26, A: 255}
	}
	return ui.surface(gtx, bg, func(gtx layout.Context) layout.Dimensions {
		status := branch.State
		if branch.Paused {
			status = "paused"
		}
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle, Gap: gtx.Dp(8)}.Layout(gtx,
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				title := fmt.Sprintf("%s / %s", branch.ID, branch.Codec)
				detail := fmt.Sprintf("%s, %s, %d packets, %s", branch.Kind, status, branch.Packets, formatBytes(branch.Bytes))
				if branch.Kind == "video" {
					detail = fmt.Sprintf("%s, %dx%d, %s, %d packets, %s", branch.Kind, branch.Width, branch.Height, status, branch.Packets, formatBytes(branch.Bytes))
				}
				return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(2)}.Layout(gtx,
					layout.Rigid(ui.label(title, 14, uiText).Layout),
					layout.Rigid(ui.label(detail, 12, uiMuted).Layout),
				)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.button(gtx, &buttons.pause, "Pause", uiPanel, branch.Bound && !branch.Paused)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.button(gtx, &buttons.resume, "Resume", uiPanel, branch.Bound && branch.Paused)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.button(gtx, &buttons.retuneDn, "- Rate", uiPanel, branch.Bound)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.button(gtx, &buttons.retuneUp, "+ Rate", uiPanel, branch.Bound)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.button(gtx, &buttons.rebranch, "Rebranch", uiAmber, branch.Bound)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.button(gtx, &buttons.remove, "Remove", uiRose, true)
			}),
		)
	})
}

func (ui *controlRoom) graphPanel(gtx layout.Context, state stateResponse) layout.Dimensions {
	return ui.panel(gtx, "Describe Graph", fmt.Sprintf("%d nodes", state.VideoGraph.Stats.NodeCount+state.AudioGraph.Stats.NodeCount), func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(12)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.graph(gtx, "video", state.VideoGraph)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.graph(gtx, "audio", state.AudioGraph)
			}),
		)
	})
}

func (ui *controlRoom) scenarioPanel(gtx layout.Context, state stateResponse) layout.Dimensions {
	return ui.panel(gtx, "Planner Scenarios", fmt.Sprintf("%d cases", len(state.Scenarios)), func(gtx layout.Context) layout.Dimensions {
		if len(state.Scenarios) == 0 {
			return ui.empty(gtx, "run scenarios to inspect planner behavior")
		}
		children := make([]layout.FlexChild, 0, len(state.Scenarios))
		for _, scenario := range state.Scenarios {
			scenario := scenario
			children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.scenarioRow(gtx, scenario)
			}))
		}
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(8)}.Layout(gtx, children...)
	})
}

func (ui *controlRoom) scenarioRow(gtx layout.Context, scenario scenarioResult) layout.Dimensions {
	c := uiTeal
	if scenario.Status == "expected-error" {
		c = uiAmber
	} else if scenario.Status == "error" {
		c = uiRose
	}
	return ui.surface(gtx, uiPanelAlt, func(gtx layout.Context) layout.Dimensions {
		detail := scenario.Summary
		if scenario.Nodes != 0 || scenario.Edges != 0 {
			detail = fmt.Sprintf("%s · %d nodes / %d edges", detail, scenario.Nodes, scenario.Edges)
		}
		if scenario.Error != "" {
			detail = scenario.Error
		}
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle, Gap: gtx.Dp(10)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions { return ui.pill(gtx, scenario.Status, c) }),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(2)}.Layout(gtx,
					layout.Rigid(ui.label(scenario.Name, 14, uiText).Layout),
					layout.Rigid(ui.label(detail, 12, uiMuted).Layout),
				)
			}),
		)
	})
}

func (ui *controlRoom) eventPanel(gtx layout.Context, state stateResponse) layout.Dimensions {
	return ui.panel(gtx, "Event Feed", fmt.Sprintf("%d events", len(state.Events)), func(gtx layout.Context) layout.Dimensions {
		if len(state.Events) == 0 {
			return ui.empty(gtx, "waiting")
		}
		events := append([]debugEvent(nil), state.Events...)
		sort.Slice(events, func(i, j int) bool { return events[i].Seq > events[j].Seq })
		if len(events) > 20 {
			events = events[:20]
		}
		children := make([]layout.FlexChild, 0, len(events))
		for _, event := range events {
			event := event
			children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.eventRow(gtx, event)
			}))
		}
		return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(6)}.Layout(gtx, children...)
	})
}

func (ui *controlRoom) eventRow(gtx layout.Context, event debugEvent) layout.Dimensions {
	c := uiMuted
	if event.Level == "error" {
		c = uiRose
	} else if event.Level == "info" {
		c = uiTeal
	} else if event.Level == "debug" {
		c = uiAmber
	}
	scope := strings.Join(compactStrings(event.Stream, event.Branch), " / ")
	text := fmt.Sprintf("#%d %s", event.Seq, event.Message)
	if scope != "" {
		text += " · " + scope
	}
	return ui.surface(gtx, uiPanelAlt, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle, Gap: gtx.Dp(8)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions { return ui.pill(gtx, event.Kind, c) }),
			layout.Flexed(1, ui.label(text, 12, uiText).Layout),
			layout.Rigid(ui.label(event.Time.Format("15:04:05"), 12, uiMuted).Layout),
		)
	})
}

func (ui *controlRoom) panel(gtx layout.Context, title, badge string, child layout.Widget) layout.Dimensions {
	return layout.Inset{Bottom: unit.Dp(12)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return ui.surface(gtx, uiPanel, func(gtx layout.Context) layout.Dimensions {
			return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				if title == "" {
					return child(gtx)
				}
				return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(10)}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle, Gap: gtx.Dp(8)}.Layout(gtx,
							layout.Flexed(1, ui.label(title, 15, uiText).Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								if badge == "" {
									return layout.Dimensions{}
								}
								return ui.pill(gtx, clipText(badge, 52), uiPanelAlt)
							}),
						)
					}),
					layout.Rigid(child),
				)
			})
		})
	})
}

func (ui *controlRoom) surface(gtx layout.Context, bg color.NRGBA, child layout.Widget) layout.Dimensions {
	macro := op.Record(gtx.Ops)
	dims := layout.UniformInset(unit.Dp(10)).Layout(gtx, child)
	call := macro.Stop()
	rect := image.Rectangle{Max: dims.Size}
	rr := clip.UniformRRect(rect, gtx.Dp(8))
	paint.FillShape(gtx.Ops, bg, rr.Op(gtx.Ops))
	paint.FillShape(gtx.Ops, uiLine, clip.Stroke{Path: rr.Path(gtx.Ops), Width: 1}.Op())
	call.Add(gtx.Ops)
	return dims
}

func (ui *controlRoom) label(text string, size unit.Sp, c color.NRGBA) material.LabelStyle {
	label := material.Label(ui.theme, size, text)
	label.Color = c
	label.MaxLines = 3
	label.Truncator = "..."
	return label
}

func (ui *controlRoom) button(gtx layout.Context, click *widget.Clickable, text string, bg color.NRGBA, enabled bool) layout.Dimensions {
	btn := material.Button(ui.theme, click, text)
	btn.CornerRadius = unit.Dp(6)
	btn.TextSize = unit.Sp(12)
	btn.Background = bg
	btn.Color = uiText
	if bg == uiTeal || bg == uiAmber {
		btn.Color = uiInk
	}
	if !enabled {
		btn.Background = color.NRGBA{R: 43, G: 48, B: 52, A: 255}
		btn.Color = color.NRGBA{R: 120, G: 132, B: 136, A: 255}
		return btn.Layout(gtx.Disabled())
	}
	return btn.Layout(gtx)
}

func (ui *controlRoom) pill(gtx layout.Context, text string, c color.NRGBA) layout.Dimensions {
	return ui.surface(gtx, c, func(gtx layout.Context) layout.Dimensions {
		labelColor := uiText
		if c == uiTeal || c == uiAmber {
			labelColor = uiInk
		}
		label := ui.label(text, 12, labelColor)
		label.MaxLines = 1
		return label.Layout(gtx)
	})
}

type metric struct {
	label string
	value string
}

func (ui *controlRoom) metricRow(gtx layout.Context, metrics []metric) layout.Dimensions {
	children := make([]layout.FlexChild, 0, len(metrics))
	for _, m := range metrics {
		m := m
		children = append(children, layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return ui.surface(gtx, uiPanelAlt, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical, Gap: gtx.Dp(3)}.Layout(gtx,
					layout.Rigid(ui.label(m.label, 11, uiMuted).Layout),
					layout.Rigid(ui.label(clipText(m.value, 34), 14, uiText).Layout),
				)
			})
		}))
	}
	return layout.Flex{Axis: layout.Horizontal, Gap: gtx.Dp(8)}.Layout(gtx, children...)
}

func (ui *controlRoom) empty(gtx layout.Context, text string) layout.Dimensions {
	return ui.surface(gtx, uiPanelAlt, func(gtx layout.Context) layout.Dimensions {
		return layout.Center.Layout(gtx, ui.label(text, 13, uiMuted).Layout)
	})
}

func (ui *controlRoom) waveform(gtx layout.Context, wave []float32, rms, peak float64) layout.Dimensions {
	size := image.Pt(gtx.Constraints.Max.X, gtx.Dp(118))
	if size.X < 160 {
		size.X = 160
	}
	rect := image.Rectangle{Max: size}
	paint.FillShape(gtx.Ops, color.NRGBA{R: 13, G: 17, B: 19, A: 255}, clip.UniformRRect(rect, gtx.Dp(8)).Op(gtx.Ops))
	mid := float32(size.Y) * 0.58
	for i := 0; i < 5; i++ {
		y := float32(16 + i*(size.Y-32)/4)
		ui.stroke(gtx, uiLine, 1, func(path *clip.Path) {
			path.MoveTo(f32.Pt(12, y))
			path.LineTo(f32.Pt(float32(size.X-12), y))
		})
	}
	if len(wave) > 1 {
		step := float32(size.X-28) / float32(len(wave)-1)
		ui.stroke(gtx, uiTeal, 2, func(path *clip.Path) {
			for i, v := range wave {
				amp := float32(math.Min(float64(v), 1))
				x := float32(14) + float32(i)*step
				y := mid - amp*float32(size.Y-36)/2
				if i == 0 {
					path.MoveTo(f32.Pt(x, y))
				} else {
					path.LineTo(f32.Pt(x, y))
				}
			}
		})
	}
	ui.drawLabelAt(gtx, fmt.Sprintf("RMS %.2f", rms), 16, 12, 12, uiMuted)
	ui.drawLabelAt(gtx, fmt.Sprintf("Peak %.2f", peak), 16, 32, 12, uiMuted)
	return layout.Dimensions{Size: size}
}

func (ui *controlRoom) graph(gtx layout.Context, label string, graph graphView) layout.Dimensions {
	nodes := append([]nodeView(nil), graph.Nodes...)
	edges := append([]edgeView(nil), graph.Edges...)
	size := image.Pt(maxInt(gtx.Constraints.Max.X, 260), gtx.Dp(260))
	if len(nodes) == 0 {
		rect := image.Rectangle{Max: size}
		paint.FillShape(gtx.Ops, color.NRGBA{R: 13, G: 17, B: 19, A: 255}, clip.UniformRRect(rect, gtx.Dp(8)).Op(gtx.Ops))
		ui.drawLabelAt(gtx, label, 14, 12, 14, uiMuted)
		ui.drawLabelAt(gtx, label+" graph waiting", size.X/2-72, size.Y/2, 13, uiMuted)
		return layout.Dimensions{Size: size}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	depth := make(map[string]int, len(nodes))
	for i := 0; i < len(nodes); i++ {
		for _, edge := range edges {
			if depth[edge.To] < depth[edge.From]+1 {
				depth[edge.To] = depth[edge.From] + 1
			}
		}
	}
	lanes := make(map[int][]nodeView)
	maxDepth := 0
	for _, node := range nodes {
		d := depth[node.Name]
		if d > maxDepth {
			maxDepth = d
		}
		lanes[d] = append(lanes[d], node)
	}
	if maxDepth == 0 {
		maxDepth = 1
	}
	boxW := clampInt((size.X-64)/(maxDepth+1)-16, 132, 210)
	boxH := gtx.Dp(58)
	laneGap := gtx.Dp(18)
	topPad := gtx.Dp(50)
	maxLane := 1
	for _, lane := range lanes {
		if len(lane) > maxLane {
			maxLane = len(lane)
		}
	}
	size.Y = maxInt(size.Y, topPad+maxLane*(boxH+laneGap)+gtx.Dp(10))
	rect := image.Rectangle{Max: size}
	paint.FillShape(gtx.Ops, color.NRGBA{R: 13, G: 17, B: 19, A: 255}, clip.UniformRRect(rect, gtx.Dp(8)).Op(gtx.Ops))
	ui.drawLabelAt(gtx, label, 14, 12, 14, uiMuted)

	pos := make(map[string]image.Point, len(nodes))
	for d, lane := range lanes {
		sort.Slice(lane, func(i, j int) bool { return lane[i].Name < lane[j].Name })
		for i, node := range lane {
			x := 18 + d*(size.X-36-boxW)/maxDepth
			y := topPad + i*(boxH+laneGap)
			if y+boxH > size.Y-12 {
				y = size.Y - boxH - 12
			}
			pos[node.Name] = image.Pt(x, y)
		}
	}
	for _, edge := range edges {
		a, okA := pos[edge.From]
		b, okB := pos[edge.To]
		if !okA || !okB {
			continue
		}
		ui.stroke(gtx, color.NRGBA{R: 77, G: 91, B: 101, A: 255}, 1.5, func(path *clip.Path) {
			path.MoveTo(f32.Pt(float32(a.X+boxW), float32(a.Y+boxH/2)))
			path.LineTo(f32.Pt(float32(b.X), float32(b.Y+boxH/2)))
		})
	}
	for _, node := range nodes {
		p := pos[node.Name]
		bg := color.NRGBA{R: 28, G: 43, B: 66, A: 255}
		switch node.Kind {
		case "source":
			bg = color.NRGBA{R: 24, G: 68, B: 61, A: 255}
		case "sink":
			bg = color.NRGBA{R: 78, G: 57, B: 31, A: 255}
		}
		r := image.Rect(p.X, p.Y, p.X+boxW, p.Y+boxH)
		paint.FillShape(gtx.Ops, bg, clip.UniformRRect(r, gtx.Dp(6)).Op(gtx.Ops))
		ui.drawLabelAtWidth(gtx, node.Name, p.X+8, p.Y+8, boxW-16, 12, uiText)
		ui.drawLabelAtWidth(gtx, firstNonEmpty(node.Detail, node.Kind), p.X+8, p.Y+32, boxW-16, 10, uiMuted)
	}
	return layout.Dimensions{Size: size}
}

func (ui *controlRoom) stroke(gtx layout.Context, c color.NRGBA, width float32, build func(*clip.Path)) {
	var path clip.Path
	path.Begin(gtx.Ops)
	build(&path)
	spec := path.End()
	paint.FillShape(gtx.Ops, c, clip.Stroke{Path: spec, Width: width}.Op())
}

func (ui *controlRoom) drawLabelAt(gtx layout.Context, text string, x, y int, size unit.Sp, c color.NRGBA) {
	ui.drawLabelAtWidth(gtx, text, x, y, maxInt(20, gtx.Constraints.Max.X-x), size, c)
}

func (ui *controlRoom) drawLabelAtWidth(gtx layout.Context, text string, x, y, width int, size unit.Sp, c color.NRGBA) {
	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	gtx2 := gtx
	gtx2.Constraints = layout.Exact(image.Pt(maxInt(20, width), gtx.Sp(size)+10))
	label := ui.label(text, size, c)
	label.MaxLines = 1
	label.Layout(gtx2)
	stack.Pop()
}

func (ui *controlRoom) buttonsForBranch(id string) *branchButtons {
	buttons := ui.branchButtons[id]
	if buttons == nil {
		buttons = &branchButtons{}
		ui.branchButtons[id] = buttons
	}
	return buttons
}

func (ui *controlRoom) handleBranchClicks(gtx layout.Context, branch branchView, buttons *branchButtons) {
	for buttons.pause.Clicked(gtx) {
		ui.withSession(func(session *session) error { return session.pauseBranch(context.Background(), branch.ID) })
	}
	for buttons.resume.Clicked(gtx) {
		ui.withSession(func(session *session) error { return session.resumeBranch(context.Background(), branch.ID) })
	}
	for buttons.retuneDn.Clicked(gtx) {
		bitrate := maxInt(16_000, branch.Bitrate/2)
		ui.withSession(func(session *session) error {
			return session.setBranchBitrate(context.Background(), branch.ID, bitrate)
		})
	}
	for buttons.retuneUp.Clicked(gtx) {
		bitrate := maxInt(16_000, branch.Bitrate+branch.Bitrate/2)
		ui.withSession(func(session *session) error {
			return session.setBranchBitrate(context.Background(), branch.ID, bitrate)
		})
	}
	for buttons.rebranch.Clicked(gtx) {
		spec := branch.branchSpec
		if spec.Kind == "video" {
			if spec.Width <= 640 {
				spec.Width, spec.Height, spec.Bitrate = 1280, 720, 1_800_000
			} else {
				spec.Width, spec.Height, spec.Bitrate = 320, 180, 320_000
			}
		} else {
			if spec.Bitrate < 96_000 {
				spec.Bitrate = 128_000
			} else {
				spec.Bitrate = 32_000
			}
		}
		ui.withSession(func(session *session) error {
			_, err := session.rebranch(context.Background(), branch.ID, spec)
			return err
		})
	}
	for buttons.remove.Clicked(gtx) {
		ui.withSession(func(session *session) error { return session.deleteBranch(context.Background(), branch.ID) })
	}
}

func (ui *controlRoom) addBranch(spec branchSpec) {
	ui.withSession(func(session *session) error {
		branch, err := session.addBranch(context.Background(), spec)
		if err != nil {
			return err
		}
		ui.message = fmt.Sprintf("added %s branch %s; browser will renegotiate", branch.Spec.Kind, branch.Spec.ID)
		return nil
	})
}

func (ui *controlRoom) requestKeyframe(kind string) {
	ui.withSession(func(session *session) error {
		if err := session.requestKeyframe(context.Background(), kind); err != nil {
			return err
		}
		ui.message = kind + " keyframe requested"
		return nil
	})
}

func (ui *controlRoom) withSession(fn func(*session) error) {
	session := ui.server.latestSession()
	if session == nil {
		ui.message = "open the browser peer and start a session first"
		return
	}
	if err := fn(session); err != nil {
		ui.message = err.Error()
	}
}

func openURL(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}

func compactStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func formatBytes(bytes uint64) string {
	switch {
	case bytes >= 1_000_000:
		return fmt.Sprintf("%.1f MB", float64(bytes)/1_000_000)
	case bytes >= 1_000:
		return fmt.Sprintf("%.1f KB", float64(bytes)/1_000)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func formatCount(v uint64) string {
	if v >= 1_000_000 {
		return fmt.Sprintf("%.1fm", float64(v)/1_000_000)
	}
	if v >= 1_000 {
		return fmt.Sprintf("%.1fk", float64(v)/1_000)
	}
	return strconv.FormatUint(v, 10)
}

func formatFPS(fps float64, status string) string {
	if fps <= 0 {
		return firstNonEmpty(status, "waiting")
	}
	return fmt.Sprintf("%.1f fps", fps)
}

func formatFrameAge(ms int64) string {
	if ms <= 0 {
		return "waiting"
	}
	if ms < 1000 {
		return fmt.Sprintf("%d ms ago", ms)
	}
	return fmt.Sprintf("%.1f s ago", float64(ms)/1000)
}

func videoStatusColor(video videoMetricsView) color.NRGBA {
	switch video.Status {
	case "live":
		return color.NRGBA{R: 23, G: 68, B: 58, A: 255}
	case "low", "waiting", "warming":
		return color.NRGBA{R: 78, G: 62, B: 30, A: 255}
	default:
		return color.NRGBA{R: 80, G: 36, B: 44, A: 255}
	}
}

func clipText(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	if max <= 3 {
		return value[:max]
	}
	return value[:max-3] + "..."
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func clampInt(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}
