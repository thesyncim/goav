package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStudioEverythingEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	t.Setenv("GOAV_STUDIO_RECORDINGS_DIR", tmpDir)

	st := newStudio("http://127.0.0.1:8080")
	if err := st.Start(ctx); err != nil {
		t.Fatalf("studio Start: %v", err)
	}
	defer st.Close()

	// 1. Wait for live program frames and default VP8 simulcast branches.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap := st.Snapshot()
		if snap.Program.Frames >= 5 && len(snap.Branches) == 2 && snap.Branches[0].Packets > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	snap := st.Snapshot()
	if snap.Program.Frames < 5 {
		t.Fatalf("expected >=5 program frames, got %d", snap.Program.Frames)
	}
	if len(snap.Graph.Nodes) == 0 {
		t.Fatal("expected non-empty combined scene + program graph")
	}

	// 2. Start recording WebM, then switch scenes MULTIPLE times while recording stays active!
	rec, err := st.StartRecording(ctx, "webm", "vp8", 500_000)
	if err != nil {
		t.Fatalf("StartRecording webm: %v", err)
	}
	if !rec.Active {
		t.Fatal("expected recording active")
	}

	pktsBeforeSwitch := st.Snapshot().Branches[0].Packets
	taskBefore := st.currentTask()

	for _, sc := range []string{"pip", "sidebyside", "screen", "pattern"} {
		if err := st.UpdateConfig(ctx, studioConfig{
			Scene:         sc,
			PiPCorner:     "br",
			Width:         960,
			Height:        540,
			FPS:           30,
			CaptureCursor: true,
		}); err != nil {
			t.Fatalf("switch scene %q while recording: %v", sc, err)
		}
		if st.currentTask() != taskBefore {
			t.Fatalf("pipeline restarted on scene switch to %q; expected control.SelectActive on same Task!", sc)
		}
		time.Sleep(150 * time.Millisecond)
		cur := st.Snapshot()
		if !cur.Recording.Active {
			t.Fatalf("recording stopped unexpectedly after switching to scene %q!", sc)
		}
		if cur.LastError != "" {
			t.Fatalf("unexpected error after scene %q: %s", sc, cur.LastError)
		}
	}

	if after := st.Snapshot().Branches[0].Packets; after <= pktsBeforeSwitch {
		t.Fatalf("expected WebRTC branch packets to keep flowing across scene switches (%d -> %d)", pktsBeforeSwitch, after)
	}

	finalWebM, err := st.StopRecording(ctx)
	if err != nil {
		t.Fatalf("StopRecording webm: %v", err)
	}
	if finalWebM.Bytes <= 0 {
		t.Fatalf("expected non-empty WebM file after scene cuts, got %d bytes", finalWebM.Bytes)
	}

	// 3. Verify recording can be restarted in MKV and MP4 on the same running task!
	for _, tc := range []struct{ format, codec string }{
		{"mkv", "vp9"},
		{"mp4", "vp9"},
	} {
		if _, err := st.StartRecording(ctx, tc.format, tc.codec, 500_000); err != nil {
			t.Fatalf("StartRecording %s/%s: %v", tc.format, tc.codec, err)
		}
		time.Sleep(350 * time.Millisecond)
		fin, err := st.StopRecording(ctx)
		if err != nil {
			t.Fatalf("StopRecording %s/%s: %v", tc.format, tc.codec, err)
		}
		if fin.Bytes <= 0 {
			t.Fatalf("expected non-empty %s recording, got %d bytes", tc.format, fin.Bytes)
		}
	}

	// 4. Hot-attach VP9 & AV1 branches, pause/resume, bitrate, rebranch, detach!
	for _, codecName := range []string{"vp9", "av1"} {
		id := codecName + "-test"
		if _, err := st.AddBranch(ctx, branchSpec{
			ID:      id,
			Codec:   codecName,
			Width:   320,
			Height:  180,
			FPS:     30,
			Bitrate: 300_000,
		}); err != nil {
			t.Fatalf("AddBranch %s: %v", codecName, err)
		}
		if err := st.PauseBranch(ctx, id, true); err != nil {
			t.Fatalf("PauseBranch %s: %v", codecName, err)
		}
		if err := st.PauseBranch(ctx, id, false); err != nil {
			t.Fatalf("ResumeBranch %s: %v", codecName, err)
		}
		if err := st.SetBitrate(ctx, id, 420_000); err != nil {
			t.Fatalf("SetBitrate %s: %v", codecName, err)
		}
		if err := st.Rebranch(ctx, id, 480, 270, 500_000); err != nil {
			t.Fatalf("Rebranch %s: %v", codecName, err)
		}
		if err := st.RemoveBranch(ctx, id); err != nil {
			t.Fatalf("RemoveBranch %s: %v", codecName, err)
		}
	}

	// 5. LL-HLS live origin: start, switch scene, stop, restart!
	if err := st.ToggleHLS(ctx, true, 600_000); err != nil {
		t.Fatalf("ToggleHLS on: %v", err)
	}
	if err := st.UpdateConfig(ctx, studioConfig{Scene: "pip", Width: 960, Height: 540, FPS: 30}); err != nil {
		t.Fatalf("scene switch with HLS active: %v", err)
	}
	if !st.Snapshot().HLS.Active {
		t.Fatal("expected HLS to stay active across scene switch")
	}
	if err := st.ToggleHLS(ctx, false, 0); err != nil {
		t.Fatalf("ToggleHLS off: %v", err)
	}
	if err := st.ToggleHLS(ctx, true, 600_000); err != nil {
		t.Fatalf("ToggleHLS restart: %v", err)
	}
	if err := st.ToggleHLS(ctx, false, 0); err != nil {
		t.Fatalf("ToggleHLS off 2: %v", err)
	}

	// 6. Verify HTTP routes, codegen, and WebRTC offer signaling.
	srv := newStudioServer(st)
	mux := http.NewServeMux()
	srv.routes(mux)

	for _, path := range []string{"/", "/watch", "/api/state"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s returned %d", path, w.Code)
		}
	}
	offerReq := httptest.NewRequest(http.MethodPost, "/api/webrtc/offer", bytes.NewBufferString(`{}`))
	offerW := httptest.NewRecorder()
	mux.ServeHTTP(offerW, offerReq)
	if offerW.Code != http.StatusOK {
		t.Fatalf("POST /api/webrtc/offer returned %d: %s", offerW.Code, offerW.Body.String())
	}
	var offerResp struct {
		ViewerID string `json:"viewerId"`
		SDP      string `json:"sdp"`
	}
	if err := json.Unmarshal(offerW.Body.Bytes(), &offerResp); err != nil {
		t.Fatalf("decode offer: %v", err)
	}
	if offerResp.ViewerID == "" || !strings.Contains(offerResp.SDP, "v=0") {
		t.Fatalf("invalid WebRTC offer response: %+v", offerResp)
	}
}
