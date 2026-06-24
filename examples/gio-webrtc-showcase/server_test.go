package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thesyncim/goav/std"
)

func TestHTTPBranchLifecycleQueuesRenegotiation(t *testing.T) {
	app := newServer(std.New(), "http://localhost:8080")
	session, err := newSession(context.Background(), app.runtime, app.browserURL)
	if err != nil {
		t.Fatalf("newSession(): %v", err)
	}
	defer session.Close()
	app.sessions[session.id] = session

	mux := http.NewServeMux()
	app.routes(mux)

	addBody := bytes.NewBufferString(`{"kind":"audio","codec":"opus","bitrate":48000}`)
	addReq := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.id+"/branches", addBody)
	addRec := httptest.NewRecorder()
	mux.ServeHTTP(addRec, addReq)
	if addRec.Code != http.StatusOK {
		t.Fatalf("add status = %d body=%s", addRec.Code, addRec.Body.String())
	}
	var add addBranchResponse
	if err := json.Unmarshal(addRec.Body.Bytes(), &add); err != nil {
		t.Fatalf("decode add: %v", err)
	}
	if !add.NeedsRenegotiate || add.Branch.Kind != "audio" || add.Branch.Codec != "opus" {
		t.Fatalf("add response = %+v", add)
	}

	state := session.State()
	if state.Renegotiate.Seq == 0 || len(state.Renegotiate.Kinds) == 0 {
		t.Fatalf("renegotiate = %+v", state.Renegotiate)
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/api/sessions/"+session.id+"/branches/"+add.Branch.ID, nil)
	delRec := httptest.NewRecorder()
	mux.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d body=%s", delRec.Code, delRec.Body.String())
	}
}

func TestHTTPScenariosRefreshState(t *testing.T) {
	app := newServer(std.New(), "http://localhost:8080")
	session, err := newSession(context.Background(), app.runtime, app.browserURL)
	if err != nil {
		t.Fatalf("newSession(): %v", err)
	}
	defer session.Close()
	app.sessions[session.id] = session

	mux := http.NewServeMux()
	app.routes(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.id+"/scenarios", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("scenario status = %d body=%s", rec.Code, rec.Body.String())
	}
	var results []scenarioResult
	if err := json.Unmarshal(rec.Body.Bytes(), &results); err != nil {
		t.Fatalf("decode scenarios: %v", err)
	}
	if len(results) == 0 || len(session.State().Scenarios) == 0 {
		t.Fatalf("empty scenarios: response=%+v state=%+v", results, session.State().Scenarios)
	}
}
