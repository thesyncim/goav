package ctlserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPAuthorizationRequiresBearerPrefix(t *testing.T) {
	const tok = "0123456789abcdef0123"
	s := &Server{token: tok, methods: map[string]*method{}}

	// Raw token without "Bearer " prefix must be rejected.
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Authorization", tok)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("raw token status = %d, want 401", rec.Code)
	}

	// Proper "Bearer <token>" header passes auth (reaches JSON-RPC handler).
	req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req2.Header.Set("Authorization", "Bearer "+tok)
	rec2 := httptest.NewRecorder()
	s.ServeHTTP(rec2, req2)
	if rec2.Code == http.StatusUnauthorized {
		t.Fatalf("Bearer token got 401, want authorized")
	}
}

func TestURIsOptionGrantsSchemeToCompile(t *testing.T) {
	s := &Server{methods: map[string]*method{}}
	if err := URIs("https")(s); err != nil {
		t.Fatal(err)
	}
	if _, err := s.compile(`uri "https://example.invalid/clip.webm" ! video ! null`); err != nil {
		t.Fatalf("compile with URIs(https) failed: %v", err)
	}
	if _, err := s.compile(`uri "http://example.invalid/clip.webm" ! video ! null`); err == nil {
		t.Fatal("compile with ungranted http scheme succeeded, want error")
	}
}
