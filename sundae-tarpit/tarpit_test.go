package tarpit

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newRequest(path, xff string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.RemoteAddr = "10.0.0.1:5555"
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	return r
}

// serve runs one request through the middleware and a passthrough handler that
// records whether it ran. It returns the response and that flag.
func serve(mw func(http.Handler) http.Handler, r *http.Request) (*http.Response, bool) {
	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusTeapot)
	})
	rec := httptest.NewRecorder()
	mw(next).ServeHTTP(rec, r)
	return rec.Result(), reached
}

func TestBaitIPGetsBaitAndSkipsHandler(t *testing.T) {
	mw := Middleware(NewMemorySource("1.2.3.4"), Config{Body: "BAIT"})
	resp, reached := serve(mw, newRequest("/anything", "1.2.3.4"))

	if reached {
		t.Fatal("handler ran for a bait IP; it must not")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "BAIT" {
		t.Fatalf("body = %q, want %q", body, "BAIT")
	}
}

func TestNonBaitIPPassesThrough(t *testing.T) {
	mw := Middleware(NewMemorySource("1.2.3.4"), Config{Body: "BAIT"})
	resp, reached := serve(mw, newRequest("/anything", "9.9.9.9"))

	if !reached {
		t.Fatal("handler did not run for a non-bait IP; it must")
	}
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, want 418 from the handler", resp.StatusCode)
	}
}

func TestExemptPathReachesHandlerEvenForBaitIP(t *testing.T) {
	mw := Middleware(NewMemorySource("1.2.3.4"), Config{
		Body:        "BAIT",
		ExemptPaths: []string{"/assessment/submit"},
	})
	resp, reached := serve(mw, newRequest("/assessment/submit", "1.2.3.4"))

	if !reached {
		t.Fatal("handler did not run for an exempt path; the honeypot must capture the submission")
	}
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, want 418 from the handler", resp.StatusCode)
	}
}

func TestClientIPUsesLastForwardedForEntry(t *testing.T) {
	// The last X-Forwarded-For entry wins: API Gateway appends the real source
	// there, and a caller cannot forge past it by prepending entries.
	if got := ClientIP(newRequest("/", "203.0.113.7, 200.181.82.126")); got != "200.181.82.126" {
		t.Fatalf("ClientIP = %q, want 200.181.82.126 (a prepended forged entry must be ignored)", got)
	}
	// A single entry (no forged prefix) resolves to that entry.
	if got := ClientIP(newRequest("/", "1.2.3.4")); got != "1.2.3.4" {
		t.Fatalf("ClientIP = %q, want 1.2.3.4", got)
	}
	// With no header, fall back to RemoteAddr with the port stripped.
	if got := ClientIP(newRequest("/", "")); got != "10.0.0.1" {
		t.Fatalf("ClientIP = %q, want 10.0.0.1", got)
	}
}

func TestNilSourceDisablesMiddleware(t *testing.T) {
	mw := Middleware(nil, Config{Body: "BAIT"})
	_, reached := serve(mw, newRequest("/anything", "1.2.3.4"))
	if !reached {
		t.Fatal("handler did not run with a nil source; the middleware must be a no-op")
	}
}

func TestDelayEndsOnClientDisconnect(t *testing.T) {
	mw := Middleware(NewMemorySource("1.2.3.4"), Config{Body: "BAIT", Delay: 2 * time.Second})
	r := newRequest("/anything", "1.2.3.4")
	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)
	cancel() // the caller has already disconnected

	start := time.Now()
	rec := httptest.NewRecorder()
	mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rec, r)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("delay did not end on disconnect; took %v", elapsed)
	}
}
