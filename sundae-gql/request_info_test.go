package sundaegql

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithRequestInfo(t *testing.T) {
	var info RequestInfo
	handler := WithRequestInfo(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		info = RequestInfoFrom(req.Context())
	}))

	req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 198.51.100.9")
	req.Header.Set("User-Agent", "audit-bot/1.0")
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Cookie", "session=secret")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if info.ClientIP != "198.51.100.9" {
		t.Fatalf("ClientIP = %q, want the last X-Forwarded-For entry", info.ClientIP)
	}
	if info.RemoteAddr != "10.0.0.1:1234" {
		t.Fatalf("RemoteAddr = %q", info.RemoteAddr)
	}
	if info.Headers.Get("User-Agent") != "audit-bot/1.0" {
		t.Fatalf("User-Agent = %q", info.Headers.Get("User-Agent"))
	}
	if info.Headers.Get("Authorization") != "" || info.Headers.Get("Cookie") != "" {
		t.Fatalf("credentials kept in headers: %v", info.Headers)
	}
	if req.Header.Get("Authorization") == "" {
		t.Fatal("the request lost its Authorization header")
	}
}
