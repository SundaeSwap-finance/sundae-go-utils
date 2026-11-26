package sundaeapigateway

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/savaki/apigateway"

	tarpit "github.com/SundaeSwap-finance/sundae-go-utils/sundae-tarpit"
)

// seen records what a handler behind Wrap observes.
type seen struct {
	clientIP   string
	remoteAddr string
	caller     string
}

func serve(t *testing.T, event apigateway.Request) seen {
	t.Helper()
	var got seen
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = seen{
			clientIP:   tarpit.ClientIP(r),
			remoteAddr: r.RemoteAddr,
			caller:     strings.Join(r.Header.Values(CallerForwardedForHeader), ","),
		}
	})
	if _, err := Wrap(h)(context.Background(), event); err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	return got
}

func v2Event(sourceIP string, headers map[string]string) apigateway.Request {
	event := apigateway.Request{Version: "2.0", RawPath: "/graphql", Headers: headers}
	event.RequestContext.DomainName = "api.example"
	event.RequestContext.Http.Method = http.MethodPost
	event.RequestContext.Http.SourceIp = sourceIP
	return event
}

func TestWithSourceIP(t *testing.T) {
	const real = "162.245.239.66"
	cases := []struct {
		name       string
		headers    map[string]string
		wantCaller string
	}{
		{name: "forged single value", headers: map[string]string{"x-forwarded-for": "127.0.0.1"}, wantCaller: "127.0.0.1"},
		{name: "forged list", headers: map[string]string{"x-forwarded-for": "10.0.0.1, 172.17.0.1"}, wantCaller: "10.0.0.1, 172.17.0.1"},
		{name: "mixed-case duplicate", headers: map[string]string{"x-forwarded-for": "127.0.0.1", "X-Forwarded-For": "10.0.0.1"}, wantCaller: "10.0.0.1,127.0.0.1"},
		{name: "forged caller header", headers: map[string]string{CallerForwardedForHeader: "1.2.3.4"}, wantCaller: ""},
		{name: "no header", headers: nil, wantCaller: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := serve(t, v2Event(real, tc.headers))
			if got.clientIP != real {
				t.Errorf("clientIP: got %q; want %q", got.clientIP, real)
			}
			if got.remoteAddr != real {
				t.Errorf("remoteAddr: got %q; want %q", got.remoteAddr, real)
			}
			// The wrapper splits comma lists into separate values; compare
			// after removing that whitespace difference.
			if norm(got.caller) != norm(tc.wantCaller) {
				t.Errorf("caller header: got %q; want %q", got.caller, tc.wantCaller)
			}
		})
	}
}

func TestWithSourceIPNoSourcePassesThrough(t *testing.T) {
	got := serve(t, v2Event("", map[string]string{"x-forwarded-for": "127.0.0.1"}))
	if got.clientIP != "127.0.0.1" {
		t.Errorf("clientIP: got %q; want %q", got.clientIP, "127.0.0.1")
	}
}

func norm(s string) string {
	return strings.ReplaceAll(s, " ", "")
}
