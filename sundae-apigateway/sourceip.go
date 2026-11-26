// Package sundaeapigateway adapts an http.Handler to an API Gateway Lambda
// event, with the caller address taken from API Gateway itself.
//
// Behind an HTTP API (payload 2.0) the caller controls X-Forwarded-For: a
// request that sends "X-Forwarded-For: 127.0.0.1" reaches the Lambda with that
// value alone, and no real address is appended. The savaki/apigateway wrapper
// copies that header into RemoteAddr, so any ban, rate limit, or tarpit lookup
// keyed on either one reads a caller-chosen address. Wrap fixes this before the
// request is built.
package sundaeapigateway

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/savaki/apigateway"
)

// CallerForwardedForHeader holds the X-Forwarded-For value the caller sent.
// WithSourceIP replaces X-Forwarded-For with API Gateway's sourceIp and keeps
// the caller's value here, for forensics only. Never trust it as an address.
const CallerForwardedForHeader = "x-sundae-caller-forwarded-for"

// Handler is the Lambda handler shape that apigateway.Wrap returns.
type Handler = func(context.Context, apigateway.Request) (apigateway.Response, error)

// Wrap is apigateway.Wrap with WithSourceIP applied. Use it in place of
// apigateway.Wrap in lambda.Start.
func Wrap(handler http.Handler, paths ...string) Handler {
	return WithSourceIP(apigateway.Wrap(handler, paths...))
}

// WithSourceIP replaces the event's X-Forwarded-For with the TCP source that
// API Gateway records in requestContext.http.sourceIp. After it runs, the
// header and RemoteAddr both hold that one trusted address.
//
// The caller's own value moves to CallerForwardedForHeader. An event with no
// sourceIp (payload 1.0, or a test event) is passed through unchanged.
func WithSourceIP(next Handler) Handler {
	return func(ctx context.Context, event apigateway.Request) (apigateway.Response, error) {
		ip := event.RequestContext.Http.SourceIp
		if ip == "" {
			return next(ctx, event)
		}

		// The wrapper canonicalizes keys as it copies them, and map order is
		// random. Match keys case-insensitively so no second spelling of
		// either header survives to override the value set here.
		xff := http.CanonicalHeaderKey("x-forwarded-for")
		caller := http.CanonicalHeaderKey(CallerForwardedForHeader)
		headers := make(map[string]string, len(event.Headers)+1)
		var forwarded []string
		for k, v := range event.Headers {
			switch http.CanonicalHeaderKey(k) {
			case xff:
				forwarded = append(forwarded, v)
			case caller:
			default:
				headers[k] = v
			}
		}
		if len(forwarded) > 0 {
			sort.Strings(forwarded)
			headers[CallerForwardedForHeader] = strings.Join(forwarded, ",")
		}
		headers["x-forwarded-for"] = ip
		event.Headers = headers

		return next(ctx, event)
	}
}
