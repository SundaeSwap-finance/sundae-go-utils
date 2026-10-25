package sundaegql

import (
	"context"
	"net/http"

	tarpit "github.com/SundaeSwap-finance/sundae-go-utils/sundae-tarpit"
)

// RequestInfo records who sent a request, so a resolver can log a rejected call.
type RequestInfo struct {
	ClientIP   string
	RemoteAddr string
	Headers    http.Header
}

type requestInfoKey struct{}

func WithRequestInfo(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		headers := req.Header.Clone()
		// Keep stray credentials out of the logs.
		headers.Del("Authorization")
		headers.Del("Cookie")
		info := RequestInfo{
			ClientIP:   tarpit.ClientIP(req),
			RemoteAddr: req.RemoteAddr,
			Headers:    headers,
		}
		next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), requestInfoKey{}, info)))
	})
}

func RequestInfoFrom(ctx context.Context) RequestInfo {
	info, _ := ctx.Value(requestInfoKey{}).(RequestInfo)
	return info
}
