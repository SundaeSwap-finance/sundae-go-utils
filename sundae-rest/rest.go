// Package sundaerest provides REST API utilities with CORS support and common middleware.
package sundaerest

import (
	"fmt"
	"net/http"
	"strings"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog"
	"github.com/savaki/apigateway"
)

func Middlewares(service sundaecli.Service, routes chi.Router) chi.Router {
	routes.Use(
		withEmbedPolicyHeaders,
		withCORS(),
		withLogger(sundaecli.Logger(service)),
		middleware.Recoverer,
	)
	return routes
}

func Webserver(service sundaecli.Service, routes chi.Router) error {
	logger := sundaecli.Logger(service)

	if sundaecli.CommonOpts.Console {
		logger.Info().Int("port", sundaecli.CommonOpts.Port).Msg("starting http server")
		addr := fmt.Sprintf(":%v", sundaecli.CommonOpts.Port)
		return http.ListenAndServe(addr, routes)
	}

	lambda.Start(apigateway.Wrap(routes, sundaecli.CommonOpts.Env))
	return nil
}

func CacheControl(handler http.HandlerFunc, maxAge int) http.HandlerFunc {
	value := fmt.Sprintf("max-age=%v", maxAge)
	return func(w http.ResponseWriter, req *http.Request) {
		req.Header.Set("Cache-Control", value)
		handler.ServeHTTP(w, req)
	}
}

func withEmbedPolicyHeaders(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		header := w.Header()
		if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/graphql") {
			handler.ServeHTTP(w, req)
			return
		}

		header.Add("cross-origin-embedder-policy", "require-corp")
		header.Add("cross-origin-opener-policy", "same-origin")
		header.Add("cross-origin-resource-policy", "cross-origin")
		handler.ServeHTTP(w, req)
	})
}

func withCORS() func(next http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
	})
}

func withLogger(logger zerolog.Logger) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := logger.WithContext(req.Context())
			req = req.WithContext(ctx)
			handler.ServeHTTP(w, req)
		})
	}
}
