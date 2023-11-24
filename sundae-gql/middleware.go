package sundaegql

import (
	"net/http"

	"github.com/go-chi/cors"
	"github.com/rs/zerolog"
)

func WithCORS() func(next http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
	})
}

func WithLogger(logger zerolog.Logger) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := logger.WithContext(req.Context())
			req = req.WithContext(ctx)
			handler.ServeHTTP(w, req)
		})
	}
}
