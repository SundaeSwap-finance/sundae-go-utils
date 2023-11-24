package graphiql

import (
	_ "embed"
	"net/http"
)

//go:embed graphiql.html
var template string

// New endpoint is the url where you have your graphql api hosted
func New(endpoint string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(template))
	}
}
