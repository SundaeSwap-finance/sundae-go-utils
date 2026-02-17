// Package graphiql provides an embedded GraphiQL interface for GraphQL API exploration.
package graphiql

import (
	"bytes"
	_ "embed"
	"fmt"
	"net/http"
	"text/template"
)

//go:embed graphiql.html
var graphiql string

// Options configures the GraphiQL interface.
type Options struct {
	// SubscriptionURL is the WebSocket URL for GraphQL subscriptions (e.g. "wss://ws.example.com").
	// If empty, subscriptions are disabled in the UI.
	SubscriptionURL string
}

type templateVars struct {
	Route           string
	SubscriptionURL string
}

// New creates a GraphiQL handler. endpoint is the URL of the GraphQL HTTP API.
func New(endpoint string, opts ...Options) http.HandlerFunc {
	templ, err := template.New("graphiql").Parse(graphiql)
	if err != nil {
		panic(err)
	}
	vars := templateVars{Route: endpoint}
	if len(opts) > 0 {
		vars.SubscriptionURL = opts[0].SubscriptionURL
	}
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		var buffer bytes.Buffer
		if err := templ.Execute(&buffer, vars); err != nil {
			fmt.Printf("Error: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, err := w.Write(buffer.Bytes()); err != nil {
			fmt.Printf("Error writing response: %v\n", err)
		}
	}
}
