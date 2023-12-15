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

// New endpoint is the url where you have your graphql api hosted
func New(endpoint string) http.HandlerFunc {
	templ, err := template.New("graphiql").Parse(graphiql)
	if err != nil {
		panic(err)
	}
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		var buffer bytes.Buffer
		var variables struct {
			Route string
		}
		variables.Route = endpoint
		if err := templ.Execute(&buffer, endpoint); err != nil {
			fmt.Printf("Error: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
		w.Write(buffer.Bytes())
	}
}
