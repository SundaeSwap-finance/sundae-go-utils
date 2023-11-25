package sundaegql

import (
	_ "embed"
)

type SchemaPart struct {
	Label  string
	Schema string
}

//go:embed common.gql
var CommonSchema string
var Common = SchemaPart{
	Label:  "Common Types",
	Schema: CommonSchema,
}

func MergeSchemas(base string, schemas ...SchemaPart) string {
	for _, part := range schemas {
		base += "\n\n# " + part.Label + "\n\n" + part.Schema
	}
	return base
}
