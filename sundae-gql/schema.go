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
