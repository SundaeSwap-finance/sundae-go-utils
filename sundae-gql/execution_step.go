package sundaegql

import _ "embed"

//go:embed execution-step.graphql
var ExecutionStepSchema string

// ExecutionSteps is the shared SchemaPart defining the StepKind enum and the
// ExecutionStep type. Merge it into any subgraph that surfaces a route —
// predicted (a quote plan) or realized (a scooped order). The subgraph must
// also define `Pool` and merge the AssetAmount schema part, both of which
// ExecutionStep references.
var ExecutionSteps = SchemaPart{
	Label:  "Execution Steps",
	Schema: ExecutionStepSchema,
}

// ExecutionStep is one step of an execution route. It is generic over the
// subgraph's pool resolver (TPool) and amount type (TAmount) so this package
// needn't import them — the same pattern as sundae-api's AssetAmount[T].
// Resolve it with graphql.UseFieldResolvers(): each exported field maps to the
// like-named GraphQL field, pointers are nullable.
//
// Predicted quote steps populate Kind/Input/Output/Predecessors and leave the
// realized-only fields (Operation, PoolInputIndex, TranscriptIndex) nil.
// Realized order steps populate Operation and the on-chain coordinates and, for
// now, leave Input/Output nil (per-step attribution is deferred).
type ExecutionStep[TPool any, TAmount any] struct {
	Pool            TPool
	Kind            *string
	Operation       *string
	Input           *TAmount
	Output          *TAmount
	Predecessors    []int32
	PoolInputIndex  *int32
	TranscriptIndex *int32
}
