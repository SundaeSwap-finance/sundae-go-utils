package cardano

import (
	"testing"

	"github.com/tj/assert"
)

func TestCanonicalizeAssetID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// ADA representations
		{name: "empty string", input: "", expected: "ada.lovelace"},
		{name: "single dot", input: ".", expected: "ada.lovelace"},
		{name: "ada.lovelace", input: "ada.lovelace", expected: "ada.lovelace"},
		{name: "cardano.ada", input: "cardano.ada", expected: "ada.lovelace"},
		{name: "hex encoded ada.lovelace", input: "616461.6c6f76656c616365", expected: "ada.lovelace"},

		// Normal assets with dot separator
		{name: "normal asset with dot", input: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77.53554e444145", expected: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77.53554e444145"},

		// Normal assets without dot separator (56 + asset name hex)
		{name: "normal asset no dot", input: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d7753554e444145", expected: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77.53554e444145"},

		// Policy-only (empty asset name)
		{name: "policy only", input: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77", expected: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77"},

		// Policy with dot and empty asset name
		{name: "policy with dot empty name", input: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77.", expected: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77."},

		// Non-hex asset name is returned as-is (not coerced)
		{name: "non-hex asset name unchanged", input: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77.SUNDAE", expected: "9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77.SUNDAE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := CanonicalizeAssetID(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIsAdaAssetID(t *testing.T) {
	assert.True(t, IsAdaAssetID(""))
	assert.True(t, IsAdaAssetID("."))
	assert.True(t, IsAdaAssetID("ada.lovelace"))
	assert.True(t, IsAdaAssetID("cardano.ada"))
	assert.False(t, IsAdaAssetID("9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77.53554e444145"))
}
