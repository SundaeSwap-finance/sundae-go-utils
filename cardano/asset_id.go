package cardano

import (
	"strings"
)

const (
	AdaAssetIDString = "ada.lovelace"
)

// CanonicalizeAssetID normalizes an asset ID string to a canonical form.
//
// ADA is always normalized to "ada.lovelace". Non-ADA assets are normalized
// to "{56-char-hex-policyId}.{hex-assetName}".
//
// If the asset name is not valid hex, the input is returned unchanged
// (callers must supply hex-encoded asset names).
//
// Known ADA representations that are normalized:
//   - "" (empty string)
//   - "." (single dot)
//   - "ada.lovelace"
//   - "cardano.ada"
//   - "616461.6c6f76656c616365" (hex-encoded "ada.lovelace")
//
// For non-ADA assets with a concatenated hex string longer than 56 chars,
// a dot separator is inserted after the 56-char policy ID.
func CanonicalizeAssetID(id string) string {
	switch id {
	case "", ".", AdaAssetIDString, "cardano.ada":
		return AdaAssetIDString
	}

	// Check for hex-encoded "ada.lovelace"
	if id == "616461.6c6f76656c616365" {
		return AdaAssetIDString
	}

	if idx := strings.Index(id, "."); idx > 0 {
		policyID := id[:idx]
		assetName := id[idx+1:]

		if policyID == "ada" || policyID == "cardano" {
			return AdaAssetIDString
		}

		// Valid policy ID is 56 hex chars
		if len(policyID) == 56 && isHex(policyID) {
			if assetName == "" || isHex(assetName) {
				return id
			}
			// Asset name is not hex — return as-is rather than coercing
			return id
		}

		return id
	}

	// No dot — if it's exactly 56 hex chars, it's a policy-only asset (empty asset name)
	if len(id) == 56 && isHex(id) {
		return id
	}

	// If it's longer than 56 hex chars with no dot, insert the dot after the policy ID
	if len(id) > 56 && isHex(id) {
		return id[:56] + "." + id[56:]
	}

	return id
}

// IsAdaAssetID returns true if the given asset ID represents ADA in any known form.
func IsAdaAssetID(id string) bool {
	return CanonicalizeAssetID(id) == AdaAssetIDString
}

func isHex(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
