package txdao

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// TestUnmarshal_SnakeCase covers the current Rust writer schema (serde default,
// snake_case: output_coin, policy_id).
func TestUnmarshal_SnakeCase(t *testing.T) {
	utxo := decodeUTxO(t, assetItem("policy_id", "output_coin", "policy-bytes", "token-bytes", "42"))
	assertUTxO(t, utxo, "policy-bytes", "token-bytes", "42")
}

// TestUnmarshal_CamelCase covers the legacy Rust writer schema (camelCase:
// outputCoin, policyId) still present on mainnet records written before the
// field-naming switch. Both must be supported — the tolerance is the whole
// point of the custom unmarshaller.
func TestUnmarshal_CamelCase(t *testing.T) {
	utxo := decodeUTxO(t, assetItem("policyId", "outputCoin", "policy-bytes", "token-bytes", "42"))
	assertUTxO(t, utxo, "policy-bytes", "token-bytes", "42")
}

func decodeUTxO(t *testing.T, item map[string]*dynamodb.AttributeValue) UTxO {
	t.Helper()
	var utxo UTxO
	if err := dynamodbattribute.UnmarshalMap(item, &utxo); err != nil {
		t.Fatalf("UnmarshalMap: %v", err)
	}
	return utxo
}

func assetItem(policyKey, coinKey, policyID, name, coin string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"address": {S: aws.String("addr-bytes")},
		"coin":    {N: aws.String("1500000")},
		"assets": {L: []*dynamodb.AttributeValue{
			{M: map[string]*dynamodb.AttributeValue{
				policyKey: {S: aws.String(policyID)},
				"assets": {L: []*dynamodb.AttributeValue{
					{M: map[string]*dynamodb.AttributeValue{
						"name": {S: aws.String(name)},
						coinKey: {S: aws.String(coin)},
					}},
				}},
			}},
		}},
	}
}

func assertUTxO(t *testing.T, utxo UTxO, wantPolicy, wantName, wantCoin string) {
	t.Helper()
	if len(utxo.Assets) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(utxo.Assets))
	}
	if utxo.Assets[0].PolicyID != wantPolicy {
		t.Errorf("PolicyID = %q, want %q", utxo.Assets[0].PolicyID, wantPolicy)
	}
	if len(utxo.Assets[0].Assets) != 1 {
		t.Fatalf("expected 1 asset, got %d", len(utxo.Assets[0].Assets))
	}
	if utxo.Assets[0].Assets[0].Name != wantName {
		t.Errorf("Name = %q, want %q", utxo.Assets[0].Assets[0].Name, wantName)
	}
	if utxo.Assets[0].Assets[0].OutputCoin.Value != wantCoin {
		t.Errorf("OutputCoin = %q, want %q", utxo.Assets[0].Assets[0].OutputCoin.Value, wantCoin)
	}
}
