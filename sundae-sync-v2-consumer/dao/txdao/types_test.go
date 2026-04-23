package txdao

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// TestUnmarshal_RustWriterSchema locks the dynamodbav struct tags against the
// field names produced by the Rust sundae-sync-v2 writer (serde default =
// snake_case). Previously this package used camelCase tags (outputCoin /
// policyId) which silently produced empty asset values and panicked in Value().
func TestUnmarshal_RustWriterSchema(t *testing.T) {
	item := map[string]*dynamodb.AttributeValue{
		"address": {S: aws.String("addr-bytes")},
		"coin":    {N: aws.String("1500000")},
		"assets": {L: []*dynamodb.AttributeValue{
			{M: map[string]*dynamodb.AttributeValue{
				"policy_id": {S: aws.String("policy-bytes")},
				"redeemer":  {NULL: aws.Bool(true)},
				"assets": {L: []*dynamodb.AttributeValue{
					{M: map[string]*dynamodb.AttributeValue{
						"name":        {S: aws.String("token-bytes")},
						"output_coin": {S: aws.String("42")},
					}},
				}},
			}},
		}},
	}

	var utxo UTxO
	if err := dynamodbattribute.UnmarshalMap(item, &utxo); err != nil {
		t.Fatalf("UnmarshalMap: %v", err)
	}

	if len(utxo.Assets) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(utxo.Assets))
	}
	if utxo.Assets[0].PolicyID != "policy-bytes" {
		t.Errorf("PolicyID = %q, want %q", utxo.Assets[0].PolicyID, "policy-bytes")
	}
	if len(utxo.Assets[0].Assets) != 1 {
		t.Fatalf("expected 1 asset, got %d", len(utxo.Assets[0].Assets))
	}
	if utxo.Assets[0].Assets[0].OutputCoin.Value != "42" {
		t.Errorf("OutputCoin = %q, want %q", utxo.Assets[0].Assets[0].OutputCoin.Value, "42")
	}
}
