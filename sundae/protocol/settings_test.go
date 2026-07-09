package protocol

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// TestSettingsUnmarshal validates the DynamoDB -> Go path for the settings list,
// including the JSON scalar's custom UnmarshalDynamoDBAttributeValue.
func TestSettingsUnmarshal(t *testing.T) {
	item := map[string]*dynamodb.AttributeValue{
		"version":     {S: aws.String("V4")},
		"environment": {S: aws.String("preview")},
		"settings": {L: []*dynamodb.AttributeValue{{M: map[string]*dynamodb.AttributeValue{
			"label": {S: aws.String("swap-order")},
			"txIn":  {M: map[string]*dynamodb.AttributeValue{"hash": {S: aws.String("aa")}, "index": {N: aws.String("0")}}},
			"datum": {S: aws.String("d87b9f")},
			"values": {M: map[string]*dynamodb.AttributeValue{
				"token":               {S: aws.String("000d039b")},
				"requiredConstraints": {L: []*dynamodb.AttributeValue{{S: aws.String("1a38df57")}, {S: aws.String("ef81595b")}}},
			}},
		}}}},
	}

	var p Protocol
	if err := dynamodbattribute.UnmarshalMap(item, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(p.Settings) != 1 || p.Settings[0].Label != "swap-order" {
		t.Fatalf("bad settings: %+v", p.Settings)
	}
	if p.Settings[0].Values == nil {
		t.Fatal("values nil")
	}
	m, ok := p.Settings[0].Values.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("values.Data not a map: %T", p.Settings[0].Values.Data)
	}
	if m["token"] != "000d039b" {
		t.Fatalf("token = %v", m["token"])
	}
	rc, ok := m["requiredConstraints"].([]interface{})
	if !ok || len(rc) != 2 {
		t.Fatalf("requiredConstraints = %v", m["requiredConstraints"])
	}
	t.Logf("OK: label=%s datum=%s values=%v", p.Settings[0].Label, p.Settings[0].Datum, m)
}
