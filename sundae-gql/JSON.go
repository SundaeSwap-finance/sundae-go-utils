package sundaegql

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type JSON struct {
	Data interface{}
}

func FromRaw(raw json.RawMessage) (JSON, error) {
	m := map[string]interface{}{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return JSON{}, err
	}
	return JSON{Data: m}, nil
}

func (JSON) ImplementsGraphQLType(name string) bool {
	return name == "JSON"
}

func (a *JSON) UnmarshalGraphQL(input interface{}) error {
	a.Data = input
	return nil
}

func (a JSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Data)
}

// MarshalDynamoDBAttributeValue stores the wrapped Data as a native DynamoDB
// value (Map, List, etc.), so JSON round-trips without a JSON-in-string blob.
func (a JSON) MarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	av, err := dynamodbattribute.Marshal(a.Data)
	if err != nil {
		return err
	}
	*item = *av
	return nil
}

// UnmarshalDynamoDBAttributeValue decodes a native DynamoDB value into Data.
func (a *JSON) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	return dynamodbattribute.Unmarshal(item, &a.Data)
}
