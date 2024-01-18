package sundaegql

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type HexBytes []byte

func (HexBytes) ImplementsGraphQLType(name string) bool {
	return name == "HexBytes"
}

func (a *HexBytes) UnmarshalGraphQL(input interface{}) error {
	s, ok := input.(string)
	if !ok {
		return fmt.Errorf("HexBytes must be a string")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("HexBytes must be a hex string")
	}
	*a = b
	return nil
}

func (h *HexBytes) UnmarshalJSON(data []byte) error {
	var hexString string
	err := json.Unmarshal(data, &hexString)
	if err != nil {
		return err
	}
	s, err := hex.DecodeString(hexString)
	if err != nil {
		return err
	}
	*h = s
	return nil
}

func (a HexBytes) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%v\"", hex.EncodeToString(a))), nil
}

func (a HexBytes) MarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	item.S = aws.String(hex.EncodeToString(a))
	return nil
}
func (a *HexBytes) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	b, err := hex.DecodeString(aws.StringValue(item.S))
	if err != nil {
		return err
	}
	*a = b
	return nil
}
