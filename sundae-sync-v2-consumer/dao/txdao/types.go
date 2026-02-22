package txdao

import (
	"encoding/base64"
	"encoding/hex"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync/num"
	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/shared"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Datum struct {
	Hash         string `dynamodbav:"hash"`         // base64 encoded
	OriginalCbor string `dynamodbav:"originalCbor"` // base64 encoded
	Payload      dynamodb.AttributeValue
}

type Asset struct {
	Name       string `dynamodbav:"name"` // base64 encoded token name
	OutputCoin string `dynamodbav:"outputCoin"`
}

type Policy struct {
	PolicyID string  `dynamodbav:"policyId"` // base64 encoded
	Assets   []Asset `dynamodbav:"assets"`
}

// DatumField handles two DynamoDB formats for the datum field:
//   - Legacy: a plain base64 string of the CBOR bytes
//   - Current: a Map with "originalCbor" (base64), "hash", and "payload" keys
type DatumField struct {
	B64 string // base64-encoded datum CBOR, populated from either format
}

func (d *DatumField) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if item == nil {
		return nil
	}
	if item.S != nil {
		d.B64 = *item.S
		return nil
	}
	if item.M != nil {
		if oc, ok := item.M["originalCbor"]; ok && oc.S != nil {
			d.B64 = *oc.S
		}
		return nil
	}
	return nil
}

type UTxO struct {
	Address string     `dynamodbav:"address"`         // base64 encoded
	Coin    string     `dynamodbav:"coin"`            // lovelace
	Assets  []Policy   `dynamodbav:"assets"`
	Datum   DatumField `dynamodbav:"datum,omitempty"`
}

// DatumCBOR returns the decoded datum CBOR bytes, or nil if not present.
func (u UTxO) DatumCBOR() []byte {
	if u.Datum.B64 == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(u.Datum.B64)
	if err != nil {
		return nil
	}
	return b
}

func (u UTxO) Value() shared.Value {
	ada, ok := num.New(u.Coin)
	if !ok {
		panic("invalid utxo")
	}
	value := shared.CreateAdaValue(ada.Int64())
	for _, policy := range u.Assets {
		policyBytes, err := base64.StdEncoding.DecodeString(policy.PolicyID)
		if err != nil {
			panic("invalid policy base64: " + policy.PolicyID)
		}
		policyHex := hex.EncodeToString(policyBytes)
		for _, asset := range policy.Assets {
			nameBytes, err := base64.StdEncoding.DecodeString(asset.Name)
			if err != nil {
				panic("invalid asset name base64: " + asset.Name)
			}
			nameHex := hex.EncodeToString(nameBytes)
			assetId := shared.FromSeparate(policyHex, nameHex)
			qty, ok := num.New(asset.OutputCoin)
			if !ok {
				panic("invalid utxo")
			}
			value.AddAsset(shared.Coin{AssetId: assetId, Amount: qty})
		}
	}
	return value
}

type Tx struct {
	Pk         string `dynamodbav:"pk" ddb:"hash"`
	Sk         string `dynamodbav:"sk" ddb:"range"`
	Block      string `dynamodbav:"block"`
	InChain    bool   `dynamodbav:"in_chain"`
	Location   string `dynamodbav:"location"`
	Successful bool   `dynamodbav:"successful"`
	Utxos      []UTxO `dynamodbav:"utxos"`
	Collateral UTxO   `dynamodbav:"collateral_out"`
}
