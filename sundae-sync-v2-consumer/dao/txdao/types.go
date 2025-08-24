package txdao

import (
	"github.com/SundaeSwap-finance/ogmigo/ouroboros/chainsync/num"
	"github.com/SundaeSwap-finance/ogmigo/ouroboros/shared"
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

type UTxO struct {
	Address string   `dynamodbav:"address"` // base64 encoded
	Coin    string   `dynamodbav:"coin"`    // lovelace
	Assets  []Policy `dynamodbav:"assets"`
	// TODO: datum, script!
}

func (u UTxO) Value() shared.Value {
	ada, ok := num.New(u.Coin)
	if !ok {
		panic("invalid utxo")
	}
	value := shared.CreateAdaValue(ada.Int64())
	for _, policy := range u.Assets {
		for _, asset := range policy.Assets {
			assetId := shared.FromSeparate(policy.PolicyID, asset.Name)
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
