package protocol

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/shared"
	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
	"github.com/tj/assert"
)

func Test_DecodeProtocol(t *testing.T) {
	protocolBytes := []byte(`
          {
            "Version": "V3",
            "Environment": "foo",
            "Blueprint": {
              "Validators": [
                {
                  "Title": "order.spend",
                  "Hash": "7fa2a9a246c648573168390652b61abeae2dc761a66e363e37b2b179",
                  "CompiledCode": "000000"
                }
              ]
            },
            "References": [
				{
					"Key": "order.spend",
					"TxIn": {
						"Hash": "00000000000000000000000000000000000000000000000000000000",
						"Index": 0		
					}
				}
			],
            "Network": "testnet"
          }
        `)
	var protocol Protocol
	err := json.Unmarshal(protocolBytes, &protocol)
	assert.Nil(t, err, "Failed to decode json")
	assert.EqualValues(t, "V3", protocol.Version)
	assert.EqualValues(t, "foo", protocol.Environment)
	assert.EqualValues(t, "order.spend", protocol.Blueprint.Validators[0].Title)
	if !reflect.DeepEqual(protocol.Blueprint.Validators[0].CompiledCode, sundaegql.HexBytes{0, 0, 0}) {
		t.Errorf("Incorrect blueprint validator 0 code: %x", protocol.Blueprint.Validators[0].CompiledCode)
	}
}

func Test_GetLPToken(t *testing.T) {
	v1ProtocolBytes := []byte(`{
		"Version": "V1",
		"Environment": "foo",
		"Blueprint": {
			"Validators": [
				{
					"Title": "pool.mint",
					"Hash": "4086577ed57c514f8e29b78f42ef4f379363355a3b65b9a032ee30c9",
					"CompiledCode": "000000"
				}
			]
		},
		"References": [],
		"Network": "testnet"
	}`)

	var v1Protocol Protocol
	v1Err := json.Unmarshal(v1ProtocolBytes, &v1Protocol)
	assert.Nil(t, v1Err)

	v1LpId, err := v1Protocol.GetLPAsset("00")
	assert.Nil(t, err)
	assert.EqualValues(t, "4086577ed57c514f8e29b78f42ef4f379363355a3b65b9a032ee30c9.6c702000", v1LpId)

	v3ProtocolBytes := []byte(`
	{
		"Version": "V1",
		"Environment": "foo",
		"Blueprint": {
			"Validators": [
				{
					"Title": "pool.mint",
					"Hash": "633a136877ed6ad0ab33e69a22611319673474c8bd0a79a4c76d9289",
					"CompiledCode": "000000"
				}
			]
		},
		"References": [],
		"Network": "testnet"
	  }
	`)

	var v3Protocol Protocol
	v3Err := json.Unmarshal(v3ProtocolBytes, &v3Protocol)
	assert.Nil(t, v3Err)

	v3LpId, err := v3Protocol.GetLPAsset("1750b21414d4198763ee4d442f5c03a295a13a6028def9be4a785463")
	assert.Nil(t, err)
	assert.EqualValues(t, "633a136877ed6ad0ab33e69a22611319673474c8bd0a79a4c76d9289.6c70201750b21414d4198763ee4d442f5c03a295a13a6028def9be4a785463", v3LpId)
}

// V4 blueprints use the same dotted validator titles as earlier versions, and
// the LP / pool NFT names share V3's CIP-68 labels.
func Test_V4Protocol(t *testing.T) {
	v4ProtocolBytes := []byte(`{
		"Version": "V4",
		"Environment": "foo",
		"Blueprint": {
			"Validators": [
				{
					"Title": "pool.mint",
					"Hash": "20d919fa44c2f96e319857b14f8e6945d83ed5df054b1b7f94b35b45",
					"CompiledCode": "000000"
				}
			]
		},
		"References": [],
		"Network": "testnet"
	}`)

	var v4Protocol Protocol
	assert.Nil(t, json.Unmarshal(v4ProtocolBytes, &v4Protocol))

	const ident = "c618676e6e120cbf6742727ce06352f6e018ffcdad33a0931ef4716b"
	lpAssetId := shared.AssetID("20d919fa44c2f96e319857b14f8e6945d83ed5df054b1b7f94b35b45.0014df10" + ident)
	nftAssetId := shared.AssetID("20d919fa44c2f96e319857b14f8e6945d83ed5df054b1b7f94b35b45.000de140" + ident)

	gotLp, err := v4Protocol.GetLPAsset(ident)
	assert.Nil(t, err)
	assert.EqualValues(t, lpAssetId, gotLp)

	gotNft, err := v4Protocol.GetPoolNFT(ident)
	assert.Nil(t, err)
	assert.EqualValues(t, nftAssetId, gotNft)

	isLp, err := v4Protocol.IsLPAsset(lpAssetId)
	assert.Nil(t, err)
	assert.True(t, isLp)

	isNft, err := v4Protocol.IsPoolNFT(nftAssetId)
	assert.Nil(t, err)
	assert.True(t, isNft)

	gotIdent, ok, err := v4Protocol.GetIdent(lpAssetId)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.EqualValues(t, ident, gotIdent)

	// A foreign policy must not classify — and must not error either, so
	// Protocols iteration over a set including V4 stays usable.
	isLp, err = v4Protocol.IsLPAsset(shared.AssetID("44a1eb2d9f58add4eb1932bd0048e6a1947e85e3fe4f32956a110414.0014df10" + ident))
	assert.Nil(t, err)
	assert.False(t, isLp)

	_, ok, err = v4Protocol.GetIdent(shared.AssetID("44a1eb2d9f58add4eb1932bd0048e6a1947e85e3fe4f32956a110414.0014df10" + ident))
	assert.Nil(t, err)
	assert.False(t, ok)
}
