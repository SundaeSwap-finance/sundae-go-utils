package protocol

import (
	"encoding/json"
	"reflect"
	"testing"

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
