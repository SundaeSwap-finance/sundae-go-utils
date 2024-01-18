package protocol

import (
	"encoding/json"
	"reflect"
	"testing"

	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
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
	if err != nil {
		t.Errorf("Failed to decode json: %v", err)
	}
	if protocol.Version != "V3" {
		t.Errorf("Incorrect protocol version: '%v'", protocol.Version)
	}
	if protocol.Environment != "foo" {
		t.Errorf("Incorrect environment: '%v'", protocol.Environment)
	}
	if protocol.Blueprint.Validators[0].Title != "order.spend" {
		t.Errorf("Incorrect blueprint validator 0")
	}
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
	if v1Err != nil {
		t.Errorf("Failed to decode json: %v", v1Err)
	}

	v1LpId := v1Protocol.GetLPToken("00")
	if v1LpId != "4086577ed57c514f8e29b78f42ef4f379363355a3b65b9a032ee30c9.6c702000" {
		t.Errorf("Incorrect LP Token returned: %v", v1LpId)
	}

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

	if v3Err != nil {
		t.Errorf("Failed to decode json: %v", v3Err)
	}

	v3LpId := v3Protocol.GetLPToken("1750b21414d4198763ee4d442f5c03a295a13a6028def9be4a785463")
	if v3LpId != "633a136877ed6ad0ab33e69a22611319673474c8bd0a79a4c76d9289.6c70201750b21414d4198763ee4d442f5c03a295a13a6028def9be4a785463" {
		t.Errorf("Incorrect LP Token returned: %v", v3LpId)
	}
}
