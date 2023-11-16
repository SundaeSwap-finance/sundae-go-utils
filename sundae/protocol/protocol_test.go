package protocol

import (
	"encoding/json"
	"reflect"
	"testing"
)

func Test_DecodeProtocol(t *testing.T) {
	protocolBytes := []byte(`
          {
            "Version": "v3",
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
            "References": {
              "order.spend": {
                "Hash": "00000000000000000000000000000000000000000000000000000000",
                "Index": 0
              }
            },
            "Network": "testnet"
          }
        `)
	var protocol Protocol
	err := json.Unmarshal(protocolBytes, &protocol)
	if err != nil {
		t.Errorf("Failed to decode json: %v", err)
	}
	if protocol.Version != "v3" {
		t.Errorf("Incorrect protocol version: '%v'", protocol.Version)
	}
	if protocol.Environment != "foo" {
		t.Errorf("Incorrect environment: '%v'", protocol.Environment)
	}
	if protocol.Blueprint.Validators[0].Title != "order.spend" {
		t.Errorf("Incorrect blueprint validator 0")
	}
	if !reflect.DeepEqual(protocol.Blueprint.Validators[0].CompiledCode, []byte{0, 0, 0}) {
		t.Errorf("Incorrect blueprint validator 0 code: %x", protocol.Blueprint.Validators[0].CompiledCode)
	}
}
