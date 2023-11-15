package protocol

import (
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
	protocol, err := DecodeJSON(protocolBytes, nil)
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
        orderAddress, err := protocol.GetOrderAddress()
        if err != nil {
                t.Errorf("Failed to get order address: %v", err)
        }
        if orderAddress.String() != "addr_test1wpl692dzgmrys4e3dqusv54kr2l2utw8vxnxud37x7etz7gdry39s" {
                t.Errorf("Incorrect order address: %s", orderAddress.String())
        }
}
