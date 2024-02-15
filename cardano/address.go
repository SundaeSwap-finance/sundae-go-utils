package cardano

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/savaki/bech32"
)

var ErrByronAddress = fmt.Errorf("cannot split byron address")

// A frontend that does validation before attempting a Bech32 decode of an address.
// There are subtle issues that could cause a panic if the address is not valid.
// For example, Byron addresses can break savaki's Bech32 library. We don't care
// about Byron, so we can just filter out non-standardized addresses.
func Bech32Decode(address string) (hrp string, data []byte, err error) {
	if strings.HasPrefix(address, "addr") || strings.HasPrefix(address, "stake") {
		return bech32.Decode(address)
	} else {
		return "", nil, fmt.Errorf("invalid address %v", address)
	}
}

func SplitAddress(address string) (paymentCredential, stakingCredential []byte, err error) {
	if !strings.HasPrefix(address, "addr") {
		return nil, nil, ErrByronAddress
	}
	_, bytes, err := Bech32Decode(address)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to decode address %v: %w", address, err)
	} else if len(bytes) < 29 {
		return nil, nil, fmt.Errorf("Bech32 address %v is too short (%v bytes)", hex.EncodeToString(bytes), len(bytes))
	}
	paymentBytes := bytes[1:29]
	stakingBytes := []byte{}
	if len(bytes) == 57 {
		stakingBytes = bytes[29:]
	}
	return paymentBytes, stakingBytes, nil
}
