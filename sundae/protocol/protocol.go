package protocol

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/SundaeSwap-finance/apollo/serialization/Address"
)

type SundaeVersion uint8

const (
	SundaeV1 SundaeVersion = iota
	SundaeV3
)

func (o *SundaeVersion) UnmarshalJSON(data []byte) error {
	var val string
	err := json.Unmarshal(data, &val)
	if err != nil {
		return fmt.Errorf("Couldn't unmarshal SundaeVersion: %w", err)
	}
	if val == "v1" {
		*o = SundaeV1
	} else if val == "v3" {
		*o = SundaeV3
	} else {
		return fmt.Errorf("Couldn't unmarshal SundaeVersion from string: %s", val)
	}
	return nil
}

func FormatSundaeVersion(v SundaeVersion) string {
	switch v {
	case SundaeV1:
		return "v1"
	case SundaeV3:
		return "v3"
	default:
		return ""
	}
}

type ObjectType uint8

const (
	EscrowObject ObjectType = iota
	PoolObject
	FactoryObject
	NoObject
)

type TxIn struct {
	Hash  []byte
	Index int
}

func (t *TxIn) UnmarshalJSON(data []byte) error {
	var m struct {
		Hash  string
		Index int
	}
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}
	hash := m.Hash
	index := m.Index
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	t.Hash = hashBytes
	t.Index = index
	return nil
}

type Protocol struct {
	Version         SundaeVersion
	EscrowAddr      string
	PoolAddr        string
	FactoryAddr     string
	LicensePolicyID string
	PoolPolicyID    string
	StakeAddr       string
	StakeValidator  string
	ScooperSkey     string
	ScooperVkey     string
	References      []TxIn
}

func (p *Protocol) PoolNFT(poolIdent string) (string, error) {
	addr, err := Address.DecodeAddress(p.PoolAddr)
	if err != nil {
		return "", err
	}
	poolScriptHash := hex.EncodeToString(addr.PaymentPart)
	return poolScriptHash + ".70" + poolIdent, nil
}
