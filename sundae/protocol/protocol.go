package protocol

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/shared"
	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
	"github.com/savaki/bech32"
)

type ProtocolVersion string

var (
	V1 ProtocolVersion = "V1"
	V3 ProtocolVersion = "V3"
)

// TODO: ogmigo type?
type TxIn struct {
	Hash  sundaegql.HexBytes `dynamodbav:"hash"`
	Index int32              `dynamodbav:",omitempty"`
}

type Validator struct {
	Title        string             `dynamodbav:"title"`
	CompiledCode sundaegql.HexBytes `dynamodbav:"compiledCode"`
	Hash         sundaegql.HexBytes `dynamodbav:"hash"`
}

type Blueprint struct {
	Validators []Validator `dynamodbav:"validators"`
}

type ScriptReference struct {
	Key  string `dynamodbav:"key"`
	TxIn TxIn   `dynamodbav:"txIn"`
}

type Protocol struct {
	Version      ProtocolVersion   `dynamodbav:"version"`
	Environment  string            `dynamodbav:"environment"`
	Blueprint    Blueprint         `dynamodbav:"blueprint"`
	BlueprintUrl string            `dynamodbav:"-"`
	References   []ScriptReference `dynamodbav:"references"`
}

type Protocols []Protocol

func (ps Protocols) Find(version ProtocolVersion) (Protocol, bool) {
	if version == "" {
		version = V1
	}
	for _, p := range ps {
		if p.Version == version {
			return p, true
		}
	}
	return Protocol{}, false
}

func (ps Protocols) IsRelevant(address string) (Protocol, bool, error) {
	_, bb, err := bech32.Decode(address)
	if err != nil {
		return Protocol{}, false, err
	}
	payment := bb[1:29]
	for _, p := range ps {
		if p.IsRelevant(payment) {
			return p, true, nil
		}
	}
	return Protocol{}, false, nil
}

func (b Blueprint) Find(key string) (Validator, bool) {
	for _, v := range b.Validators {
		if v.Title == key {
			return v, true
		}
	}
	return Validator{}, false
}

func (p Protocol) IsRelevant(paymentCredential []byte) bool {
	for _, v := range p.Blueprint.Validators {
		if bytes.Equal(paymentCredential, v.Hash) {
			return true
		}
	}
	return false
}

func (v Validator) IsPaymentCredentialOf(address string) bool {
	_, bb, err := bech32.Decode(address)
	if err != nil {
		return false
	}
	payment := bb[1:29]
	return bytes.Equal(payment, v.Hash)
}

// V1 specific constants
const V1FactoryNFTHexName = "666163746F7279"
const V1PoolNFTHexPrefix = "7020"
const V1LPHexPrefix = "6c7020"

// V3 specific constants
const V3PoolNFTHexPrefix = "000de140"
const V3LPHexPrefix = "0014df10"

const OrderScriptKey = "order.spend"
const PoolScriptKey = "pool.spend"
const SettingsScriptKey = "settings.spend"
const StakeScriptKey = "stake.stake"

func (p *Protocol) getScript(key string) ([]byte, error) {
	for _, v := range p.Blueprint.Validators {
		if v.Title == key {
			return v.CompiledCode, nil
		}
	}
	return nil, fmt.Errorf("couldn't find '%s' script in the blueprint", key)
}

func (p *Protocol) GetOrderScript() ([]byte, error) {
	return p.getScript(OrderScriptKey)
}

func (p *Protocol) GetPoolScript() ([]byte, error) {
	return p.getScript(PoolScriptKey)
}

func (p *Protocol) GetSettingsScript() ([]byte, error) {
	return p.getScript(SettingsScriptKey)
}

func (p *Protocol) GetStakeScript() ([]byte, error) {
	return p.getScript(StakeScriptKey)
}

func (p Protocol) GetLPToken(ident string) shared.AssetID {
	poolScript, ok := p.Blueprint.Find("pool.mint")
	if !ok {
		panic("assumed pool.mint, but not found in protocol")
	}
	poolScriptHash := hex.EncodeToString(poolScript.Hash)
	switch p.Version {
	case V1:
		return shared.FromSeparate(poolScriptHash, V1LPHexPrefix+ident)
	case V3:
		return shared.FromSeparate(poolScriptHash, V3LPHexPrefix+ident)
	default:
		panic("unrecognized protocol")
	}
}
