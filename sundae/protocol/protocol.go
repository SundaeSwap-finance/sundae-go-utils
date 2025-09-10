package protocol

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/SundaeSwap-finance/ogmigo/ouroboros/shared"
	"github.com/SundaeSwap-finance/sundae-go-utils/cardano"
	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
)

type ProtocolVersion string

var (
	V1          ProtocolVersion = "V1"
	V3          ProtocolVersion = "V3"
	Stableswaps ProtocolVersion = "Stableswaps"
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
	payment, _, err := cardano.SplitAddress(address)
	if err != nil {
		if err == cardano.ErrByronAddress || err == cardano.ErrStakeAddress {
			return Protocol{}, false, nil
		}
		return Protocol{}, false, err
	}
	for _, p := range ps {
		if p.IsRelevant(payment) {
			return p, true, nil
		}
	}
	return Protocol{}, false, nil
}

func (ps Protocols) IsLPAsset(assetId shared.AssetID) (Protocol, bool, error) {
	for _, p := range ps {
		if ok, err := p.IsLPAsset(assetId); ok {
			return p, true, nil
		} else if err != nil {
			return Protocol{}, false, err
		}
	}
	return Protocol{}, false, nil
}

func (ps Protocols) PoolIdent(assetId shared.AssetID) (string, bool, Protocol, error) {
	for _, p := range ps {
		if ident, ok, err := p.GetIdent(assetId); ok {
			return ident, true, p, nil
		} else if err != nil {
			return "", false, Protocol{}, err
		}
	}
	return "", false, Protocol{}, nil
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
	// Sanity check to prevent things like Byron addresses from being processed.
	payment, _, err := cardano.SplitAddress(address)
	if err != nil {
		return false
	}
	return bytes.Equal(payment, v.Hash)
}

func (p Protocol) GetPoolNFT(ident string) (shared.AssetID, error) {
	poolScript, ok := p.Blueprint.Find("pool.mint")
	if !ok {
		return "", fmt.Errorf("pool.mint not found in protocol: %v", p.Version)
	}
	poolScriptHash := hex.EncodeToString(poolScript.Hash)
	switch p.Version {
	case V1:
		return shared.FromSeparate(poolScriptHash, V1PoolNFTHexPrefix+ident), nil
	case V3:
		return shared.FromSeparate(poolScriptHash, V3PoolNFTHexPrefix+ident), nil
	default:
		return "", fmt.Errorf("unrecognized protocol version %v", p.Version)
	}
}
func (p Protocol) MustGetPoolNFT(ident string) shared.AssetID {
	assetId, err := p.GetPoolNFT(ident)
	if err != nil {
		panic(err)
	}
	return assetId
}

func (p Protocol) IsPoolNFT(assetId shared.AssetID) (bool, error) {
	poolMint, ok := p.Blueprint.Find("pool.mint")
	if !ok {
		return false, fmt.Errorf("pool.mint not found in protocol %v", p.Version)
	}
	if hex.EncodeToString(poolMint.Hash) != assetId.PolicyID() {
		return false, nil
	}
	switch p.Version {
	case V1:
		return strings.HasPrefix(assetId.AssetName(), V1PoolNFTHexPrefix), nil
	case V3:
		return strings.HasPrefix(assetId.AssetName(), V3PoolNFTHexPrefix), nil
	default:
		return false, fmt.Errorf("unrecognized protocol version %v", p.Version)
	}
}

func (p Protocol) HasPoolNFT(value shared.Value) (bool, error) {
	for policyId, assets := range value {
		for assetName := range assets {
			assetId := shared.FromSeparate(policyId, assetName)
			if ok, err := p.IsPoolNFT(assetId); ok {
				return true, nil
			} else if err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

func (p Protocol) GetLPAsset(ident string) (shared.AssetID, error) {
	poolScript, ok := p.Blueprint.Find("pool.mint")
	if !ok {
		return "", fmt.Errorf("pool.mint not found in protocol %v", p.Version)
	}
	poolScriptHash := hex.EncodeToString(poolScript.Hash)
	switch p.Version {
	case V1:
		return shared.FromSeparate(poolScriptHash, V1LPHexPrefix+ident), nil
	case V3:
		return shared.FromSeparate(poolScriptHash, V3LPHexPrefix+ident), nil
	default:
		return "", fmt.Errorf("unrecognized protocol version %v", p.Version)
	}
}
func (p Protocol) MustGetLPAsset(ident string) shared.AssetID {
	assetId, err := p.GetLPAsset(ident)
	if err != nil {
		panic(err)
	}
	return assetId
}

func (p Protocol) IsLPAsset(assetId shared.AssetID) (bool, error) {
	poolMint, ok := p.Blueprint.Find("pool.mint")
	if !ok {
		return false, fmt.Errorf("pool.mint not found in protocol %v", p.Version)
	}
	if hex.EncodeToString(poolMint.Hash) != assetId.PolicyID() {
		return false, nil
	}
	switch p.Version {
	case V1:
		return strings.HasPrefix(assetId.AssetName(), V1LPHexPrefix), nil
	case V3:
		return strings.HasPrefix(assetId.AssetName(), V3LPHexPrefix), nil
	default:
		return false, fmt.Errorf("unrecognized protocol version %v", p.Version)
	}
}

func (p Protocol) GetIdent(assetId shared.AssetID) (string, bool, error) {
	poolMint, ok := p.Blueprint.Find("pool.mint")
	if !ok {
		return "", false, fmt.Errorf("pool.mint not found in protocol %v", p.Version)
	}
	if hex.EncodeToString(poolMint.Hash) != assetId.PolicyID() {
		return "", false, nil
	}
	switch p.Version {
	case V1:
		switch {
		case strings.HasPrefix(assetId.AssetName(), V1PoolNFTHexPrefix):
			return strings.TrimPrefix(assetId.AssetName(), V1PoolNFTHexPrefix), true, nil
		case strings.HasPrefix(assetId.AssetName(), V1LPHexPrefix):
			return strings.TrimPrefix(assetId.AssetName(), V1LPHexPrefix), true, nil
		default:
			return "", false, nil
		}
	case V3:
		switch {
		case strings.HasPrefix(assetId.AssetName(), V3PoolNFTHexPrefix):
			return strings.TrimPrefix(assetId.AssetName(), V3PoolNFTHexPrefix), true, nil
		case strings.HasPrefix(assetId.AssetName(), V3LPHexPrefix):
			return strings.TrimPrefix(assetId.AssetName(), V3LPHexPrefix), true, nil
		default:
			return "", false, nil
		}
	default:
		return "", false, fmt.Errorf("unrecognized protocol version %v", p.Version)
	}
}

// V1 specific constants
const V1FactoryNFTHexName = "666163746f7279"
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
