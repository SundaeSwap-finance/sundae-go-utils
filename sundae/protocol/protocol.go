package protocol

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/SundaeSwap-finance/apollo/constants"
	"github.com/SundaeSwap-finance/apollo/serialization/Address"
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

type Validator struct {
	Title        string
	CompiledCode []byte
	Hash         []byte
}

func (v *Validator) UnmarshalJSON(data []byte) error {
	var s struct {
		Title        string
		CompiledCode string
		Hash         string
	}
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	code := s.CompiledCode
	hash := s.Hash
	codeBytes, err := hex.DecodeString(code)
	if err != nil {
		return err
	}
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	v.Title = s.Title
	v.CompiledCode = codeBytes
	v.Hash = hashBytes
	return nil
}

type Blueprint struct {
	Validators []Validator
}

type Protocol struct {
	Version      string
	Environment  string
	Blueprint    Blueprint
	BlueprintUrl string
	References   map[string]TxIn
	Network      string
}

const OrderScriptKey = "order.spend"
const PoolScriptKey = "pool.spend"
const SettingsScriptKey = "settings.spend"
const StakeScriptKey = "stake.stake"

func (p *Protocol) getAddress(key string, addressType byte) (*Address.Address, error) {
	var network constants.Network
	if p.Network == "mainnet" {
		network = constants.MAINNET
	} else {
		network = constants.TESTNET
	}
	for _, v := range p.Blueprint.Validators {
		if v.Title == key {
			addr, err := Address.AddressFromBytes(v.Hash, nil, network, addressType)
			if err != nil {
				return nil, fmt.Errorf("Couldn't create address from '%s' script hash: %v", key, v.Hash)
			}
			return addr, nil
		}
	}
	return nil, fmt.Errorf("Couldn't find '%s' script in the blueprint", key)
}

func (p *Protocol) GetOrderAddress() (*Address.Address, error) {
	return p.getAddress(OrderScriptKey, Address.SCRIPT_NONE)
}

func (p *Protocol) GetPoolAddress() (*Address.Address, error) {
	return p.getAddress(PoolScriptKey, Address.SCRIPT_NONE)
}

func (p *Protocol) GetSettingsAddress() (*Address.Address, error) {
	return p.getAddress(SettingsScriptKey, Address.SCRIPT_NONE)
}

func (p *Protocol) GetStakeAddress() (*Address.Address, error) {
	return p.getAddress(StakeScriptKey, Address.NONE_SCRIPT)
}

func (p *Protocol) getScript(key string) ([]byte, error) {
	for _, v := range p.Blueprint.Validators {
		if v.Title == key {
			return v.CompiledCode, nil
		}
	}
	return nil, fmt.Errorf("Couldn't find '%s' script in the blueprint", key)
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
