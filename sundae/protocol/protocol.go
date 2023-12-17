package protocol

import (
	"fmt"

	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
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
	Version      string            `dynamodbav:"version"`
	Environment  string            `dynamodbav:"environment"`
	Blueprint    Blueprint         `dynamodbav:"blueprint"`
	BlueprintUrl string            `dynamodbav:"-"`
	References   []ScriptReference `dynamodbav:"references"`
}

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
