package sundaegql

import (
	"encoding/json"
	"fmt"

	"github.com/SundaeSwap-finance/ogmigo/v6/ouroboros/chainsync/num"
)

// TODO: just move these to ogmigo?
type BigInteger num.Int

func (BigInteger) ImplementsGraphQLType(name string) bool {
	return name == "BigInteger"
}

func (a *BigInteger) UnmarshalGraphQL(input interface{}) error {
	s, ok := input.(string)
	if !ok {
		return fmt.Errorf("unable to parse big integer %v", input)
	}
	n, ok := num.New(s)
	if !ok {
		return fmt.Errorf("unable to parse big integer %v", input)
	}
	*a = BigInteger(n)
	return nil
}

func (a BigInteger) MarshalJSON() ([]byte, error) {
	var n num.Int = num.Int(a)
	return json.Marshal(n.String())
}
