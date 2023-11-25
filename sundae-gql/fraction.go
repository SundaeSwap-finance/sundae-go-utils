package sundaegql

import (
	"encoding/json"
	"fmt"
)

type Fraction []int32

func (Fraction) ImplementsGraphQLType(name string) bool {
	return name == "Fraction"
}

func (a *Fraction) UnmarshalGraphQL(input interface{}) error {
	f, ok := input.([]int32)
	if !ok {
		return fmt.Errorf("invalid fraction %v", input)
	}
	*a = f
	return nil
}

func (a Fraction) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int32(a))
}
