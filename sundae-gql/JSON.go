package sundaegql

import "encoding/json"

type JSON struct {
	Data interface{}
}

func FromRaw(raw json.RawMessage) (JSON, error) {
	m := map[string]interface{}{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return JSON{}, err
	}
	return JSON{Data: m}, nil
}

func (JSON) ImplementsGraphQLType(name string) bool {
	return name == "JSON"
}

func (a *JSON) UnmarshalGraphQL(input interface{}) error {
	a.Data = input
	return nil
}

func (a JSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Data)
}
