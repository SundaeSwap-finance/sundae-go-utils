package cardano

import (
	"encoding/hex"
	"fmt"
)

type MetadataBlob map[string]interface{}

func (blob MetadataBlob) Parse() (map[string]interface{}, error) {
	results := make(map[string]interface{}, len(blob))
	for metaDatumlabel, raw := range blob {
		parsedRaw, err := parse("/"+metaDatumlabel+"/", raw)
		if err != nil {
			return nil, fmt.Errorf("unable to parse metadatum label: %v: %w", metaDatumlabel, err)
		}
		results[metaDatumlabel] = parsedRaw
	}

	return results, nil
}

const (
	typeInt    = "int"
	typeString = "string"
	typeBytes  = "bytes"
	typeList   = "list"
	typeMap    = "map"
)

func parse(path string, raw interface{}) (interface{}, error) {
	asMap, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unable to convert to map")
	}
	for k, v := range asMap {
		switch k {
		case typeInt:
			return parseInt(path+k, v)
		case typeString:
			return parseString(path+k, v)
		case typeBytes:
			return parseBytes(path+k, v)
		case typeList:
			return parseList(path+k, v)
		case typeMap:
			return parseMap(path+k, v)
		default:
			return nil, fmt.Errorf("invalid type: %v", k)
		}
	}
	return nil, nil
}

func parseList(path string, v interface{}) ([]interface{}, error) {
	asList, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unable to parse list at path: %v", path)
	}

	result := make([]interface{}, 0, len(asList))
	for i, valRaw := range asList {
		childPath := fmt.Sprintf("%v-%v", path, i)
		val, err := parse(childPath, valRaw)
		if err != nil {
			return nil, fmt.Errorf("unable to parse path '%v': %w", path, err)
		}
		result = append(result, val)
	}
	return result, nil
}

func parseBytes(path string, v interface{}) ([]byte, error) {
	result, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("path: %v: unable to parse '%v' as bytes", path, v)
	}
	if result[0:2] == "0x" {
		result = result[2:]
	}

	return hex.DecodeString(result)
}

func parseInt(path string, v interface{}) (int64, error) {
	result, ok := v.(float64) // JSON unmarshals to float64
	if !ok {
		return 0, fmt.Errorf("path: %v: unable to parse '%v' as int64", path, v)
	}

	return int64(result), nil
}

func parseString(path string, v interface{}) (string, error) {
	result, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("path: %v: unable to parse '%v' as string", path, v)
	}

	return result, nil
}

func parseMap(path string, v interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	rawList, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("could not parse as map")
	}
	for i, kvPairRaw := range rawList {
		path := fmt.Sprintf("%v-%v", path, i)
		kvPair, ok := kvPairRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expecting map at path: %v", i)
		}
		keyRaw, ok := kvPair["k"]
		if !ok {
			return nil, fmt.Errorf("missing 'k'")
		}
		keyParsed, err := parse(path+"-k", keyRaw)
		if err != nil {
			return nil, err
		}
		keyString, ok := keyParsed.(string)
		if !ok {
			// force as string
			keyString = fmt.Sprintf("%v", keyParsed)
		}

		valueRaw, ok := kvPair["v"]
		if !ok {
			return nil, fmt.Errorf("missing 'v'")
		}
		valueParsed, err := parse(path+"-v", valueRaw)
		if err != nil {
			return nil, err
		}

		result[keyString] = valueParsed

	}
	return result, nil
}
