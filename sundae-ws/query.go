package sundaews

import (
	"fmt"
	"strings"
)

// SubscriptionFieldExtractor extracts the field name and arguments from a
// subscription query payload. This is provided by the caller (e.g., the
// ws-handler Lambda) which has access to a full GraphQL parser.
type SubscriptionFieldExtractor func(payload SubscribePayload) (fieldName string, args map[string]interface{}, err error)

// SimpleExtractSubscriptionField provides a basic extraction of the subscription
// field name from the query string. It looks for the first field after
// "subscription" and passes through payload.Variables as arguments. It does NOT
// parse inline arguments from the query text. For full query parsing with inline
// argument extraction, use a proper GraphQL parser via SubscriptionFieldExtractor.
func SimpleExtractSubscriptionField(payload SubscribePayload) (string, map[string]interface{}, error) {
	query := strings.TrimSpace(payload.Query)

	// Strip "subscription" or "subscription Name" prefix
	lower := strings.ToLower(query)
	if strings.HasPrefix(lower, "subscription") {
		query = query[len("subscription"):]
		query = strings.TrimSpace(query)
		// Skip optional operation name
		if len(query) > 0 && query[0] != '{' {
			idx := strings.IndexByte(query, '{')
			if idx < 0 {
				return "", nil, fmt.Errorf("malformed subscription query")
			}
			query = query[idx:]
		}
	}

	// Strip outer braces
	query = strings.TrimSpace(query)
	if len(query) < 2 || query[0] != '{' {
		return "", nil, fmt.Errorf("malformed subscription query")
	}
	query = strings.TrimSpace(query[1:])

	// Extract field name (up to '(' or '{' or whitespace)
	fieldEnd := len(query)
	for i, ch := range query {
		if ch == '(' || ch == '{' || ch == ' ' || ch == '\n' || ch == '\t' {
			fieldEnd = i
			break
		}
	}
	fieldName := query[:fieldEnd]
	if fieldName == "" {
		return "", nil, fmt.Errorf("empty subscription field name")
	}

	// Extract arguments from variables if available
	args := make(map[string]interface{})
	if payload.Variables != nil {
		for k, v := range payload.Variables {
			args[k] = v
		}
	}

	return fieldName, args, nil
}
