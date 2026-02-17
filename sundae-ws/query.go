package sundaews

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// SubscriptionFieldExtractor extracts the field name and arguments from a
// subscription query payload. This is provided by the caller (e.g., the
// ws-handler Lambda) which has access to a full GraphQL parser.
type SubscriptionFieldExtractor func(payload SubscribePayload) (fieldName string, args map[string]interface{}, err error)

// SimpleExtractSubscriptionField extracts the subscription field name and
// arguments from the query string. It parses inline arguments (e.g.,
// `poolId: "abc"`) and resolves variable references (e.g., `poolId: $poolId`)
// from payload.Variables. Inline arguments take precedence over variables with
// the same name.
func SimpleExtractSubscriptionField(payload SubscribePayload) (string, map[string]interface{}, error) {
	query := strings.TrimSpace(payload.Query)

	// Strip "subscription" or "subscription Name" or
	// "subscription Name($var: Type)" prefix
	lower := strings.ToLower(query)
	if strings.HasPrefix(lower, "subscription") {
		query = query[len("subscription"):]
		query = strings.TrimSpace(query)
		// Skip optional operation name and variable definitions
		if len(query) > 0 && query[0] != '{' {
			// Could be "Name { ... }" or "Name($v: T!) { ... }" or "($v: T!) { ... }"
			if query[0] == '(' {
				// Skip variable definitions: find matching ')'
				depth := 0
				for i, ch := range query {
					if ch == '(' {
						depth++
					} else if ch == ')' {
						depth--
						if depth == 0 {
							query = query[i+1:]
							break
						}
					}
				}
			} else {
				// Skip operation name, then optionally variable definitions
				idx := strings.IndexAny(query, "{(")
				if idx < 0 {
					return "", nil, fmt.Errorf("malformed subscription query")
				}
				if query[idx] == '(' {
					// Skip past the variable definitions
					depth := 0
					for i := idx; i < len(query); i++ {
						if query[i] == '(' {
							depth++
						} else if query[i] == ')' {
							depth--
							if depth == 0 {
								query = query[i+1:]
								break
							}
						}
					}
				} else {
					query = query[idx:]
				}
			}
			query = strings.TrimSpace(query)
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

	// Start with variables as base args
	args := make(map[string]interface{})
	if payload.Variables != nil {
		for k, v := range payload.Variables {
			args[k] = v
		}
	}

	// Parse inline arguments if present — these take precedence over variables
	rest := strings.TrimSpace(query[fieldEnd:])
	if len(rest) > 0 && rest[0] == '(' {
		closing := findMatchingParen(rest)
		if closing < 0 {
			return "", nil, fmt.Errorf("unmatched '(' in subscription arguments")
		}
		inlineArgs, err := parseArgs(rest[1:closing], payload.Variables)
		if err != nil {
			return "", nil, fmt.Errorf("parsing inline arguments: %w", err)
		}
		for k, v := range inlineArgs {
			args[k] = v
		}
	}

	return fieldName, args, nil
}

// findMatchingParen returns the index of the ')' that matches the opening '('
// at s[0], accounting for nested parens and quoted strings.
func findMatchingParen(s string) int {
	depth := 0
	inString := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inString {
			if ch == '\\' {
				i++ // skip escaped char
			} else if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// parseArgs parses a GraphQL argument list (the text between parens) into a map.
// It handles string, number, boolean, null, enum, variable reference, and list values.
func parseArgs(s string, variables map[string]interface{}) (map[string]interface{}, error) {
	args := make(map[string]interface{})
	s = strings.TrimSpace(s)

	for len(s) > 0 {
		// Parse argument name
		nameEnd := strings.IndexAny(s, ": \t\n")
		if nameEnd < 0 {
			return nil, fmt.Errorf("expected ':' after argument name")
		}
		name := s[:nameEnd]
		s = strings.TrimSpace(s[nameEnd:])

		// Expect ':'
		if len(s) == 0 || s[0] != ':' {
			return nil, fmt.Errorf("expected ':' after argument name %q", name)
		}
		s = strings.TrimSpace(s[1:])

		// Parse value
		val, rest, err := parseValue(s, variables)
		if err != nil {
			return nil, fmt.Errorf("argument %q: %w", name, err)
		}
		args[name] = val
		s = strings.TrimSpace(rest)

		// Skip optional comma
		if len(s) > 0 && s[0] == ',' {
			s = strings.TrimSpace(s[1:])
		}
	}

	return args, nil
}

// parseValue parses a single GraphQL value and returns it along with the
// remaining unparsed string.
func parseValue(s string, variables map[string]interface{}) (interface{}, string, error) {
	if len(s) == 0 {
		return nil, "", fmt.Errorf("unexpected end of input")
	}

	switch {
	case s[0] == '"':
		return parseString(s)
	case s[0] == '[':
		return parseList(s, variables)
	case s[0] == '$':
		return parseVariable(s, variables)
	case strings.HasPrefix(s, "true"):
		return true, skipIdent(s), nil
	case strings.HasPrefix(s, "false"):
		return false, skipIdent(s), nil
	case strings.HasPrefix(s, "null"):
		return nil, skipIdent(s), nil
	case s[0] == '-' || (s[0] >= '0' && s[0] <= '9'):
		return parseNumber(s)
	default:
		// Enum value — bare identifier, treat as string
		return parseEnum(s)
	}
}

func parseString(s string) (string, string, error) {
	var b strings.Builder
	i := 1 // skip opening quote
	for i < len(s) {
		ch := s[i]
		if ch == '\\' && i+1 < len(s) {
			next := s[i+1]
			switch next {
			case '"', '\\', '/':
				b.WriteByte(next)
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			default:
				b.WriteByte('\\')
				b.WriteByte(next)
			}
			i += 2
			continue
		}
		if ch == '"' {
			return b.String(), s[i+1:], nil
		}
		b.WriteByte(ch)
		i++
	}
	return "", "", fmt.Errorf("unterminated string")
}

func parseList(s string, variables map[string]interface{}) ([]interface{}, string, error) {
	s = s[1:] // skip '['
	var list []interface{}

	s = strings.TrimSpace(s)
	for len(s) > 0 && s[0] != ']' {
		val, rest, err := parseValue(s, variables)
		if err != nil {
			return nil, "", err
		}
		list = append(list, val)
		s = strings.TrimSpace(rest)
		if len(s) > 0 && s[0] == ',' {
			s = strings.TrimSpace(s[1:])
		}
	}

	if len(s) == 0 {
		return nil, "", fmt.Errorf("unterminated list")
	}
	return list, s[1:], nil // skip ']'
}

func parseVariable(s string, variables map[string]interface{}) (interface{}, string, error) {
	s = s[1:] // skip '$'
	end := 0
	for end < len(s) && isIdentChar(rune(s[end])) {
		end++
	}
	if end == 0 {
		return nil, "", fmt.Errorf("expected variable name after '$'")
	}
	name := s[:end]
	if variables != nil {
		if val, ok := variables[name]; ok {
			return val, s[end:], nil
		}
	}
	return nil, s[end:], nil // unresolved variable → nil
}

func parseNumber(s string) (interface{}, string, error) {
	end := 0
	if end < len(s) && s[end] == '-' {
		end++
	}
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	isFloat := false
	if end < len(s) && s[end] == '.' {
		isFloat = true
		end++
		for end < len(s) && s[end] >= '0' && s[end] <= '9' {
			end++
		}
	}
	numStr := s[:end]
	if isFloat {
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid number %q", numStr)
		}
		return f, s[end:], nil
	}
	n, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return nil, "", fmt.Errorf("invalid number %q", numStr)
	}
	return n, s[end:], nil
}

func parseEnum(s string) (string, string, error) {
	end := 0
	for end < len(s) && isIdentChar(rune(s[end])) {
		end++
	}
	if end == 0 {
		return "", "", fmt.Errorf("unexpected character %q", string(s[0]))
	}
	return s[:end], s[end:], nil
}

// skipIdent advances past a keyword like "true", "false", or "null".
func skipIdent(s string) string {
	i := 0
	for i < len(s) && isIdentChar(rune(s[i])) {
		i++
	}
	return s[i:]
}

func isIdentChar(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch)
}
