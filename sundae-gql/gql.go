// Package sundaegql provides GraphQL server utilities with built-in CORS,
// logging middleware, and common GraphQL scalar types.
//
// This package includes server setup with sensible defaults, custom scalar types
// for blockchain data (BigInteger, HexBytes, JSON, Fraction), and schema
// introspection controls.
package sundaegql

import (
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
)

func AllowIntrospection() bool {
	return sundaecli.CommonOpts.Network != "mainnet" || sundaecli.CommonOpts.Console
}

type Resolver interface {
	Schema() string
	Config() *BaseConfig
}
