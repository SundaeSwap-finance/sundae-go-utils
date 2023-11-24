package sundaegql

import (
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
)

func AllowIntrospection() bool {
	return sundaecli.CommonOpts.Env != "mainnet" || sundaecli.CommonOpts.Console
}

type Resolver interface {
	Schema() string
	Config() *BaseConfig
}
