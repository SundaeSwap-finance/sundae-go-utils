package main

import (
	_ "embed"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
)

//go:embed example.gql
var schema string

type Resolver struct {
}

func (r *Resolver) Schema() string {
	return schema
}

func (r *Resolver) Config() *sundaegql.BaseConfig {
	return &sundaegql.BaseConfig{
		Logger:  sundaecli.Logger(service),
		Service: service,
	}
}

func (r *Resolver) Hello() string {
	return "world!"
}

func (r *Resolver) World() string {
	return "Hello"
}
