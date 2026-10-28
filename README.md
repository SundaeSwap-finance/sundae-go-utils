# sundae-go-utils

[![Go Version](https://img.shields.io/github/go-mod/go-version/SundaeSwap-finance/sundae-go-utils)](go.mod)
[![Go Report Card](https://goreportcard.com/badge/github.com/SundaeSwap-finance/sundae-go-utils)](https://goreportcard.com/report/github.com/SundaeSwap-finance/sundae-go-utils)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Common Go utilities and boilerplate for building various services throughout the Sundae Labs stack.

## Installation

```bash
go get github.com/SundaeSwap-finance/sundae-go-utils
```

## Modules

### cardano

Utilities for working with Cardano blockchain primitives including address parsing, slot time conversion, and metadata handling.

**Key features:**

- Shelley address parsing and validation
- Payment and staking credential extraction
- Slot number to datetime conversion
- Metadata encoding utilities

**Example:**

```go
import "github.com/SundaeSwap-finance/sundae-go-utils/cardano"

// Parse a Cardano address into payment and staking credentials
payment, staking, err := cardano.SplitAddress("addr1...")

// Check if an address has a stake credential
hasStake, err := cardano.HasStakeAddress("addr1...")

// Convert slot number to datetime
dateTime, err := cardano.SlotToDateTimeEnv(12345678, "mainnet")
```

### sundae-cli

Common CLI boilerplate for building command-line applications and Lambda functions with consistent flag handling, logging, and service metadata.

**Key features:**

- Standardized service configuration
- Common CLI flags (environment, port, dry-run, etc.)
- Structured logging setup
- Build info and version tracking

**Example:**

```go
import sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"

var service = sundaecli.NewService("my-service")

func main() {
    app := sundaecli.App(service, action, sundaecli.CommonFlags...)
    app.Run(os.Args)
}
```

### sundae-gql

GraphQL server utilities with built-in CORS, logging middleware, and common GraphQL scalar types.

**Key features:**

- GraphQL server setup with sensible defaults
- CORS middleware
- Custom scalar types (BigInteger, HexBytes, JSON, Fraction)
- Schema introspection controls
- Request logging

**Example:**

```go
import sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"

type Resolver struct{}

func (r *Resolver) Schema() string { return schema }
func (r *Resolver) Config() *sundaegql.BaseConfig {
    return &sundaegql.BaseConfig{
        Logger:  logger,
        Service: &service,
    }
}

sundaegql.Webserver(&Resolver{})
```

### sundae-kinesis

Utilities for building AWS Kinesis consumers that process blockchain synchronization events using sundae-sync v1.

**NOTE**: This is being deprecated in favor of sundae-sync-v2.

**Key features:**

- Kinesis stream event handling
- Ogmios chainsync integration
- Automatic rollback/rollforward handling
- Cursor management for resumable processing
- Block and transaction-level callbacks

**Example:**

```go
import sundaekinesis "github.com/SundaeSwap-finance/sundae-go-utils/sundae-kinesis"

handler := sundaekinesis.NewTxHandler(
    service,
    rollForwardTx,
    rollBackward,
)
handler.Start(ctx)
```

### sundae-sync-v2-consumer

Blockchain synchronization consumer for processing Cardano transactions from sundae-sync-v2, which uses Kinesis streams and S3 block storage.

**Key features:**

- S3-backed block data retrieval
- Transaction advance/undo callbacks
- Parallel block downloading
- DynamoDB transaction tracking

**Sub-transactions:**

Live sync and replay still invoke advance/undo once per top-level transaction,
with its original block transaction index. Inside a callback, use
`syncV2Consumer.WalkTransaction` to visit sub-transactions in ledger order,
then the parent. Each `TransactionView` pairs its own `ID` and `Body` with its
own `Witnesses`; never merge input lists before interpreting redeemer indices.

For example, an indexer of regular order outputs can use this pattern
(`indexOrders` is application-specific and reports whether it persisted any
relevant effects):

```go
func advance(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error {
    if !tx.IsValid() {
        // Regular outputs were not produced. An indexer that tracks collateral
        // must handle tx.Produced()/tx.Consumed() here instead.
        return nil
    }
    return syncV2Consumer.WalkTransaction(tx, func(view syncV2Consumer.TransactionView) error {
        relevant, err := indexOrders(ctx, view.ID, view.Body.Outputs(), view.Witnesses.PlutusData())
        if err != nil || !relevant {
            return err
        }
        // Publish the producing ID, including sub-transaction IDs, only after
        // effects are persisted. This is a no-op outside distributed replay.
        return replay.PublishRelevantTx(ctx, view.ID.String())
    })
}
```

Use `view.Body.Inputs()` with `view.Witnesses.Redeemers()` for spend processing.
Inline datums remain on outputs; `Witnesses.PlutusData()` contains witness
datums only. The walker is structural and does not filter invalid transactions;
use the enclosing transaction's `Produced()`/`Consumed()` for actual ledger
effects (including collateral).

Rollback must remove the same `(producing ID, output index)` keys. If undo
operations depend on order, call `syncV2Consumer.WalkTransactionReverse` inside
the undo callback. It visits the parent first, then sub-transactions in reverse
ledger order, keeping each body paired with its own witnesses. Both walkers
validate body/witness counts before any visits and stop at the first visitor
error. The consumer still reverses the top-level transactions within the block;
the reverse walker reverses the parts within each transaction. Do not wait for
replay completion of an earlier sub-transaction within the same enclosing
callback: local completion is recorded only after that callback returns.

### sundae-rest

REST API utilities with CORS support and common middleware.

### sundae-secret

AWS Secrets Manager integration for loading configuration secrets.

**Example:**

```go
import sundaesecret "github.com/SundaeSwap-finance/sundae-go-utils/sundae-secret"

var config MyConfig
err := sundaesecret.LoadSecret(session, "my-secret-name", &config)
```

### sundae-cron

Utilities for building scheduled Lambda functions.

### sundae-ddb

DynamoDB and DAX client utilities with common patterns.

### sundae-report

Report generation utilities for recurring data exports.

### sundae/protocol

SundaeSwap protocol-specific utilities for working with distinct versions of the sundae protocol, smart contracts, and other related utilities. Loosely modeled off the plutus.json blueprints generated by Aiken.

**Key features:**

- Protocol version management (V1, V3, Stableswaps)
- Pool NFT and LP token identification
- Pool identifier extraction
- Script reference management

## Templates

The `templates/` directory contains example applications demonstrating how to use the various utilities:

- **example-cli** - Basic CLI application template
- **example-gql** - GraphQL API server template
- **example-kinesis** - Kinesis stream consumer template
- **example-v2-consumer** - Blockchain sync consumer template
- **example-ddb-stream** - DynamoDB stream processor template
- **example-report** - Report generation template

Each template is a complete, runnable example that can be copied and customized for your needs.

## Usage Patterns

### Building a GraphQL API Service

```go
package main

import (
    sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
    sundaegql "github.com/SundaeSwap-finance/sundae-go-utils/sundae-gql"
)

var service = sundaecli.NewService("my-api")

func main() {
    app := sundaecli.App(service, action, sundaecli.CommonFlags...)
    app.Run(os.Args)
}

func action(ctx *cli.Context) error {
    return sundaegql.Webserver(&Resolver{})
}
```

### Building a Kinesis Consumer

```go
package main

import (
    sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
    sundaekinesis "github.com/SundaeSwap-finance/sundae-go-utils/sundae-kinesis"
)

var service = sundaecli.NewService("my-consumer")

func main() {
    handler := sundaekinesis.NewTxHandler(service, processTransaction, rollback)

    app := sundaecli.App(service, handler.Start,
        append(sundaecli.CommonFlags, sundaekinesis.CommonFlags...)...)
    app.Run(os.Args)
}

func processTransaction(ctx context.Context, logger zerolog.Logger,
    point chainsync.PointStruct, tx chainsync.Tx) error {
    // Process transaction
    return nil
}

func rollback(ctx context.Context, logger zerolog.Logger,
    block uint64, txs ...string) error {
    // Handle rollback
    return nil
}
```

## Development

### Running Tests

```bash
# Run unit tests
go test ./...

# Run integration tests (requires local DynamoDB on localhost:8000)
go test -tags=integration ./...
```

### Building Examples

```bash
cd templates/example-gql
go build
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
