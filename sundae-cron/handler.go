// Package sundaecron provides utilities for building scheduled Lambda functions.
package sundaecron

import (
	"context"
	"encoding/json"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/rs/zerolog"
)

type RunCallback func(ctx context.Context) error

type Handler struct {
	service sundaecli.Service
	logger  zerolog.Logger

	runOnce RunCallback
}

func NewHandler(
	service sundaecli.Service,
	runOnce RunCallback,
) *Handler {
	return &Handler{
		service: service,
		logger:  sundaecli.Logger(service),
		runOnce: runOnce,
	}
}

func (h *Handler) RunOnce(ctx context.Context, _ json.RawMessage) error {
	h.logger.Info().Msg("running scheduled task")
	return h.runOnce(ctx)
}

func (h *Handler) Start() error {
	switch {
	case sundaecli.CommonOpts.Console:
		return h.runOnce(context.Background())

	default:
		lambda.Start(h.RunOnce)
	}
	return nil
}
