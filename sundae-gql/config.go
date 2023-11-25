package sundaegql

import (
	"os"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/rs/zerolog"
)

type BaseConfig struct {
	Logger  zerolog.Logger
	Service sundaecli.Service
}

func NewConfig(service sundaecli.Service) BaseConfig {
	return BaseConfig{
		Logger: zerolog.New(os.Stdout).With().
			Str("service", service.Name).
			Str("version", service.Version).
			Logger(),
		Service: service,
	}
}
