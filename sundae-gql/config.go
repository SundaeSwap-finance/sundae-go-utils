package sundaegql

import (
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/rs/zerolog"
)

type BaseConfig struct {
	Logger  zerolog.Logger
	Service sundaecli.Service
}
