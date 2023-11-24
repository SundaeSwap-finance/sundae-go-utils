package sundaegql

import "github.com/rs/zerolog"

type BaseConfig struct {
	Logger  zerolog.Logger
	Service Service
}
