package sundaecli

import (
	"os"

	"github.com/rs/zerolog"
)

func Logger(service Service) zerolog.Logger {
	return zerolog.New(os.Stdout).With().
		Str("service", service.Name).
		Str("version", service.Version).
		Logger()
}
