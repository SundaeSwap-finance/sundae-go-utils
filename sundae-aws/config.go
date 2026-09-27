// Package sundaeaws builds aws-sdk-go (v1) configuration that honors the
// AWS_ENDPOINT_URL family of environment variables, which the v1 SDK ignores.
//
// This lets every service run against local emulators (e.g. LocalStack for
// DynamoDB/Kinesis and a separate S3-compatible store for block archives)
// without code changes:
//
//	AWS_ENDPOINT_URL=http://localhost:4566      // default for every service
//	AWS_ENDPOINT_URL_S3=http://localhost:9100   // per-service override
//
// Per-service variables follow the AWS convention: AWS_ENDPOINT_URL_ plus the
// service id upper-cased with non-alphanumerics replaced by underscores
// (S3, DYNAMODB, KINESIS, STREAMS_DYNAMODB, ...). When no variable is set the
// returned config is identical to aws.NewConfig(), so deployed Lambdas are
// unaffected.
package sundaeaws

import (
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
)

// Config returns a new aws.Config honoring AWS_ENDPOINT_URL and
// AWS_ENDPOINT_URL_<SERVICE>. S3 path-style addressing is forced whenever a
// custom endpoint is in use, since local emulators don't serve
// virtual-hosted bucket subdomains.
func Config() *aws.Config {
	config := aws.NewConfig()
	if !overridden() {
		return config
	}
	return config.
		WithS3ForcePathStyle(true).
		WithEndpointResolver(endpoints.ResolverFunc(resolve))
}

// NewSession returns a session built from Config().
func NewSession() *session.Session {
	return session.Must(session.NewSession(Config()))
}

func resolve(service, region string, opts ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
	if url := EndpointFor(service); url != "" {
		return endpoints.ResolvedEndpoint{URL: url, SigningRegion: region}, nil
	}
	return endpoints.DefaultResolver().EndpointFor(service, region, opts...)
}

// EndpointFor returns the endpoint override for the given service id, or ""
// when the SDK's default endpoint should be used.
func EndpointFor(service string) string {
	if url := os.Getenv("AWS_ENDPOINT_URL_" + envSuffix(service)); url != "" {
		return url
	}
	return os.Getenv("AWS_ENDPOINT_URL")
}

func overridden() bool {
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "AWS_ENDPOINT_URL") {
			return true
		}
	}
	return false
}

func envSuffix(service string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r - 'a' + 'A'
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, service)
}
