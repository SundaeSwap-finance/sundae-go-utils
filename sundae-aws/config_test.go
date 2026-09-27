package sundaeaws

import (
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/endpoints"
)

func TestConfigWithoutOverridesIsDefault(t *testing.T) {
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "AWS_ENDPOINT_URL") {
			t.Setenv(name, "") // restored after the test
			os.Unsetenv(name)
		}
	}
	config := Config()
	if config.EndpointResolver != nil || config.S3ForcePathStyle != nil {
		t.Fatalf("expected an untouched default config, got %+v", config)
	}
}

func TestEndpointForPrefersServiceOverride(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9100")

	cases := map[string]string{
		endpoints.S3ServiceID:       "http://localhost:9100",
		endpoints.DynamodbServiceID: "http://localhost:4566",
		endpoints.KinesisServiceID:  "http://localhost:4566",
	}
	for service, want := range cases {
		if got := EndpointFor(service); got != want {
			t.Errorf("EndpointFor(%q) = %q, want %q", service, got, want)
		}
	}
}

func TestResolverUsesOverrides(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9100")

	config := Config()
	if config.S3ForcePathStyle == nil || !*config.S3ForcePathStyle {
		t.Fatal("expected path-style S3 when an endpoint override is set")
	}
	got, err := config.EndpointResolver.EndpointFor(endpoints.S3ServiceID, "us-east-2")
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "http://localhost:9100" || got.SigningRegion != "us-east-2" {
		t.Fatalf("unexpected S3 endpoint %+v", got)
	}
}

func TestEnvSuffix(t *testing.T) {
	if got := envSuffix("streams.dynamodb"); got != "STREAMS_DYNAMODB" {
		t.Fatalf("envSuffix = %q", got)
	}
}
