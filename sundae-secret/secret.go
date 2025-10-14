// Package sundaesecret provides AWS Secrets Manager integration for loading
// configuration secrets into Go structs.
package sundaesecret

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/savaki/secrets"
)

func LoadSecret(s *session.Session, secretName string, data interface{}) error {
	api := secrets.WithSecretsManager(secretsmanager.New(s))
	manager, err := secrets.NewManager(api)
	if err != nil {
		return fmt.Errorf("failed to initialize secrets: %w", err)
	}

	if err := manager.Decode(secretName, &data); err != nil {
		return fmt.Errorf("failed to load secret %v: %v", secretName, err)
	}
	return nil
}
