package sundaeddb

import (
	"fmt"

	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DAXWrapper struct {
	*dax.Dax
}

func DynamoDBAPI(s *session.Session) (dynamodbiface.DynamoDBAPI, error) {
	if DDBOpts.DAXCluster != "" {
		config := dax.DefaultConfig()
		config.HostPorts = []string{DDBOpts.DAXCluster}
		config.Region = "us-east-2"
		daxClient, err := dax.New(config)
		if err != nil {
			return nil, err
		}
		return DAXWrapper{Dax: daxClient}, nil
	} else {
		return dynamodb.New(s), nil
	}
}

// These methods aren't implemented by the DAX library, meaning we can't use it as dynamodbiface
// We don't really need them, so we just return an unimplemented error
func (DAXWrapper) DeleteResourcePolicy(*dynamodb.DeleteResourcePolicyInput) (*dynamodb.DeleteResourcePolicyOutput, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (DAXWrapper) DeleteResourcePolicyWithContext(aws.Context, *dynamodb.DeleteResourcePolicyInput, ...request.Option) (*dynamodb.DeleteResourcePolicyOutput, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (DAXWrapper) DeleteResourcePolicyRequest(*dynamodb.DeleteResourcePolicyInput) (*request.Request, *dynamodb.DeleteResourcePolicyOutput) {
	return nil, nil
}
func (DAXWrapper) GetResourcePolicy(*dynamodb.GetResourcePolicyInput) (*dynamodb.GetResourcePolicyOutput, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (DAXWrapper) GetResourcePolicyWithContext(aws.Context, *dynamodb.GetResourcePolicyInput, ...request.Option) (*dynamodb.GetResourcePolicyOutput, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (DAXWrapper) GetResourcePolicyRequest(*dynamodb.GetResourcePolicyInput) (*request.Request, *dynamodb.GetResourcePolicyOutput) {
	return nil, nil
}
func (DAXWrapper) PutResourcePolicy(*dynamodb.PutResourcePolicyInput) (*dynamodb.PutResourcePolicyOutput, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (DAXWrapper) PutResourcePolicyWithContext(aws.Context, *dynamodb.PutResourcePolicyInput, ...request.Option) (*dynamodb.PutResourcePolicyOutput, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (DAXWrapper) PutResourcePolicyRequest(*dynamodb.PutResourcePolicyInput) (*request.Request, *dynamodb.PutResourcePolicyOutput) {
	return nil, nil
}
