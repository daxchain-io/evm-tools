package awssink

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// ClientConfig is the resolved, non-secret AWS connection configuration.
// Credentials are deliberately absent: the SDK default chain supplies them
// (environment, shared config/profile, IRSA/web identity, or an instance role),
// so no secret material lives in evm-tools config.
type ClientConfig struct {
	// Region is the AWS region; empty lets the SDK resolve it from the environment
	// or shared config.
	Region string
	// EndpointURL overrides the service endpoint (for LocalStack, a VPC endpoint,
	// or tests). Empty uses the default AWS endpoint for the region.
	EndpointURL string
}

// LoadAWSConfig builds an aws.Config using the SDK default credential chain with
// an optional region and base-endpoint override. Pass a bounded context: some
// credential providers (SSO, web identity, IMDS) perform network I/O.
func LoadAWSConfig(ctx context.Context, cc ClientConfig) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if cc.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cc.Region))
	}
	if cc.EndpointURL != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(cc.EndpointURL))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("awssink: load AWS config: %w", err)
	}
	return cfg, nil
}
