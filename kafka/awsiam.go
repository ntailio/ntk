// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"

	"github.com/ntailio/ntk/buildinfo"
	"github.com/ntailio/ntk/profile"
)

// awsCredentials loads the AWS credential chain once per profile and caches
// what it hands out, refreshing temporary credentials before they expire.
type awsCredentials struct {
	a *profile.AWS

	once     sync.Once
	provider aws.CredentialsProvider
	err      error
}

func (c *awsCredentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	c.once.Do(func() {
		var opts []func(*config.LoadOptions) error
		if c.a != nil && c.a.Profile != "" {
			opts = append(opts, config.WithSharedConfigProfile(c.a.Profile))
		}
		// Assuming a role calls STS, which needs a region; any works for that.
		opts = append(opts, config.WithDefaultRegion("us-east-1"))
		cfg, err := config.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			c.err = fmt.Errorf("loading AWS config: %w", err)
			return
		}
		c.provider = cfg.Credentials
		if c.a != nil && c.a.RoleARN != "" {
			c.provider = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), c.a.RoleARN,
				func(o *stscreds.AssumeRoleOptions) { o.RoleSessionName = "ntk" }))
		}
	})
	if c.err != nil {
		return aws.Credentials{}, c.err
	}
	if c.provider == nil {
		return aws.Credentials{}, fmt.Errorf("no AWS credentials found (set AWS_PROFILE, run `aws sso login`, or configure auth.aws.profile)")
	}
	creds, err := c.provider.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("AWS credentials: %w", err)
	}
	return creds, nil
}

// AWSCredentials fetches the credentials an aws-msk-iam profile would use.
func AWSCredentials(ctx context.Context, a profile.Auth) (aws.Credentials, error) {
	return (&awsCredentials{a: a.AWS}).Retrieve(ctx)
}

func awsMechanism(a profile.Auth, _ func(string) string) (sasl.Mechanism, error) {
	creds := &awsCredentials{a: a.AWS}
	return awssasl.ManagedStreamingIAM(func(ctx context.Context) (awssasl.Auth, error) {
		c, err := creds.Retrieve(ctx)
		if err != nil {
			return awssasl.Auth{}, err
		}
		return awssasl.Auth{
			AccessKey:    c.AccessKeyID,
			SecretKey:    c.SecretAccessKey,
			SessionToken: c.SessionToken,
			UserAgent:    "ntk/" + buildinfo.Get().Version,
		}, nil
	}), nil
}
