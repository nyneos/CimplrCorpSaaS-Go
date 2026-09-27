package mailruntime

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"CimplrCorpSaas/internal/mailengine/s3store"
)

// HealthCheck verifies the mail engine's dependencies actually work — chiefly
// that AWS config resolves and the inbound bucket is reachable. There is no
// more separate process to ping, so this is the meaningful "is mail
// processing usable right now" signal that callers (jobs/email/serviceHealth.go)
// gate polling on.
func (r *Runtime) HealthCheck(ctx context.Context) error {
	return runIsolated(ctx, healthCheckTimeout(), func(ctx context.Context) error {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(s3store.Region()))
		if err != nil {
			return fmt.Errorf("aws config: %w", err)
		}
		client := s3.NewFromConfig(cfg)
		_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(s3store.Bucket()),
		})
		if err != nil {
			return fmt.Errorf("s3 bucket %s unreachable: %w", s3store.Bucket(), err)
		}
		return nil
	})
}
