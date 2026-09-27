package ses

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
)

// RuleSpec is one SES receipt rule — one recipient per mailbox (industry standard).
type RuleSpec struct {
	RuleName  string `json:"rule_name"`
	Recipient string `json:"recipient"`
}

type SyncRequest struct {
	RuleSetName string     `json:"rule_set_name"`
	S3Bucket    string     `json:"s3_bucket"`
	S3Prefix    string     `json:"s3_prefix"`
	Rules       []RuleSpec `json:"rules"`
}

type SyncResult struct {
	RuleSetName string   `json:"rule_set_name"`
	Synced      int      `json:"synced"`
	Removed     int      `json:"removed"`
	Errors      []string `json:"errors,omitempty"`
}

var (
	clientOnce sync.Once
	sesClient  *ses.Client
	region     string
)

func client(ctx context.Context) (*ses.Client, error) {
	clientOnce.Do(func() {
		region = strings.TrimSpace(os.Getenv("AWS_REGION"))
		if region == "" {
			region = strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_REGION"))
		}
		if region == "" {
			region = "ap-south-1"
		}
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err == nil {
			sesClient = ses.NewFromConfig(cfg)
		}
	})
	if sesClient == nil {
		return nil, fmt.Errorf("failed to initialize SES client")
	}
	return sesClient, nil
}

func defaultRuleSetName() string {
	if v := strings.TrimSpace(os.Getenv("EMAIL_SES_RULE_SET_NAME")); v != "" {
		return v
	}
	return "cimplr-inbound"
}

func defaultBucket() string {
	if v := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_BUCKET")); v != "" {
		return v
	}
	return "cimplr"
}

func defaultPrefix() string {
	if v := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_PREFIX")); v != "" {
		return v
	}
	return "email/inbound/raw/"
}

func sesIAMRoleARN() (string, error) {
	arn := strings.TrimSpace(os.Getenv("EMAIL_SES_IAM_ROLE_ARN"))
	if arn == "" {
		return "", fmt.Errorf("EMAIL_SES_IAM_ROLE_ARN is not set in .env (required for SES → S3 receipt rules)")
	}
	if !strings.HasPrefix(arn, "arn:aws:iam::") {
		return "", fmt.Errorf("EMAIL_SES_IAM_ROLE_ARN is invalid: %q", arn)
	}
	return arn, nil
}

// SyncReceiptRules reconciles SES receipt rules with the desired mailbox list.
// Creates/updates one rule per mailbox; removes rules not in the desired set.
func SyncReceiptRules(ctx context.Context, req SyncRequest) (*SyncResult, error) {
	if req.RuleSetName == "" {
		req.RuleSetName = defaultRuleSetName()
	}
	if req.S3Bucket == "" {
		req.S3Bucket = defaultBucket()
	}
	if req.S3Prefix == "" {
		req.S3Prefix = defaultPrefix()
	}

	roleARN, err := sesIAMRoleARN()
	if err != nil {
		return nil, err
	}

	c, err := client(ctx)
	if err != nil {
		return nil, err
	}

	if err := ensureRuleSet(ctx, c, req.RuleSetName); err != nil {
		return nil, err
	}

	desired := map[string]RuleSpec{}
	for _, r := range req.Rules {
		name := strings.TrimSpace(r.RuleName)
		rcpt := strings.ToLower(strings.TrimSpace(r.Recipient))
		if name == "" || rcpt == "" {
			continue
		}
		desired[name] = RuleSpec{RuleName: name, Recipient: rcpt}
	}

	existing, err := listRuleNames(ctx, c, req.RuleSetName)
	if err != nil {
		return nil, err
	}

	result := &SyncResult{RuleSetName: req.RuleSetName}

	for name, spec := range desired {
		if err := upsertRule(ctx, c, req.RuleSetName, req.S3Bucket, req.S3Prefix, roleARN, spec); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("upsert %s: %v", name, err))
			continue
		}
		result.Synced++
		delete(existing, name)
	}

	for name := range existing {
		if strings.HasPrefix(name, "cimplr-inbox-") {
			if _, err := c.DeleteReceiptRule(ctx, &ses.DeleteReceiptRuleInput{
				RuleName:    aws.String(name),
				RuleSetName: aws.String(req.RuleSetName),
			}); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("delete %s: %v", name, err))
				continue
			}
			result.Removed++
		}
	}

	return result, nil
}

// DeleteReceiptRule removes a single mailbox rule from the active rule set.
func DeleteReceiptRule(ctx context.Context, ruleSetName, ruleName string) error {
	if ruleSetName == "" {
		ruleSetName = defaultRuleSetName()
	}
	c, err := client(ctx)
	if err != nil {
		return err
	}
	_, err = c.DeleteReceiptRule(ctx, &ses.DeleteReceiptRuleInput{
		RuleName:    aws.String(ruleName),
		RuleSetName: aws.String(ruleSetName),
	})
	return err
}

func ensureRuleSet(ctx context.Context, c *ses.Client, name string) error {
	_, err := c.CreateReceiptRuleSet(ctx, &ses.CreateReceiptRuleSetInput{
		RuleSetName: aws.String(name),
	})
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		return fmt.Errorf("create rule set: %w", err)
	}
	_, err = c.SetActiveReceiptRuleSet(ctx, &ses.SetActiveReceiptRuleSetInput{
		RuleSetName: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("activate rule set: %w", err)
	}
	return nil
}

func listRuleNames(ctx context.Context, c *ses.Client, ruleSetName string) (map[string]struct{}, error) {
	out, err := c.DescribeReceiptRuleSet(ctx, &ses.DescribeReceiptRuleSetInput{
		RuleSetName: aws.String(ruleSetName),
	})
	if err != nil {
		if strings.Contains(err.Error(), "RuleSetDoesNotExist") {
			return map[string]struct{}{}, nil
		}
		return nil, err
	}
	names := map[string]struct{}{}
	for _, r := range out.Rules {
		if r.Name != nil {
			names[*r.Name] = struct{}{}
		}
	}
	return names, nil
}

func upsertRule(ctx context.Context, c *ses.Client, ruleSetName, bucket, prefix, roleARN string, spec RuleSpec) error {
	rule := types.ReceiptRule{
		Name:       aws.String(spec.RuleName),
		Enabled:    true,
		Recipients: []string{spec.Recipient},
		Actions: []types.ReceiptAction{
			{
				S3Action: &types.S3Action{
					BucketName:      aws.String(bucket),
					ObjectKeyPrefix: aws.String(prefix),
					IamRoleArn:      aws.String(roleARN),
				},
			},
		},
		ScanEnabled: true,
	}

	// Try create first; if exists, delete then recreate (SES has no update API).
	_, createErr := c.CreateReceiptRule(ctx, &ses.CreateReceiptRuleInput{
		Rule:        &rule,
		RuleSetName: aws.String(ruleSetName),
	})
	if createErr == nil {
		return nil
	}
	if !strings.Contains(createErr.Error(), "AlreadyExists") {
		return createErr
	}

	_, _ = c.DeleteReceiptRule(ctx, &ses.DeleteReceiptRuleInput{
		RuleName:    aws.String(spec.RuleName),
		RuleSetName: aws.String(ruleSetName),
	})
	_, err := c.CreateReceiptRule(ctx, &ses.CreateReceiptRuleInput{
		Rule:        &rule,
		RuleSetName: aws.String(ruleSetName),
	})
	return err
}
