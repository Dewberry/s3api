package blobstore

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type AWSCreds struct {
	AWS_ACCESS_KEY_ID     string `json:"AWS_ACCESS_KEY_ID"`
	AWS_SECRET_ACCESS_KEY string `json:"AWS_SECRET_ACCESS_KEY"`
	AWS_S3_BUCKET         string `json:"AWS_S3_BUCKET"`
}

type AWSConfig struct {
	// UseIAMRole ignores any static keys (in this file or AWS_* env vars)
	// and uses only the EC2 instance profile from instance metadata.
	UseIAMRole      bool       `json:"use_iam_role"`
	Accounts        []AWSCreds `json:"accounts"`
	BucketAllowList []string   `json:"bucket_allow_list"`
	// BucketBlockList buckets are never exposed, even when allowed (e.g. by "*").
	BucketBlockList []string `json:"bucket_block_list"`
}

type MinioConfig struct {
	S3Endpoint      string `json:"MINIO_S3_ENDPOINT"`
	DisableSSL      string `json:"MINIO_S3_DISABLE_SSL"`
	ForcePathStyle  string `json:"MINIO_S3_FORCE_PATH_STYLE"`
	AccessKeyID     string `json:"MINIO_ACCESS_KEY_ID"`
	SecretAccessKey string `json:"MINIO_SECRET_ACCESS_KEY"`
	Bucket          string `json:"AWS_S3_BUCKET"`
	S3Mock          string `json:"S3_MOCK"`
}

func (creds MinioConfig) validateMinioConfig() error {
	missingFields := []string{}
	if creds.S3Endpoint == "" {
		missingFields = append(missingFields, "S3Endpoint")
	}
	if creds.DisableSSL == "" {
		missingFields = append(missingFields, "DisableSSL")
	}
	if creds.ForcePathStyle == "" {
		missingFields = append(missingFields, "ForcePathStyle")
	}
	if creds.AccessKeyID == "" {
		missingFields = append(missingFields, "AccessKeyID")
	}
	if creds.SecretAccessKey == "" {
		missingFields = append(missingFields, "SecretAccessKey")
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("missing fields: %s", strings.Join(missingFields, ", "))
	}
	return nil
}

// validateAWSConfig checks a parsed .env.json. Static keys are only required
// when the IAM role is not in use.
func validateAWSConfig(awsConfig AWSConfig, useIAMRole bool) error {
	if !useIAMRole {
		for i, account := range awsConfig.Accounts {
			missingFields := []string{}
			if account.AWS_ACCESS_KEY_ID == "" {
				missingFields = append(missingFields, "AWS_ACCESS_KEY_ID")
			}
			if account.AWS_SECRET_ACCESS_KEY == "" {
				missingFields = append(missingFields, "AWS_SECRET_ACCESS_KEY")
			}
			if len(missingFields) > 0 {
				return fmt.Errorf("missing fields (%s) for AWS account %d in envJson file", strings.Join(missingFields, ", "), i+1)
			}
		}
	}
	if len(awsConfig.BucketAllowList) == 0 {
		return fmt.Errorf("no buckets in the `bucket_allow_list`, please provide required buckets, or `*` for access to all buckets")
	}
	return nil
}

// newAWSConfig reads .env.json. The IAM role is used when the file is empty,
// defines no "accounts", or sets "use_iam_role": true. Otherwise the static
// keys in "accounts" are used, as before.
func newAWSConfig(envJson string) (awsConfig AWSConfig, useIAMRole bool, err error) {
	jsonData, err := os.ReadFile(envJson)
	if err != nil {
		return awsConfig, false, fmt.Errorf("error reading .env.json: %s", err.Error())
	}

	if len(strings.TrimSpace(string(jsonData))) == 0 {
		// Empty file: IAM role with access to all buckets.
		awsConfig.BucketAllowList = []string{"*"}
		useIAMRole = true
	} else {
		if err := json.Unmarshal(jsonData, &awsConfig); err != nil {
			return awsConfig, false, fmt.Errorf("error parsing .env.json: %s", err.Error())
		}
		useIAMRole = awsConfig.UseIAMRole || len(awsConfig.Accounts) == 0
	}

	if err := validateAWSConfig(awsConfig, useIAMRole); err != nil {
		return awsConfig, false, err
	}

	if useIAMRole {
		// One account with no keys: the session falls back to the IAM role.
		awsConfig.Accounts = []AWSCreds{{}}
	}
	return awsConfig, useIAMRole, nil
}

func newMinioConfig() MinioConfig {
	var mc MinioConfig
	mc.S3Endpoint = os.Getenv("MINIO_S3_ENDPOINT")
	mc.DisableSSL = os.Getenv("MINIO_S3_DISABLE_SSL")
	mc.ForcePathStyle = os.Getenv("MINIO_S3_FORCE_PATH_STYLE")
	mc.AccessKeyID = os.Getenv("MINIO_ACCESS_KEY_ID")
	mc.Bucket = os.Getenv("AWS_S3_BUCKET")
	mc.SecretAccessKey = os.Getenv("MINIO_SECRET_ACCESS_KEY")
	return mc
}
