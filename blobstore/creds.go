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
	Accounts        []AWSCreds `json:"accounts"`
	BucketAllowList []string   `json:"bucket_allow_list"`
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

func validateEnvJSON(filePath string) error {
	// Read the contents of the .env.json file
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading .env.json: %s", err.Error())
	}

	// Parse the JSON data into the AWSConfig struct
	var awsConfig AWSConfig
	if err := json.Unmarshal(jsonData, &awsConfig); err != nil {
		return fmt.Errorf("error parsing .env.json: %s", err.Error())
	}

	// Check if there is at least one account defined
	if len(awsConfig.Accounts) == 0 {
		return fmt.Errorf("no AWS accounts defined in .env.json")
	}

	// Check if each account has the required fields
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
	if len(awsConfig.BucketAllowList) == 0 {
		return fmt.Errorf("no buckets in the `bucket_allow_list`, please provide required buckets, or `*` for access to all buckets")
	}
	// If all checks pass, return nil (no error)
	return nil
}

func newAWSConfig(envJson string) (AWSConfig, error) {
	var awsConfig AWSConfig
	err := validateEnvJSON(envJson)
	if err != nil {
		return awsConfig, fmt.Errorf(err.Error())
	}
	jsonData, err := os.ReadFile(envJson)
	if err != nil {
		return awsConfig, err
	}

	if err := json.Unmarshal(jsonData, &awsConfig); err != nil {
		return awsConfig, err
	}
	return awsConfig, nil
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
