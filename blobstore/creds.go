package blobstore

import (
	"encoding/json"
	"os"
)

type Credentials interface {
	Exist() bool
}

type AWSCreds struct {
	AWS_ACCESS_KEY_ID     string `json:"AWS_ACCESS_KEY_ID"`
	AWS_SECRET_ACCESS_KEY string `json:"AWS_SECRET_ACCESS_KEY"`
	AWS_REGION            string `json:"AWS_REGION"`
}

func (creds AWSCreds) Exist() bool {
	if creds.AWS_ACCESS_KEY_ID == "" ||
		creds.AWS_SECRET_ACCESS_KEY == "" ||
		creds.AWS_REGION == "" {
		return false
	}
	return true
}

type AWSConfig struct {
	Accounts []AWSCreds `json:"accounts"`
}

func NewAWSConfig(envJson string) (AWSConfig, error) {
	var awsConfig AWSConfig
	jsonData, err := os.ReadFile(envJson)
	if err != nil {
		return awsConfig, err
	}

	if err := json.Unmarshal(jsonData, &awsConfig); err != nil {
		return awsConfig, err
	}
	return awsConfig, nil
}

func AWSFromENV() AWSCreds {
	var creds AWSCreds
	creds.AWS_ACCESS_KEY_ID = os.Getenv("AWS_ACCESS_KEY_ID")
	creds.AWS_SECRET_ACCESS_KEY = os.Getenv("AWS_SECRET_ACCESS_KEY")
	creds.AWS_REGION = os.Getenv("AWS_REGION")
	return creds
}

type MinioConfig struct {
	S3Endpoint      string `json:"MINIO_S3_ENDPOINT"`
	S3Region        string `json:"MINIO_S3_REGION"`
	DisableSSL      string `json:"MINIO_S3_DISABLE_SSL"`
	ForcePathStyle  string `json:"MINIO_S3_FORCE_PATH_STYLE"`
	AccessKeyID     string `json:"MINIO_ACCESS_KEY_ID"`
	SecretAccessKey string `json:"MINIO_SECRET_ACCESS_KEY"`
	S3Mock          string `json:"S3_MOCK"`
}

func NewMinioConfig() MinioConfig {
	var mc MinioConfig
	mc.S3Endpoint = os.Getenv("MINIO_S3_ENDPOINT")
	mc.S3Region = os.Getenv("MINIO_S3_REGION")
	mc.DisableSSL = os.Getenv("MINIO_S3_DISABLE_SSL")
	mc.ForcePathStyle = os.Getenv("MINIO_S3_FORCE_PATH_STYLE")
	mc.AccessKeyID = os.Getenv("MINIO_ACCESS_KEY_ID")
	mc.SecretAccessKey = os.Getenv("MINIO_SECRET_ACCESS_KEY")
	return mc
}

func (creds MinioConfig) Exist() bool {
	if creds.S3Endpoint == "" ||
		creds.S3Region == "" ||
		creds.DisableSSL == "" ||
		creds.ForcePathStyle == "" ||
		creds.AccessKeyID == "" ||
		creds.SecretAccessKey == "" {
		return false
	}

	return true
}
