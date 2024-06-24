package blobstore

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
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

func (mc MinioConfig) validateMinioConfig() error {
	missingFields := []string{}
	if mc.S3Endpoint == "" {
		missingFields = append(missingFields, "S3Endpoint")
	}
	if mc.DisableSSL == "" {
		missingFields = append(missingFields, "DisableSSL")
	}
	if mc.ForcePathStyle == "" {
		missingFields = append(missingFields, "ForcePathStyle")
	}
	if mc.AccessKeyID == "" {
		missingFields = append(missingFields, "AccessKeyID")
	}
	if mc.SecretAccessKey == "" {
		missingFields = append(missingFields, "SecretAccessKey")
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("missing fields:  %+q", missingFields)
	}
	return nil
}

func (mc MinioConfig) minIOSessionManager() (*s3.S3, *session.Session, error) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(mc.S3Endpoint),
		Region:           aws.String("us-east-1"),
		Credentials:      credentials.NewStaticCredentials(mc.AccessKeyID, mc.SecretAccessKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, nil, err
	}
	log.Info("Using minio to mock s3")

	// Check if the bucket exists
	s3SVC := s3.New(sess)
	_, err = s3SVC.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(mc.Bucket),
	})
	if err != nil {
		// Bucket does not exist, create it
		_, err := s3SVC.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(mc.Bucket),
		})
		if err != nil {
			log.Errorf("error creating bucket: %s", err.Error())
			return nil, nil, nil
		}
		log.Info("Bucket created successfully")
	} else {
		log.Info("Bucket already exists")
	}

	// Create the policy as a byte array
	policyBytes := []byte(`{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": [
            "*"
        ]
      },
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::test-bucket/*"
      ]
    }
  ]}`)

	// Set the policy on the bucket
	_, err = s3SVC.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(mc.Bucket),
		Policy: aws.String(string(policyBytes)),
	})
	if err != nil {
		log.Errorf("error setting bucket policy: %s", err.Error())
		// Handle error appropriately
	} else {
		log.Info("Bucket policy set successfully")
	}

	return s3SVC, sess, nil
}

func (ac AWSCreds) aWSSessionManager() (*s3.S3, *session.Session, error) {
	log.Info("Using AWS S3")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(ac.AWS_ACCESS_KEY_ID, ac.AWS_SECRET_ACCESS_KEY, ""),
	})
	if err != nil {
		return nil, nil, err
	}
	return s3.New(sess), sess, nil
}
