package blobstore

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
)

func (bh *BlobHandler) keyExists(bucket string, key string) (bool, error) {
	_, err := bh.S3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
				return false, nil
			default:
				return false, fmt.Errorf("keyExists: %s", err)
			}
		}
		return false, fmt.Errorf("keyExists: %s", err)
	}
	return true, nil
}

func getBucketParam(c echo.Context, defaultBucket string) (string, error) {
	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if defaultBucket == "" {
			return "", errors.New("error: `bucket` parameter was not provided by the user and is not a default value")
		}
		bucket = defaultBucket
	}
	return bucket, nil
}

func generateRandomString() string {
	const (
		chars       = "abcdefghijklmnopqrstuvwxyz"
		randomChars = 6
	)

	rand.Seed(time.Now().UnixNano())

	b := make([]byte, randomChars)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}

	return string(b)
}
