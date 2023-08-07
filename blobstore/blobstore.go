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

const (
	chars       = "abcdefghijklmnopqrstuvwxyz"
	randomChars = 6
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

// getList returns the list of object keys in the specified S3 bucket with the given prefix.

func GenerateRandomString() string {
	rand.Seed(time.Now().UnixNano())

	b := make([]byte, randomChars)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}

	return string(b)
}

// listBuckets returns the list of all S3 buckets.
func (bh *BlobHandler) listBuckets() (*s3.ListBucketsOutput, error) {
	// Set up input parameters for the ListBuckets API
	input := &s3.ListBucketsInput{}

	// Retrieve the list of buckets
	result, err := bh.S3Svc.ListBuckets(input)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (bh *BlobHandler) createBucket(bucketName string) error {
	// Set up input parameters for the CreateBucket API
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}

	// Create the bucket
	_, err := bh.S3Svc.CreateBucket(input)
	if err != nil {
		return err
	}

	return nil
}

// deleteBucket deletes the specified S3 bucket.
func (bh *BlobHandler) deleteBucket(bucketName string) error {
	// Set up input parameters for the DeleteBucket API
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}

	// Delete the bucket
	_, err := bh.S3Svc.DeleteBucket(input)
	if err != nil {
		return err
	}

	return nil
}

// getBucketACL retrieves the ACL (Access Control List) for the specified bucket.
func (bh *BlobHandler) getBucketACL(bucketName string) (*s3.GetBucketAclOutput, error) {
	// Set up input parameters for the GetBucketAcl API
	input := &s3.GetBucketAclInput{
		Bucket: aws.String(bucketName),
	}

	// Get the bucket ACL
	result, err := bh.S3Svc.GetBucketAcl(input)
	if err != nil {
		return nil, err
	}

	return result, nil
}
