package blobstore

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s3Ctrl *S3Controller) KeyExists(bucket string, key string) (bool, error) {
	_, err := s3Ctrl.S3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
				return false, nil
			default:
				return false, fmt.Errorf("KeyExists: %s", err)
			}
		}
		return false, fmt.Errorf("KeyExists: %s", err)
	}
	return true, nil
}

// function that will get the most recently uploaded file in a prefix
func (s3Ctrl *S3Controller) getMostRecentModTime(bucket, prefix string) (time.Time, error) {
	// Initialize a time variable to store the most recent modification time
	var mostRecent time.Time

	// Call GetList to retrieve the list of objects with the specified prefix
	response, err := s3Ctrl.GetList(bucket, prefix, false)
	if err != nil {
		return time.Time{}, err
	}
	// Iterate over the returned objects to find the most recent modification time
	for _, item := range response.Contents {
		if item.LastModified != nil && item.LastModified.After(mostRecent) {
			mostRecent = *item.LastModified
		}
	}
	return mostRecent, nil
}
