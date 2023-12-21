package blobstore

import (
	"fmt"
	"math/rand"
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

func arrayContains(a string, arr []string) bool {
	for _, b := range arr {
		if b == a {
			return true
		}
	}
	return false
}

func isIdenticalArray(array1, array2 []string) bool {
	if len(array1) != len(array2) {
		return false
	}

	set := make(map[string]bool)

	for _, str := range array1 {
		set[str] = true
	}

	for _, str := range array2 {
		if !set[str] {
			return false
		}
	}

	return true
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
