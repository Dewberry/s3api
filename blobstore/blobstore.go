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

func DoesArrcontain(a string, l []string) bool {
	for _, b := range l {
		if b == a {
			return true
		}
	}
	return false
}

func arrayContainsAll(array1, array2 []string) bool {
	set := make(map[string]bool)

	// Convert array1 into a set
	for _, str := range array1 {
		set[str] = true
	}

	// Check if every string element in array2 exists in the set
	for _, str := range array2 {
		if !set[str] {
			return false
		}
	}

	return true
}
