package blobstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
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
				return false, err
			}
		}
		return false, err
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

func RecursivelyDeleteObjects(client *s3.S3, bucket, folderPath string) error {
	s3Path := strings.Trim(folderPath, "/") + "/"
	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(s3Path),
	}
	resp, err := client.ListObjectsV2(query)
	if err != nil {
		return err
	}
	if len(resp.Contents) > 0 {
		var objectsToDelete []*s3.ObjectIdentifier

		for _, obj := range resp.Contents {
			objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		if len(objectsToDelete) > 0 {
			_, err = client.DeleteObjects(&s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3.Delete{
					Objects: objectsToDelete,
				},
			})

			if err != nil {
				return err
			}
		}
	} else {
		return errors.New("object not found and no objects were deleted")
	}

	return nil
}

func (bh *BlobHandler) UploadS3Obj(bucket string, key string, body io.ReadCloser) error {
	// Initialize the multipart upload to S3
	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	resp, err := bh.S3Svc.CreateMultipartUpload(params)
	if err != nil {
		return fmt.Errorf("error initializing multipart upload. %s", err.Error())
	}

	// Create the variables that will track upload progress
	var totalBytes int64 = 0
	var partNumber int64 = 1
	completedParts := []*s3.CompletedPart{}
	buffer := bytes.NewBuffer(nil)

	for {
		// Read from the request body into the buffer
		chunkSize := 1024 * 1024 * 5
		buf := make([]byte, chunkSize)
		n, err := body.Read(buf)

		// This would be a true error while reading
		if err != nil && err != io.EOF {
			return fmt.Errorf("error copying POST body to S3. %s", err.Error())
		}

		// Add the buffer data to the buffer
		buffer.Write(buf[:n])

		// Upload a part if the buffer contains more than 5mb of data to avoid AWS EntityTooSmall error
		if buffer.Len() > chunkSize {
			params := &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   resp.UploadId,
				PartNumber: aws.Int64(partNumber),
				Body:       bytes.NewReader(buffer.Bytes()),
			}

			result, err := bh.S3Svc.UploadPart(params)
			if err != nil {
				return fmt.Errorf("error streaming POST body to S3. %s, %+v", err.Error(), result)
			}

			totalBytes += int64(buffer.Len())
			completedParts = append(completedParts, &s3.CompletedPart{
				ETag:       result.ETag,
				PartNumber: aws.Int64(partNumber),
			})

			buffer.Reset()
			partNumber++
		}

		if err == io.EOF {
			break
		}
	}

	// Upload the remaining data as the last part
	params2 := &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   resp.UploadId,
		PartNumber: aws.Int64(partNumber),
		Body:       bytes.NewReader(buffer.Bytes()),
	}

	result, err := bh.S3Svc.UploadPart(params2)
	if err != nil {
		return fmt.Errorf("error streaming POST body to S3. %s, %+v", err.Error(), result)
	}

	totalBytes += int64(buffer.Len())
	completedParts = append(completedParts, &s3.CompletedPart{
		ETag:       result.ETag,
		PartNumber: aws.Int64(partNumber),
	})

	// Complete the multipart upload
	completeParams := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
	}
	_, err = bh.S3Svc.CompleteMultipartUpload(completeParams)
	if err != nil {
		return fmt.Errorf("error completing multipart upload. %s", err.Error())
	}

	return nil
}

func deleteKeys(svc *s3.S3, bucket string, key ...string) error {
	objects := make([]*s3.ObjectIdentifier, 0, len(key))
	for _, p := range key {
		s3Path := strings.TrimPrefix(p, "/")
		object := &s3.ObjectIdentifier{
			Key: aws.String(s3Path),
		}
		objects = append(objects, object)
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	}

	_, err := svc.DeleteObjects(input)
	return err
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
