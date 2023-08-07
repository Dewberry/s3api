package blobstore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

const (
	TEMP_PREFIX  string = "downloads-temp" // temp prefix prepended to targzs created by HandleGetPresignedURLMultiObj
	URL_EXP_DAYS int    = 7                // default validity period (expiration in days) for urls made in HandleGetPresignedURLMultiObj
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

func (bh *BlobHandler) getSize(list *s3.ListObjectsV2Output) (uint64, uint32, error) {
	var size uint64 = 0
	fileCount := uint32(len(list.Contents))

	for _, file := range list.Contents {
		size += uint64(*file.Size)
	}

	return size, fileCount, nil
}

// getList returns the list of object keys in the specified S3 bucket with the given prefix.

const (
	chars       = "abcdefghijklmnopqrstuvwxyz"
	randomChars = 6
)

func GenerateRandomString() string {
	rand.Seed(time.Now().UnixNano())

	b := make([]byte, randomChars)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}

	return string(b)
}

func getPresignedURL(bucket, key string, expDays int) (string, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := s3.New(sess)

	duration := time.Duration(expDays) * 24 * time.Hour
	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(duration)
}

func (bh *BlobHandler) tarS3Files(r *s3.ListObjectsV2Output, bucket string, outputFile string, prefix string) (err error) {
	uploader := s3manager.NewUploader(bh.Sess)
	pr, pw := io.Pipe()

	gzipWriter := gzip.NewWriter(pw)
	tarWriter := tar.NewWriter(gzipWriter)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		log.Debug("start writing files to:", outputFile)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(outputFile),
			Body:   pr,
		})
		if err != nil {
			log.Error("Failed to upload tar.gz file to S3:", err)
			return
		}
		log.Debug("completed writing files to:", outputFile)
	}()

	for _, item := range r.Contents {
		filePath := filepath.Join(strings.TrimPrefix(aws.StringValue(item.Key), prefix))
		copyObj := aws.StringValue(item.Key)
		log.Infof("Copying %s to %s", copyObj, outputFile)

		getResp, err := bh.S3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(copyObj),
		})
		if err != nil {
			log.Error("Failed to download file:", copyObj)
			return err
		}
		defer getResp.Body.Close()

		header := &tar.Header{
			Name: filePath,
			Size: *getResp.ContentLength,
			Mode: int64(0644),
		}

		err = tarWriter.WriteHeader(header)
		if err != nil {
			log.Error("Failed to write tar header for file:", copyObj)
			return err
		}

		_, err = io.Copy(tarWriter, getResp.Body)
		if err != nil {
			log.Error("Failed to write file content to tar:", copyObj)
			return err
		}
		log.Debug("Complete copying...", copyObj)
	}

	err = tarWriter.Close()
	if err != nil {
		log.Error("tar close failure")
		return err
	}

	gzipWriter.Close()
	if err != nil {
		log.Error("gzip close failure")
		return err
	}

	pw.Close()
	if err != nil {
		log.Error("pw close failure")
		return err
	}

	wg.Wait()

	log.Info("Done!")
	return nil
}

// func getFileModificationTime(filePath string) int64 {
// 	fi2, err := os.Open(filePath)
// 	fi, err := os.Stat(filePath)
// 	fmt.Println(fi2)
// 	if err != nil {
// 		return 0
// 	}
// 	return fi.ModTime().Unix()
// }

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

// renameObject renames an object within a bucket.
func (bh *BlobHandler) renameObject(bucketName, oldObjectKey, newObjectKey string) error {
	// Check if the new key already exists in the bucket
	_, err := bh.S3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(newObjectKey),
	})
	if err == nil {
		// The new key already exists, return an error to indicate conflict
		return errors.New("object with the new key already exists in the bucket")
	}

	// Set up input parameters for the CopyObject API to rename the object
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		CopySource: aws.String(bucketName + "/" + oldObjectKey),
		Key:        aws.String(newObjectKey),
	}

	// Copy the object to the new key (effectively renaming)
	_, err = bh.S3Svc.CopyObject(copyInput)
	if err != nil {
		return err
	}

	// Delete the old object after successful copy (rename)
	_, err = bh.S3Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(oldObjectKey),
	})
	if err != nil {
		return err
	}

	return nil
}
