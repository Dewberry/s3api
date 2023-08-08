package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) HandleMovePrefix(c echo.Context) error {
	srcPrefix := c.QueryParam("src_prefix")
	destPrefix := c.QueryParam("dest_prefix")
	if srcPrefix == "" || destPrefix == "" {
		err := errors.New("paramters `src_key` and `dest_key` are required")
		log.Error("HandleCopyPrefix", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleCopyPrefix: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	// List objects within the source prefix

	listOutput, err := bh.getList(bucket, srcPrefix, true)
	if err != nil {
		return errors.New("error listing objects with prefix " + srcPrefix + " in bucket " + bucket + ", " + err.Error())
	}

	if len(listOutput.Contents) == 0 {
		err := errors.New("source prefix" + srcPrefix + " does not exist")
		log.Error(err.Error())
		return c.JSON(http.StatusNotFound, err.Error())
	}

	// Copy each object to the destination prefix
	for _, object := range listOutput.Contents {
		srcObjectKey := aws.StringValue(object.Key)
		destObjectKey := strings.Replace(srcObjectKey, srcPrefix, destPrefix, 1)

		copyErr := bh.copyObject(bucket, srcObjectKey, destObjectKey)
		if copyErr != nil {
			// If an error occurs during copying, return immediately
			return copyErr
		}
	}

	return c.JSON(http.StatusOK, fmt.Sprintf("Succesfully moved prefix from %s to %s", srcPrefix, destPrefix))
}

func (bh *BlobHandler) HandleMoveObject(c echo.Context) error {
	srcObjectKey := c.QueryParam("src_key")
	destObjectKey := c.QueryParam("dest_key")
	if srcObjectKey == "" || destObjectKey == "" {
		err := errors.New("paramters `src_key` and `dest_key` are required")
		log.Error("HandleCopyObject", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleCopyObject: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	err = bh.copyObject(bucket, srcObjectKey, destObjectKey)
	if err != nil {
		log.Error("HandleCopyObject: Error when implementing copyObject", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, fmt.Sprintf("Succesfully moved object from %s to %s", srcObjectKey, destObjectKey))
}

// renameObject renames an object within a bucket.
func (bh *BlobHandler) copyObject(bucket, srcObjectKey, destObjectKey string) error {
	// Check if the new key already exists in the bucket
	_, err := bh.S3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(destObjectKey),
	})
	if err == nil {
		// The new key already exists, return an error to indicate conflict
		return errors.New("object" + srcObjectKey + "with the new key" + destObjectKey + "already exists in the bucket, " + err.Error())
	}

	// Set up input parameters for the CopyObject API to rename the object
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(bucket + "/" + srcObjectKey),
		Key:        aws.String(destObjectKey),
	}

	// Copy the object to the new key (effectively renaming)
	_, err = bh.S3Svc.CopyObject(copyInput)
	if err != nil {
		return errors.New("error copying object/s" + srcObjectKey + "with the new key" + destObjectKey + ", " + err.Error())
	}

	// Delete the source object
	_, err = bh.S3Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(srcObjectKey),
	})
	if err != nil {
		return errors.New("error deleting old object " + srcObjectKey + " in bucket " + bucket + ", " + err.Error())
	}

	return nil
}
