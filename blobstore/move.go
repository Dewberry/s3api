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
	if !strings.HasSuffix(srcPrefix, "/") {
		srcPrefix = srcPrefix + "/"
	}
	if !strings.HasSuffix(destPrefix, "/") {
		destPrefix = destPrefix + "/"
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
		err := errors.New("source prefix " + srcPrefix + " does not exist")
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
	fmt.Println("tetsing before getting teh bucket param")
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

	fmt.Println("tetsing after getting teh bucket param")
	return c.JSON(http.StatusOK, fmt.Sprintf("Succesfully moved object from %s to %s", srcObjectKey, destObjectKey))
}

// renameObject renames an object within a bucket.
func (bh *BlobHandler) copyObject(bucket, srcObjectKey, destObjectKey string) error {

	// Check if the old key already exists in the bucket
	oldKeyExists, err := bh.keyExists(bucket, srcObjectKey)
	if err != nil {
		// The new key already exists, return an error to indicate conflict
		return fmt.Errorf("object %s already exists in the bucket; duplication will cause an overwrite. Please rename dest_key to a different name: %v", destObjectKey, err)
	}
	if !oldKeyExists {
		return errors.New("object " + srcObjectKey + " does not exist")
	}
	// Check if the new key already exists in the bucket
	newKeyExists, err := bh.keyExists(bucket, srcObjectKey)
	if err != nil {
		// The new key already exists, return an error to indicate conflict
		return errors.New("object " + srcObjectKey + " with the new key " + destObjectKey + " already exists in the bucket, " + err.Error())
	}
	if !newKeyExists {
		return errors.New(destObjectKey + " already exists in the bucket; duplication will cause an overwrite. Please rename dest_key to a different name")
	}
	// Check if the source and destination keys are the same
	if srcObjectKey == destObjectKey {
		return fmt.Errorf("source `%s` and destination `%s` keys are identical; no action taken", srcObjectKey, destObjectKey)
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
