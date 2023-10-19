package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (bh *BlobHandler) HandleMovePrefix(c echo.Context) error {
	srcPrefix := c.QueryParam("src_prefix")
	destPrefix := c.QueryParam("dest_prefix")
	if srcPrefix == "" || destPrefix == "" {
		err := errors.New("parameters `src_key` and `dest_key` are required")
		log.Error("HandleCopyPrefix", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	if !strings.HasSuffix(srcPrefix, "/") {
		srcPrefix = srcPrefix + "/"
	}
	if !strings.HasSuffix(destPrefix, "/") {
		destPrefix = destPrefix + "/"
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		log.Errorf("bucket %s is not available", bucket)
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	err = s3Ctrl.CopyPrefix(bucket, srcPrefix, destPrefix)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return c.JSON(http.StatusNotFound, err.Error())
		}
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, fmt.Sprintf("Successfully moved prefix from %s to %s", srcPrefix, destPrefix))
}

func (s3Ctrl *S3Controller) CopyPrefix(bucket, srcPrefix, destPrefix string) error {
	// List objects within the source prefix
	listOutput, err := s3Ctrl.GetList(bucket, srcPrefix, true)
	if err != nil {
		return errors.New("error listing objects with prefix " + srcPrefix + " in bucket " + bucket + ", " + err.Error())
	}

	if len(listOutput.Contents) == 0 {
		return errors.New("source prefix " + srcPrefix + " does not exist")
	}

	// Copy each object to the destination prefix
	for _, object := range listOutput.Contents {
		srcObjectKey := aws.StringValue(object.Key)
		destObjectKey := strings.Replace(srcObjectKey, srcPrefix, destPrefix, 1)

		copyErr := s3Ctrl.CopyObject(bucket, srcObjectKey, destObjectKey)
		if copyErr != nil {
			// If an error occurs during copying, return immediately
			return copyErr
		}
	}
	return nil
}

func (bh *BlobHandler) HandleMoveObject(c echo.Context) error {
	srcObjectKey := c.QueryParam("src_key")
	destObjectKey := c.QueryParam("dest_key")
	if srcObjectKey == "" || destObjectKey == "" {
		err := errors.New("paramters `src_key` and `dest_key` are required")
		log.Error("HandleCopyObject", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		log.Errorf("bucket %s is not available", bucket)
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	err = s3Ctrl.CopyObject(bucket, srcObjectKey, destObjectKey)
	if err != nil {
		if strings.Contains(err.Error(), "keys are identical; no action taken") {
			return c.JSON(http.StatusBadRequest, err.Error()) // 400 Bad Request
		} else if strings.Contains(err.Error(), "already exists in the bucket; duplication will cause an overwrite") {
			return c.JSON(http.StatusConflict, err.Error()) // 409 Conflict
		} else if strings.Contains(err.Error(), "does not exist") {
			return c.JSON(http.StatusNotFound, err.Error())
		}
		log.Error("HandleCopyObject: Error when implementing copyObject", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, fmt.Sprintf("Succesfully moved object from %s to %s", srcObjectKey, destObjectKey))
}

func (s3Ctrl *S3Controller) CopyObject(bucket, srcObjectKey, destObjectKey string) error {
	// Check if the source and destination keys are the same
	if srcObjectKey == destObjectKey {
		return fmt.Errorf("source `%s` and destination `%s` keys are identical; no action taken", srcObjectKey, destObjectKey)
	}
	// Check if the old key exists in the bucket
	oldKeyExists, err := s3Ctrl.KeyExists(bucket, srcObjectKey)
	if err != nil {
		return fmt.Errorf("error checking if object %s exists: %s", destObjectKey, err.Error())
	}
	if !oldKeyExists {
		return errors.New("`srcObjectKey` " + srcObjectKey + " does not exist")
	}
	// Check if the new key already exists in the bucket
	newKeyExists, err := s3Ctrl.KeyExists(bucket, destObjectKey)
	if err != nil {
		return fmt.Errorf("error checking if object %s exists: %s", destObjectKey, err.Error())
	}
	if newKeyExists {
		return errors.New(destObjectKey + " already exists in the bucket; duplication will cause an overwrite. Please rename dest_key to a different name")
	}
	// Set up input parameters for the CopyObject API to rename the object
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(bucket + "/" + srcObjectKey),
		Key:        aws.String(destObjectKey),
	}

	// Copy the object to the new key (effectively renaming)
	_, err = s3Ctrl.S3Svc.CopyObject(copyInput)
	if err != nil {
		return errors.New("error copying object" + srcObjectKey + "with the new key" + destObjectKey + ", " + err.Error())
	}

	// Delete the source object
	_, err = s3Ctrl.S3Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(srcObjectKey),
	})
	if err != nil {
		return errors.New("error deleting old object " + srcObjectKey + " in bucket " + bucket + ", " + err.Error())
	}

	return nil
}
