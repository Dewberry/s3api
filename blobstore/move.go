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
		errMsg := fmt.Errorf("parameters `src_key` and `dest_key` are required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
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
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	err = s3Ctrl.MovePrefix(bucket, srcPrefix, destPrefix)
	if err != nil {
		if strings.Contains(err.Error(), "source prefix not found") {
			errMsg := fmt.Errorf("no objects found with source prefix: %s", srcPrefix)
			log.Error(errMsg.Error())
			return c.JSON(http.StatusNotFound, errMsg.Error())
		}
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, fmt.Sprintf("Successfully moved prefix from %s to %s", srcPrefix, destPrefix))
}

func (s3Ctrl *S3Controller) MovePrefix(bucket, srcPrefix, destPrefix string) error {
	var objectsFound bool

	processPage := func(page *s3.ListObjectsV2Output) error {
		if len(page.Contents) == 0 {
			return nil // No objects to process in this page
		}
		objectsFound = true // Objects found, set the flag

		for _, object := range page.Contents {
			srcObjectKey := aws.StringValue(object.Key)
			destObjectKey := strings.Replace(srcObjectKey, srcPrefix, destPrefix, 1)

			// Copy the object to the new location
			copyInput := &s3.CopyObjectInput{
				Bucket:     aws.String(bucket),
				CopySource: aws.String(bucket + "/" + srcObjectKey),
				Key:        aws.String(destObjectKey),
			}
			_, err := s3Ctrl.S3Svc.CopyObject(copyInput)
			if err != nil {
				return fmt.Errorf("error copying object %s to %s: %v", srcObjectKey, destObjectKey, err)
			}
		}

		// Deleting the source objects should be handled carefully
		// Ensure that your application logic requires this before proceeding
		err := s3Ctrl.DeleteList(page, bucket)
		if err != nil {
			return fmt.Errorf("error deleting from source prefix %s: %v", srcPrefix, err)
		}
		return nil
	}

	err := s3Ctrl.GetListWithCallBack(bucket, srcPrefix, false, processPage)
	if err != nil {
		return fmt.Errorf("error processing objects for move: %v", err)
	}

	// Check if objects were found after processing all pages
	if !objectsFound {
		return fmt.Errorf("source prefix not found")
	}

	return nil
}

func (bh *BlobHandler) HandleMoveObject(c echo.Context) error {
	srcObjectKey := c.QueryParam("src_key")
	destObjectKey := c.QueryParam("dest_key")
	if srcObjectKey == "" || destObjectKey == "" {
		errMsg := fmt.Errorf("paramters `src_key` and `dest_key` are required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	err = s3Ctrl.CopyObject(bucket, srcObjectKey, destObjectKey)
	if err != nil {
		if strings.Contains(err.Error(), "keys are identical; no action taken") {
			log.Error(err.Error())
			return c.JSON(http.StatusBadRequest, err.Error()) // 400 Bad Request
		} else if strings.Contains(err.Error(), "already exists in the bucket; duplication will cause an overwrite") {
			log.Error(err.Error())
			return c.JSON(http.StatusConflict, err.Error()) // 409 Conflict
		} else if strings.Contains(err.Error(), "does not exist") {
			log.Error(err.Error())
			return c.JSON(http.StatusNotFound, err.Error())
		}
		errMsg := fmt.Errorf("error when copying object: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
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
