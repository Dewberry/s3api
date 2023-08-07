package blobstore

import (
	"errors"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

// HandleRenameObject renames an object within a bucket.
func (bh *BlobHandler) HandleCopyObject(c echo.Context) error {
	srcBucketName := c.QueryParam("src_bucket")
	destBucketName := c.QueryParam("dest_bucket")
	srcObjectKey := c.QueryParam("src_key")
	destObjectKey := c.QueryParam("dest_key")
	srcPrefix := c.QueryParam("src_prefix")
	destPrefix := c.QueryParam("dest_prefix")

	if srcBucketName == "" || destBucketName == "" {
		err := errors.New("request must include `src_bucket` and `dest_bucket` parameters")
		log.Error("HandleCopyObject: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	if (srcObjectKey != "" && destObjectKey != "") || (srcPrefix != "" && destPrefix != "") {
		// Determine if the operation involves copying a single object or a prefix
		if srcObjectKey != "" && destObjectKey != "" {
			//make sure that we are in the correct endpoint url
			isCorrectPath := strings.Contains(c.Request().URL.String(), "/object/")

			if !isCorrectPath {
				err := errors.New("user provided object keys but is attempting to copy prefix through `prefix/copy` endpoint")
				log.Error("HandleCopyObject: " + err.Error())
				return c.JSON(http.StatusUnprocessableEntity, err.Error())
			}
			// Copy a single object
			// Check if the source object exists
			_, err := bh.S3Svc.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(srcBucketName),
				Key:    aws.String(srcObjectKey),
			})
			if err != nil {
				err := errors.New("source object " + srcObjectKey + " does not exist: " + err.Error())
				log.Error("error while chekcing is source object exists " + err.Error())
				return c.JSON(http.StatusNotFound, err.Error())
			}

			// Call your copyObject function with the appropriate arguments
			return bh.copyObject(srcBucketName, destBucketName, srcObjectKey, destObjectKey)
		} else if srcPrefix != "" && destPrefix != "" {
			// Copy a prefix
			//make sure that we are in the correct endpoint url
			isCorrectPath := strings.Contains(c.Request().URL.String(), "/prefix/")

			if !isCorrectPath {
				err := errors.New("user provided prefix keys but is attempting to copy prefix through `object/copy` endpoint")
				log.Error("HandleCopyObject: " + err.Error())
				return c.JSON(http.StatusUnprocessableEntity, err.Error())
			}
			// Check if the source prefix exists
			resp, err := bh.S3Svc.ListObjectsV2(&s3.ListObjectsV2Input{
				Bucket:  aws.String(srcBucketName),
				Prefix:  aws.String(srcPrefix),
				MaxKeys: aws.Int64(1), // Only need to check if any objects exist
			})
			if err != nil {
				err := errors.New("error listing source prefix: " + err.Error())
				log.Error("error while chekcing is source object exists " + err.Error())
				return c.JSON(http.StatusInternalServerError, err.Error())
			}
			if len(resp.Contents) == 0 {
				err := errors.New("source prefix" + srcPrefix + "does not exist")
				log.Error(err.Error())
				return c.JSON(http.StatusNotFound, err.Error())
			}

			// Call your copyPrefix function with the appropriate arguments
			// Call your copyPrefix function with the appropriate arguments
			err = bh.copyPrefix(srcBucketName, destBucketName, srcPrefix, destPrefix)
			if err != nil {
				log.Error("Error in copyPrefix method: " + err.Error())
				return c.JSON(http.StatusInternalServerError, err.Error())
			}
			log.Info("HandleCopyObject: Successfully copied object/prefix")
			return c.JSON(http.StatusOK, "Object/prefix copied successfully")
		} else {
			err := errors.New("invalid combination of parameters")
			log.Error(err.Error())
			return c.JSON(http.StatusUnprocessableEntity, err.Error())
		}
	} else {
		err := errors.New("invalid combination of parameters")
		log.Error("HandleCopyObject: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
}

func (bh *BlobHandler) copyPrefix(srcBucketName, destBucketName, srcPrefix, destPrefix string) error {
	// List objects within the source prefix
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(srcBucketName),
		Prefix: aws.String(srcPrefix),
	}

	listOutput, err := bh.S3Svc.ListObjectsV2(listInput)
	if err != nil {
		return errors.New("error listing objects with prefix " + srcPrefix + " in bucket " + srcBucketName + ", " + err.Error())
	}

	// Copy each object to the destination prefix
	for _, object := range listOutput.Contents {
		srcObjectKey := aws.StringValue(object.Key)
		destObjectKey := strings.Replace(srcObjectKey, srcPrefix, destPrefix, 1)

		copyErr := bh.copyObject(srcBucketName, destBucketName, srcObjectKey, destObjectKey)
		if copyErr != nil {
			// If an error occurs during copying, return immediately
			return copyErr
		}
	}

	return nil
}

// renameObject renames an object within a bucket.
func (bh *BlobHandler) copyObject(srcBucketName, destBucketName, srcObjectKey, destObjectKey string) error {
	// Check if the new key already exists in the bucket
	_, err := bh.S3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(destBucketName),
		Key:    aws.String(destObjectKey),
	})
	if err == nil {
		// The new key already exists, return an error to indicate conflict
		return errors.New("object" + destObjectKey + "with the new key" + destBucketName + "already exists in the bucket, " + err.Error())
	}

	// Set up input parameters for the CopyObject API to rename the object
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(destBucketName),
		CopySource: aws.String(srcBucketName + "/" + srcObjectKey),
		Key:        aws.String(destObjectKey),
	}

	// Copy the object to the new key (effectively renaming)
	_, err = bh.S3Svc.CopyObject(copyInput)
	if err != nil {
		return errors.New("error copying object/s" + destObjectKey + "with the new key" + destBucketName + ", " + err.Error())
	}

	// Delete the source object
	_, err = bh.S3Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(srcBucketName),
		Key:    aws.String(srcObjectKey),
	})
	if err != nil {
		// If deleting the source object fails, attempt to revert the copy
		revertInput := &s3.CopyObjectInput{
			Bucket:     aws.String(srcBucketName),
			CopySource: aws.String(destBucketName + "/" + destObjectKey),
			Key:        aws.String(srcObjectKey),
		}

		// Revert the copy by copying the object back to the original key
		_, revertErr := bh.S3Svc.CopyObject(revertInput)
		if revertErr != nil {
			return errors.New("error deleting old object " + srcObjectKey + " in bucket " + srcBucketName + ", and failed to revert copy, " + err.Error())
		}

		return errors.New("error deleting old object " + srcObjectKey + " in bucket " + srcBucketName + ", " + err.Error())
	}

	return nil
}
