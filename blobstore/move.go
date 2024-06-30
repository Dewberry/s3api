package blobstore

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

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
				return err
			}
		}

		err := s3Ctrl.DeleteList(page, bucket)
		if err != nil {
			return fmt.Errorf("error deleting list, %w", err)
		}
		return nil
	}

	err := s3Ctrl.GetListWithCallBack(bucket, srcPrefix, false, processPage)
	if err != nil {
		return err
	}

	// Check if objects were found after processing all pages
	if !objectsFound {
		return errors.New("`src_prefix` not found")
	}

	return nil
}

func (s3Ctrl *S3Controller) CopyObject(bucket, srcObjectKey, destObjectKey string) error {
	// Check if the source and destination keys are the same
	if srcObjectKey == destObjectKey {
		return awserr.New("InvalidParameter", "Source and Destination Keys are Identical",
			fmt.Errorf("`src_key` %s and `dest_key` %s cannot be the same for move operation", srcObjectKey, destObjectKey))

	}

	// Check if the new key already exists in the bucket
	newKeyExists, err := s3Ctrl.KeyExists(bucket, destObjectKey)
	if err != nil {
		return err
	}
	if newKeyExists {
		return awserr.New("AlreadyExists", "Destination Key Already Exists",
			fmt.Errorf("%s already exists in the bucket; consider renaming `dest_key`", destObjectKey))
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
		return err
	}

	// Delete the source object
	_, err = s3Ctrl.S3Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(srcObjectKey),
	})
	if err != nil {
		return fmt.Errorf("error deleting object with `src_key` %s, %w", srcObjectKey, err)
	}

	return nil
}

func (bh *BlobHandler) HandleMovePrefix(c echo.Context) error {
	params := map[string]string{
		"srcPrefix":  c.QueryParam("src_prefix"),
		"destPrefix": c.QueryParam("dest_prefix"),
	}
	if appErr := configberry.CheckRequiredParams(params); appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, false))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	adjustedPrefix, appErr := s3Ctrl.checkAndAdjustPrefix(bucket, params["srcPrefix"])
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	params["srcPrefix"] = adjustedPrefix
	fmt.Println(params["srcPrefix"])
	adjustedPrefix, appErr = s3Ctrl.checkAndAdjustPrefix(bucket, params["destPrefix"])
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	params["destPrefix"] = adjustedPrefix

	err = s3Ctrl.MovePrefix(bucket, params["srcPrefix"], params["destPrefix"])
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error moving `src_prefix` %s, to `dest_prefix` %s", params["srcPrefix"], params["destPrefix"]))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Successfully moved `src_prefix` %s, to `dest_prefix` %s", params["srcPrefix"], params["destPrefix"]))
}

func (bh *BlobHandler) HandleMoveObject(c echo.Context) error {
	params := map[string]string{
		"srcObjectKey":  c.QueryParam("src_key"),
		"destObjectKey": c.QueryParam("dest_key"),
	}
	if appErr := configberry.CheckRequiredParams(params); appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, false))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.ValidationError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	err = s3Ctrl.CopyObject(bucket, params["srcObjectKey"], params["destObjectKey"])
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error copying object with `src_key` %s to `dest_key` %s", params["srcObjectKey"], params["destObjectKey"]))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Succesfully moved object with `src_key` %s to `dest_key` %s", params["srcObjectKey"], params["destObjectKey"]))
}
