package blobstore

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
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

		// Deleting the source objects should be handled carefully
		// Ensure that your application logic requires this before proceeding
		err := s3Ctrl.DeleteList(page, bucket)
		if err != nil {
			return err
		}
		return nil
	}

	err := s3Ctrl.GetListWithCallBack(bucket, srcPrefix, false, processPage)
	if err != nil {
		return err
	}

	// Check if objects were found after processing all pages
	if !objectsFound {
		return errors.New("source prefix not found")
	}

	return nil
}

func (s3Ctrl *S3Controller) CopyObject(bucket, srcObjectKey, destObjectKey string) error {
	// Check if the source and destination keys are the same
	if srcObjectKey == destObjectKey {
		return fmt.Errorf("source `%s` and destination `%s` keys are identical; no action taken", srcObjectKey, destObjectKey)
	}
	// Check if the old key exists in the bucket
	oldKeyExists, err := s3Ctrl.KeyExists(bucket, srcObjectKey)
	if err != nil {
		return err
	}

	if !oldKeyExists {
		return fmt.Errorf("`srcObjectKey` " + srcObjectKey + " does not exist")
	}
	// Check if the new key already exists in the bucket
	newKeyExists, err := s3Ctrl.KeyExists(bucket, destObjectKey)
	if err != nil {
		return err
	}
	if newKeyExists {
		return fmt.Errorf(destObjectKey + " already exists in the bucket; duplication will cause an overwrite. Please rename dest_key to a different name")
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
		return err
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

	if !strings.HasSuffix(params["srcPrefix"], "/") {
		params["srcPrefix"] = params["srcPrefix"] + "/"
	}
	if !strings.HasSuffix(params["destPrefix"], "/") {
		params["destPrefix"] = params["destPrefix"] + "/"
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	err = s3Ctrl.MovePrefix(bucket, params["srcPrefix"], params["destPrefix"])
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error moving prefix")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Successfully moved prefix from %s to %s", params["srcPrefix"], params["destPrefix"]))
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
		appErr := configberry.NewAppError(configberry.ValidationError, fmt.Sprintf("`bucket` %s is not available", bucket), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	err = s3Ctrl.CopyObject(bucket, params["srcObjectKey"], params["destObjectKey"])
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error copying prefix")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Succesfully moved object from %s to %s", params["srcObjectKey"], params["destObjectKey"]))
}
