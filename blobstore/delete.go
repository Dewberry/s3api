package blobstore

import (
	"fmt"
	"strings"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-playground/validator"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) DeleteObjectIfExists(bucket, key string) error {
	// Check if the object exists
	if _, err := s3Ctrl.GetMetaData(bucket, key); err != nil {
		return err
	}

	// Delete the object
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := s3Ctrl.S3Svc.DeleteObject(deleteInput)
	if err != nil {
		return err
	}

	return nil
}

func (s3Ctrl *S3Controller) DeleteList(page *s3.ListObjectsV2Output, bucket string) error {
	if len(page.Contents) == 0 {
		return nil // No objects to delete in this page
	}

	var objectsToDelete []*s3.ObjectIdentifier
	for _, obj := range page.Contents {
		objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{Key: obj.Key})
	}

	// Perform the delete operation for the current page
	_, err := s3Ctrl.S3Svc.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objectsToDelete,
			Quiet:   aws.Bool(true),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (s3Ctrl *S3Controller) DeleteKeys(bucket string, key []string) error {
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

	_, err := s3Ctrl.S3Svc.DeleteObjects(input)
	if err != nil {
		return err
	}
	return nil
}

// HandleDeleteObject handles the API endpoint for deleting an object/s from an S3 bucket.
// It expects the 'key' query parameter to specify the object key and the 'bucket' query parameter to specify the bucket name (optional, falls back to environment variable 'AWS_S3_BUCKET').
// It returns an appropriate JSON response indicating the success or failure of the deletion.
func (bh *BlobHandler) HandleDeleteObject(c echo.Context) error {
	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, "parameter `key` is required", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	err = s3Ctrl.DeleteObjectIfExists(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error deleting object %s", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("successfully deleted file with key: %s", key)
	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Successfully deleted object: %s", key))
}

func (bh *BlobHandler) HandleDeletePrefix(c echo.Context) error {
	const maxRetries = 3

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	prefix := c.QueryParam("prefix")
	if prefix == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, "parameter `prefix` is required", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	var objectsFound bool

	err = s3Ctrl.GetListWithCallBack(bucket, prefix, false, func(page *s3.ListObjectsV2Output) error {
		if len(page.Contents) > 0 {
			objectsFound = true
		}

		if len(page.Contents) == 0 {
			return nil // No objects to delete in this page
		}

		// Perform the delete operation for the current page
		for retries := 0; retries < maxRetries; retries++ {
			deleteErr := s3Ctrl.DeleteList(page, bucket)
			if deleteErr == nil {
				// Successfully deleted, break out of the retry loop
				break
			}
			if retries == maxRetries-1 {
				// Log the error and return if we've reached the max retries
				log.Errorf("failed to delete objects in page after %d retries: %v", maxRetries, deleteErr)
				return deleteErr
			}
			// Log retry attempt
			log.Warnf("retrying delete for page, attempt %d/%d", retries+1, maxRetries)
		}

		return nil
	})

	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("failed to delete objects with prefix %s", prefix))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if !objectsFound {
		appErr := configberry.NewAppError(configberry.NotFoundError, fmt.Sprintf("prefix %s not found", prefix), nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("Successfully deleted prefix and its contents for prefix: %s", prefix)
	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Successfully deleted prefix and its contents for prefix: %s", prefix))
}

func (bh *BlobHandler) HandleDeleteObjectsByList(c echo.Context) error {
	// Define the validator
	validate := validator.New()

	// Parse the list of objects from the request body
	type DeleteRequest struct {
		Keys []string `json:"keys" validate:"required,min=1,dive,required"`
	}
	var deleteRequest DeleteRequest
	if err := c.Bind(&deleteRequest); err != nil {
		appErr := configberry.NewAppError(configberry.ValidationError, "error parsing request body", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	// Validate the request
	if err := validate.Struct(deleteRequest); err != nil {
		appErr := configberry.HandleStructValidationErrors(err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)

	}

	// Prepare the keys for deletion
	keys := make([]string, 0, len(deleteRequest.Keys))
	for _, p := range deleteRequest.Keys {
		s3Path := strings.TrimPrefix(p, "/")
		key := aws.String(s3Path)

		// Check if the key exists before appending it to the keys list
		keyExists, err := s3Ctrl.KeyExists(bucket, s3Path)
		if err != nil {
			appErr := configberry.HandleAWSError(err, "error checking if object exists")
			log.Error(configberry.LogErrorFormatter(appErr, true))
			return configberry.HandleErrorResponse(c, appErr)
		}
		if !keyExists {
			appErr := configberry.NewAppError(configberry.NotFoundError, fmt.Sprintf("object %s not found", s3Path), nil)
			log.Error(configberry.LogErrorFormatter(appErr, true))
			return configberry.HandleErrorResponse(c, appErr)
		}

		keys = append(keys, *key)
	}

	// Delete the objects using the deleteKeys function
	err = s3Ctrl.DeleteKeys(bucket, keys)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error deleting objects")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("Successfully deleted objects: %v", deleteRequest.Keys)
	return configberry.HandleSuccessfulResponse(c, "Successfully deleted objects")
}
