package blobstore

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-playground/validator"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) DeleteObject(bucket, key string) error {
	// Check if the object exists
	if _, err := s3Ctrl.GetMetaData(bucket, key); err != nil {
		//wrapping error since it does not pertain to the method's main functionality which in this case is deletion
		return fmt.Errorf("error checking object's existence while attempting to delete, %w", err)
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
	var nonExistentKeys []string

	for _, p := range key {
		s3Path := strings.TrimPrefix(p, "/")
		exists, err := s3Ctrl.KeyExists(bucket, p)
		if err != nil {
			// Wrap error with awserr based on the specific error type
			var awsErr error
			if errors.As(err, &awsErr) {
				// If it's an awserr.Error, return it directly
				return awsErr
			} else {
				// Otherwise, create a new awserr with generic message
				return awserr.New("S3KeyCheckError", "Error checking object existence", err)
			}
		}

		if !exists {
			nonExistentKeys = append(nonExistentKeys, s3Path)
		}

		object := &s3.ObjectIdentifier{
			Key: aws.String(s3Path),
		}

		objects = append(objects, object)
	}

	if len(nonExistentKeys) > 0 {
		// Don't delete anything, return error for non-existent keys
		return awserr.New("NoSuchKey", "Objects Not Found", fmt.Errorf("following keys do not exist: %+q", nonExistentKeys))
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
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, parameterKeyRequired, nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	err = s3Ctrl.DeleteObject(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error deleting object with `key`: %s", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("successfully deleted file with `key`: %s", key)
	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Successfully deleted object with `key`: %s", key))
}

func (bh *BlobHandler) HandleDeletePrefix(c echo.Context) error {
	const maxRetries = 3

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	prefix := c.QueryParam("prefix")
	if prefix == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, parameterPrefixRequired, nil)
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
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("failed to delete objects that belong to the `prefix`: %s", prefix))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if !objectsFound {
		appErr := configberry.NewAppError(configberry.NotFoundError, fmt.Sprintf("`prefix` %s not found", prefix), nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("Successfully deleted `prefix` %s and its content: ", prefix)
	return configberry.HandleSuccessfulResponse(c, fmt.Sprintf("Successfully deleted `prefix` %s and its content: ", prefix))
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
		appErr := configberry.NewAppError(configberry.ValidationError, parseingBodyRequestError, err)
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
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)

	}

	// Delete the objects using the deleteKeys function
	err = s3Ctrl.DeleteKeys(bucket, deleteRequest.Keys)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error deleting objects: %+q", deleteRequest.Keys))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("Successfully deleted objects: %+q", deleteRequest.Keys)
	return configberry.HandleSuccessfulResponse(c, "Successfully deleted objects")
}
