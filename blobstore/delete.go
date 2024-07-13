package blobstore

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

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
		return fmt.Errorf("error deleting objects: %v", err)
	}

	return nil
}

func (s3Ctrl *S3Controller) RecursivelyDeleteObjects(bucket, prefix string) error {
	var objectsFound bool
	err := s3Ctrl.GetListWithCallBack(bucket, prefix, false, func(page *s3.ListObjectsV2Output) error {
		if len(page.Contents) > 0 {
			objectsFound = true
		}
		return s3Ctrl.DeleteList(page, bucket)
	})
	if err != nil {
		return fmt.Errorf("error processing objects for deletion: %v", err)
	}

	if !objectsFound {
		return fmt.Errorf("prefix not found")
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
		errMsg := fmt.Errorf("parameter `bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("parameter `key` is required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	httpCode, err := bh.CheckUserS3Permission(c, bucket, key, []string{"write"})
	if err != nil {
		errMsg := fmt.Errorf("error while checking for user permission: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(httpCode, errMsg.Error())
	}

	// If the key is not a folder, proceed with deleting a single object
	keyExist, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		errMsg := fmt.Errorf("error checking if object exists: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	if !keyExist {
		errMsg := fmt.Errorf("object %s not found", key)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusNotFound, errMsg.Error())
	}

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err = s3Ctrl.S3Svc.DeleteObject(deleteInput)
	if err != nil {
		errMsg := fmt.Errorf("error deleting object. %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Infof("successfully deleted file with key: %s", key)
	return c.JSON(http.StatusOK, fmt.Sprintf("Successfully deleted object: %s", key))
}

func (bh *BlobHandler) HandleDeletePrefix(c echo.Context) error {

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("parameter `bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		errMsg := fmt.Errorf("parameter `prefix` is required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	httpCode, err := bh.CheckUserS3Permission(c, bucket, prefix, []string{"write"})
	if err != nil {
		errMsg := fmt.Errorf("error while checking for user permission: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(httpCode, errMsg.Error())
	}

	err = s3Ctrl.RecursivelyDeleteObjects(bucket, prefix)
	if err != nil {
		if strings.Contains(err.Error(), "prefix not found") {
			errMsg := fmt.Errorf("no objects found with prefix: %s", prefix)
			log.Error(errMsg.Error())
			return c.JSON(http.StatusNotFound, errMsg.Error())
		}
		errMsg := fmt.Errorf("error deleting objects: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	log.Info("Successfully deleted prefix and its contents for prefix:", prefix)
	return c.JSON(http.StatusOK, "Successfully deleted prefix and its contents")
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
		return fmt.Errorf("error deleting objects: %s", err.Error())
	}
	return nil
}

func (bh *BlobHandler) HandleDeleteObjectsByList(c echo.Context) error {
	// Parse the list of objects from the request body
	type DeleteRequest struct {
		Keys []string `json:"keys"`
	}
	var deleteRequest DeleteRequest
	if err := c.Bind(&deleteRequest); err != nil {
		errMsg := fmt.Errorf("error parsing request body: %s" + err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error())
	}

	// Ensure there are keys to delete
	if len(deleteRequest.Keys) == 0 {
		errMsg := fmt.Errorf("no keys to delete. Please provide 'keys' in the request body")
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

	// Prepare the keys for deletion
	keys := make([]string, 0, len(deleteRequest.Keys))
	for _, p := range deleteRequest.Keys {
		s3Path := strings.TrimPrefix(p, "/")
		key := aws.String(s3Path)

		// Check if the key exists before appending it to the keys list
		keyExists, err := s3Ctrl.KeyExists(bucket, s3Path)
		if err != nil {
			errMsg := fmt.Errorf("error checking if object exists. %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg)
		}
		if !keyExists {
			errMsg := fmt.Errorf("object %s not found", s3Path)
			log.Error(errMsg.Error())
			return c.JSON(http.StatusNotFound, errMsg.Error())
		}

		httpCode, err := bh.CheckUserS3Permission(c, bucket, s3Path, []string{"write"})
		if err != nil {
			errMsg := fmt.Errorf("error while checking for user permission: %s", err)
			log.Error(errMsg.Error())
			return c.JSON(httpCode, errMsg.Error())
		}

		keys = append(keys, *key)
	}

	// Delete the objects using the deleteKeys function
	err = s3Ctrl.DeleteKeys(bucket, keys)
	if err != nil {
		errMsg := fmt.Errorf("error deleting objects. %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg)
	}

	log.Info("Successfully deleted objects:", deleteRequest.Keys)
	return c.JSON(http.StatusOK, "Successfully deleted objects")
}
