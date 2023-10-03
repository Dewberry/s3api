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

func (bh *BlobHandler) RecursivelyDeleteObjects(bucket, prefix string) error {
	prefixPath := strings.Trim(prefix, "/") + "/"
	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefixPath),
	}
	resp, err := bh.S3Svc.ListObjectsV2(query)
	if err != nil {
		return fmt.Errorf("recursivelyDeleteObjects: error listing objects: %s", err)
	}
	if len(resp.Contents) > 0 {
		var objectsToDelete []*s3.ObjectIdentifier

		for _, obj := range resp.Contents {
			objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		if len(objectsToDelete) > 0 {
			_, err = bh.S3Svc.DeleteObjects(&s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3.Delete{
					Objects: objectsToDelete,
				},
			})

			if err != nil {
				return fmt.Errorf("recursivelyDeleteObjects: error Deleting objects %v: %s", objectsToDelete, err)
			}
		}
	} else {
		return fmt.Errorf("recursivelyDeleteObjects: object %s not found and no objects were deleted", prefixPath)
	}
	return nil
}

// HandleDeleteObject handles the API endpoint for deleting an object/s from an S3 bucket.
// It expects the 'key' query parameter to specify the object key and the 'bucket' query parameter to specify the bucket name (optional, falls back to environment variable 'S3_BUCKET').
// It returns an appropriate JSON response indicating the success or failure of the deletion.
func (bh *BlobHandler) HandleDeleteObject(c echo.Context) error {
	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Errorf("HandleDeleteObjects: %s", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	key := c.QueryParam("key")
	if key == "" {
		err = errors.New("parameter 'key' is required")
		log.Errorf("HandleDeleteObjects: %s", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	// If the key is not a folder, proceed with deleting a single object
	keyExist, err := bh.KeyExists(bucket, key)
	if err != nil {
		log.Errorf("HandleDeleteObjects: Error checking if key exists: %s", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found", key)
		log.Errorf("HandleDeleteObjects: %s", err.Error())
		return c.JSON(http.StatusNotFound, err.Error())
	}

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err = bh.S3Svc.DeleteObject(deleteInput)
	if err != nil {
		msg := fmt.Sprintf("error deleting object. %s", err.Error())
		log.Errorf("HandleDeleteObjects: %s", err.Error())
		return c.JSON(http.StatusInternalServerError, msg)
	}

	log.Info("HandleDeleteObjects: Successfully deleted file with key:", key)
	return c.JSON(http.StatusOK, fmt.Sprintf("Successfully deleted object: %s", key))
}

func (bh *BlobHandler) HandleDeletePrefix(c echo.Context) error {

	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Errorf("HandleDeleteObjects: %s", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err = errors.New("parameter 'prefix' is required")
		log.Errorf("HandleDeleteObjects: %s", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	response, err := bh.GetList(bucket, prefix, false)
	if err != nil {
		log.Errorf("HandleDeleteObjects:  Error getting list: %s", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}
	if *response.KeyCount == 0 {
		err := fmt.Errorf("the specified prefix %s does not exist in S3", prefix)
		log.Errorf("HandleDeleteObjects: %s", err.Error())
		return c.JSON(http.StatusNotFound, err.Error())
	}
	// This will recursively delete all objects with the specified prefix
	err = bh.RecursivelyDeleteObjects(bucket, prefix)
	if err != nil {
		msg := fmt.Sprintf("error deleting objects. %s", err.Error())
		log.Errorf("HandleDeleteObjects: %s", msg)
		return c.JSON(http.StatusInternalServerError, msg)
	}

	log.Info("HandleDeleteObjects: Successfully deleted prefix and its contents for prefix:", prefix)
	return c.JSON(http.StatusOK, "Successfully deleted prefix and its contents")

}

func (bh *BlobHandler) DeleteKeys(bucket string, key []string) error {
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

	_, err := bh.S3Svc.DeleteObjects(input)
	if err != nil {
		return fmt.Errorf("deleteKeys: error Deleting objects: %s", err.Error())
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
		log.Errorf("HandleDeleteObjectsByList: Error parsing request body: %s" + err.Error())
		return c.JSON(http.StatusBadRequest, "Invalid request body")
	}

	// Ensure there are keys to delete
	if len(deleteRequest.Keys) == 0 {
		errMsg := "No keys to delete. Please provide 'keys' in the request body."
		log.Errorf("HandleDeleteObjectsByList: %s", errMsg)
		return c.JSON(http.StatusUnprocessableEntity, errMsg)
	}

	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Errorf("HandleDeleteObjectsByList: %s", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	// Prepare the keys for deletion
	keys := make([]string, 0, len(deleteRequest.Keys))
	for _, p := range deleteRequest.Keys {
		s3Path := strings.TrimPrefix(p, "/")
		key := aws.String(s3Path)

		// Check if the key exists before appending it to the keys list
		keyExists, err := bh.KeyExists(bucket, s3Path)
		if err != nil {
			msg := fmt.Errorf("error checking if key exists. %s", err.Error())
			log.Errorf("HandleDeleteObjectsByList: %s", msg)
			return c.JSON(http.StatusInternalServerError, msg)
		}
		if !keyExists {
			errMsg := fmt.Sprintf("object %s not found", s3Path)
			log.Errorf("HandleDeleteObjectsByList: %s", errMsg)
			return c.JSON(http.StatusNotFound, errMsg)
		}

		keys = append(keys, *key)
	}

	// Delete the objects using the deleteKeys function
	err = bh.DeleteKeys(bucket, keys)
	if err != nil {
		msg := fmt.Sprintf("error deleting objects. %s", err.Error())
		log.Errorf("HandleDeleteObjectsByList: %s", msg)
		return c.JSON(http.StatusInternalServerError, msg)
	}

	log.Info("HandleDeleteObjectsByList: Successfully deleted objects:", deleteRequest.Keys)
	return c.JSON(http.StatusOK, "Successfully deleted objects")
}
