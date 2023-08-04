package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

// HandleDeleteObject handles the API endpoint for deleting an object/s from an S3 bucket.
// It expects the 'key' query parameter to specify the object key and the 'bucket' query parameter to specify the bucket name (optional, falls back to environment variable 'S3_BUCKET').
// It returns an appropriate JSON response indicating the success or failure of the deletion.
func (bh *BlobHandler) HandleDeleteObjects(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Info("HandleDeleteObjects: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleDeleteObjects: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	// Split the key into segments
	segments := strings.Split(key, "/")

	// Check if the key is two levels deep
	if len(segments) < 3 {
		errMsg := fmt.Errorf("invalid key: %s. Only objects three levels deep can be deleted", key)
		log.Info("HandleDeleteObjects: " + errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error())
	}

	// Check if the key represents a prefix
	if strings.HasSuffix(key, "/") {
		response, err := bh.getList(bucket, key, false)
		if err != nil {
			log.Info("HandleDeleteObjects: Error getting list:", err.Error())
			return c.JSON(http.StatusInternalServerError, err)
		}
		if *response.KeyCount == 0 {
			errMsg := fmt.Errorf("the specified prefix %s does not exist in S3", key)
			log.Info("HandleDeleteObjects: " + errMsg.Error())
			return c.JSON(http.StatusBadRequest, errMsg.Error())
		}
		// This will recursively delete all objects with the specified prefix
		err = RecursivelyDeleteObjects(bh.S3Svc, bucket, key)
		if err != nil {
			msg := fmt.Sprintf("error deleting objects. %s", err.Error())
			log.Info("HandleDeleteObjects: " + msg)
			return c.JSON(http.StatusInternalServerError, msg)
		}

		log.Info("HandleDeleteObjects: Successfully deleted folder and its contents for prefix:", key)
		return c.JSON(http.StatusOK, "Successfully deleted folder and its contents")
	}

	// If the key is not a folder, proceed with deleting a single object
	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleDeleteObjects: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found, add a trailing `/` if you want to delete multiple files", key)
		log.Info("HandleDeleteObjects: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err = bh.S3Svc.DeleteObject(deleteInput)
	if err != nil {
		msg := fmt.Sprintf("error deleting object. %s", err.Error())
		log.Info("HandleDeleteObjects: " + msg)
		return c.JSON(http.StatusInternalServerError, msg)
	}

	log.Info("HandleDeleteObjects: Successfully deleted file with key:", key)
	return c.JSON(http.StatusOK, "Successfully deleted file")
}

func (bh *BlobHandler) HandleDeleteObjectsByList(c echo.Context) error {
	// Parse the list of objects from the request body
	type DeleteRequest struct {
		Keys []string `json:"keys"`
	}
	var deleteRequest DeleteRequest
	if err := c.Bind(&deleteRequest); err != nil {
		log.Info("HandleDeleteObjects: Error parsing request body:", err.Error())
		return c.JSON(http.StatusBadRequest, "Invalid request body")
	}

	// Ensure there are keys to delete
	if len(deleteRequest.Keys) == 0 {
		errMsg := "No keys to delete. Please provide 'keys' in the request body."
		log.Info("HandleDeleteObjects: " + errMsg)
		return c.JSON(http.StatusBadRequest, errMsg)
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleDeleteObjects: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	// Prepare the keys for deletion
	keys := make([]string, 0, len(deleteRequest.Keys))
	for _, p := range deleteRequest.Keys {
		s3Path := strings.TrimPrefix(p, "/")
		key := aws.String(s3Path)

		// Check if the key exists before appending it to the keys list
		keyExists, err := bh.keyExists(bucket, s3Path)
		if err != nil {
			msg := fmt.Sprintf("error checking if key exists. %s", err.Error())
			log.Info("HandleDeleteObjects: " + msg)
			return c.JSON(http.StatusInternalServerError, msg)
		}
		if !keyExists {
			errMsg := fmt.Sprintf("object %s not found, add a trailing `/` if you want to delete multiple files", s3Path)
			log.Info("HandleDeleteObjects: " + errMsg)
			return c.JSON(http.StatusBadRequest, errMsg)
		}

		keys = append(keys, *key)
	}

	// Delete the objects using the deleteKeys function
	err := deleteKeys(bh.S3Svc, bucket, keys...)
	if err != nil {
		msg := fmt.Sprintf("error deleting objects. %s", err.Error())
		log.Info("HandleDeleteObjects: " + msg)
		return c.JSON(http.StatusInternalServerError, msg)
	}

	log.Info("HandleDeleteObjects: Successfully deleted objects:", deleteRequest.Keys)
	return c.JSON(http.StatusOK, "Successfully deleted objects")
}
