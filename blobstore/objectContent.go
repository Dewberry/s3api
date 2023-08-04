package blobstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) HandleObjectContents(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleObjectContents: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	fileEXT := strings.ToLower(filepath.Ext(key))

	if fileEXT == "" {
		err := errors.New("file has no extension")
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleObjectContents: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found", key)
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	var contentType string
	switch fileEXT {
	case ".csv", ".txt", ".py":
		contentType = "text/plain"
	case ".png":
		contentType = "image/png"
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".html", ".log":
		contentType = "text/html"
	case ".json":
		contentType = "application/json"
	default:
		err := fmt.Errorf("file of type `%s` cannot be viewed", filepath.Ext(key))
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := bh.S3Svc.GetObject(input)
	if err != nil {
		log.Info("HandleObjectContents: Error getting object from S3:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	var data []byte
	err = json.NewDecoder(output.Body).Decode(&data)
	if err != nil {
		log.Info("HandleObjectContents: Error reading object data:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleObjectContents: Successfully fetched object data for key:", key)
	return c.Blob(http.StatusOK, contentType, data)
}
