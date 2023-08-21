package blobstore

import (
	"errors"
	"fmt"
	"io"
	"net/http"
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
		log.Error("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	fileEXT := strings.ToLower(filepath.Ext(key))

	if fileEXT == "" {
		err := errors.New("file has no extension")
		log.Error("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Error("HandleObjectContents: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found", key)
		log.Error("HandleObjectContents: " + err.Error())
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
		log.Error("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := bh.S3Svc.GetObject(input)
	if err != nil {
		log.Error("HandleObjectContents: Error getting object from S3:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	body, err := io.ReadAll(output.Body)
	if err != nil {
		log.Error("HandleObjectContents: Error reading object data:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleObjectContents: Successfully fetched object data for key:", key)
	return c.JSON(http.StatusOK, body)
}
