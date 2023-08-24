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
	return c.Blob(http.StatusOK,"", body)
}
