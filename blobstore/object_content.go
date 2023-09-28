package blobstore

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) FetchObjectContent(bucket string, key string) ([]byte, error) {
	keyExist, err := bh.KeyExists(bucket, key)
	if err != nil {
		return nil, err
	}
	if !keyExist {
		return nil, fmt.Errorf("object %s not found", key)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := bh.S3Svc.GetObject(input)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

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

	body, err := bh.FetchObjectContent(bucket, key)
	if err != nil {
		log.Error("HandleObjectContents: " + err.Error())
		if strings.Contains(err.Error(), "object") {
			return c.JSON(http.StatusNotFound, err.Error())
		} else {
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
	}

	log.Info("HandleObjectContents: Successfully fetched object data for key:", key)
	//TODO: add contentType
	return c.Blob(http.StatusOK, "", body)
}
