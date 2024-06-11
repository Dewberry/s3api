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
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) FetchObjectContent(bucket string, key string) (io.ReadCloser, error) {
	keyExist, err := s3Ctrl.KeyExists(bucket, key)
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
	output, err := s3Ctrl.S3Svc.GetObject(input)
	if err != nil {
		return nil, err
	}

	return output.Body, nil
}

func (bh *BlobHandler) HandleObjectContents(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Error("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("bucket %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	outPutBody, err := s3Ctrl.FetchObjectContent(bucket, key)
	if err != nil {
		log.Error("HandleObjectContents: " + err.Error())
		if strings.Contains(err.Error(), "object") {
			return c.JSON(http.StatusNotFound, err.Error())
		} else {
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
	}
	body, err := io.ReadAll(outPutBody)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	log.Info("HandleObjectContents: Successfully fetched object data for key:", key)
	//TODO: add contentType
	return c.Blob(http.StatusOK, "", body)
}
