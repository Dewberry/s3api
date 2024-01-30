package blobstore

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) FetchObjectContent(bucket string, key string) ([]byte, error) {
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

	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (bh *BlobHandler) HandleObjectContents(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("parameter 'key' is required")
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

	body, err := s3Ctrl.FetchObjectContent(bucket, key)
	if err != nil {
		errMsg := fmt.Errorf("error fetching object's content: %s", err.Error())
		log.Error(errMsg.Error())
		if strings.Contains(err.Error(), "object") {
			return c.JSON(http.StatusNotFound, errMsg.Error())
		} else {
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}
	}

	log.Info("successfully fetched object data for key:", key)
	//TODO: add contentType
	return c.Blob(http.StatusOK, "", body)
}
