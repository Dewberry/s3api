package blobstore

import (
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (bh *BlobHandler) GetSize(page *s3.ListObjectsV2Output, totalSize *uint64, fileCount *uint64) error {
	if page == nil {
		return fmt.Errorf("input page is nil")
	}

	for _, file := range page.Contents {
		if file.Size == nil {
			return fmt.Errorf("file size is nil")
		}
		*totalSize += uint64(*file.Size)
		*fileCount++
	}

	return nil
}

// HandleGetSize retrieves the total size and the number of files in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleGetSize(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		errMsg := fmt.Errorf("request must include a `prefix` parameter")
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
	permissions, fullAccess, statusCode, err := bh.GetS3ReadPermissions(c, bucket)
	if err != nil {
		log.Error(err.Error())
		return c.JSON(statusCode, err.Error())
	}
	if !fullAccess && !isPermittedPrefix(bucket, prefix, permissions) {
		errMsg := fmt.Errorf("user does not have read permission to read this prefix %s", prefix)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusForbidden, errMsg.Error())
	}
	// Check if the prefix points directly to an object
	isObject, err := s3Ctrl.KeyExists(bucket, prefix)
	if err != nil {
		errMsg := fmt.Errorf("error checking if prefix is an object: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	if isObject {
		errMsg := fmt.Errorf("the provided prefix %s points to a single object rather than a collection", prefix)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusTeapot, errMsg.Error())
	}

	var totalSize uint64
	var fileCount uint64
	err = s3Ctrl.GetListWithCallBack(bucket, prefix, false, func(page *s3.ListObjectsV2Output) error {
		return bh.GetSize(page, &totalSize, &fileCount)
	})

	if err != nil {
		errMsg := fmt.Errorf("error processing objects: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	if fileCount == 0 {
		errMsg := fmt.Errorf("prefix %s not found", prefix)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusNotFound, errMsg.Error())
	}
	response := struct {
		Size      uint64 `json:"size"`
		FileCount uint64 `json:"file_count"`
		Prefix    string `json:"prefix"`
	}{
		Size:      totalSize,
		FileCount: fileCount,
		Prefix:    prefix,
	}

	log.Info("Successfully retrieved size for prefix:", prefix)
	return c.JSON(http.StatusOK, response)
}

func (bh *BlobHandler) HandleGetMetaData(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("request must include a `key` parameter")
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
	permissions, fullAccess, statusCode, err := bh.GetS3ReadPermissions(c, bucket)
	if err != nil {
		log.Error(err.Error())
		return c.JSON(statusCode, err.Error())
	}

	if !fullAccess && !isPermittedPrefix(bucket, key, permissions) {
		errMsg := fmt.Errorf("user does not have read permission to read this key %s", key)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusForbidden, errMsg.Error())
	}
	result, err := s3Ctrl.GetMetaData(bucket, key)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			errMsg := fmt.Errorf("object %s not found", key)
			log.Error(errMsg.Error())
			return c.JSON(http.StatusNotFound, errMsg.Error())
		}
		errMsg := fmt.Errorf("error getting metadata: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Info("successfully retrieved metadata for key:", key)
	return c.JSON(http.StatusOK, result)
}

func (bh *BlobHandler) HandleGetObjExist(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("request must include a `key` parameter")
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

	permissions, fullAccess, statusCode, err := bh.GetS3ReadPermissions(c, bucket)
	if err != nil {
		log.Error(err.Error())
		return c.JSON(statusCode, err.Error())
	}

	if !fullAccess && !isPermittedPrefix(bucket, key, permissions) {
		errMsg := fmt.Errorf("user does not have read permission to read this key %s", key)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusForbidden, errMsg.Error())
	}

	result, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		errMsg := fmt.Errorf("error checking if object exists: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	log.Info("successfully retrieved metadata for key:", key)
	return c.JSON(http.StatusOK, result)
}

func (s3Ctrl *S3Controller) GetMetaData(bucket, key string) (*s3.HeadObjectOutput, error) {
	// Set up the input parameters for the list objects operation
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Ctrl.S3Svc.HeadObject(input)
	if err != nil {
		return nil, err
	}

	return result, nil
}
