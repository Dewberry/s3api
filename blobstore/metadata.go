package blobstore

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) GetSize(list *s3.ListObjectsV2Output) (uint64, uint32, error) {
	if list == nil {
		return 0, 0, errors.New("getSize: input list is nil")
	}

	var size uint64 = 0
	fileCount := uint32(len(list.Contents))

	for _, file := range list.Contents {
		if file.Size == nil {
			return 0, 0, errors.New("getSize: file size is nil")
		}
		size += uint64(*file.Size)
	}

	return size, fileCount, nil
}

// HandleGetSize retrieves the total size and the number of files in the specified S3 bucket with the given prefix.
// HandleGetSize retrieves the total size and the number of files in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleGetSize(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Error("HandleGetSize: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleGetSize: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	// Check if the prefix points directly to an object
	isObject, err := bh.KeyExists(bucket, prefix)
	if err != nil {
		log.Error("HandleGetSize: Error checking if prefix is an object:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	if isObject {
		// Prefix points directly to an object instead of a collection of objects
		return c.JSON(http.StatusTeapot, "The provided prefix points to a single object rather than a collection")
	}
	list, err := bh.GetList(bucket, prefix, false)
	if err != nil {
		log.Error("HandleGetSize: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	if len(list.Contents) == 0 {
		// No objects found with the provided prefix
		return c.JSON(http.StatusNotFound, "Prefix not found")
	}

	size, fileCount, err := bh.GetSize(list)
	if err != nil {
		log.Error("HandleGetSize: Error getting size:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	response := struct {
		Size      uint64 `json:"size"`
		FileCount uint32 `json:"file_count"`
		Prefix    string `json:"prefix"`
	}{
		Size:      size,
		FileCount: fileCount,
		Prefix:    prefix,
	}

	log.Info("HandleGetSize: Successfully retrieved size for prefix:", prefix)
	return c.JSON(http.StatusOK, response)
}

func (bh *BlobHandler) GetMetaData(bucket, key string) (*s3.HeadObjectOutput, error) {
	// Set up the input parameters for the list objects operation
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := bh.S3Svc.HeadObject(input)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (bh *BlobHandler) HandleGetMetaData(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("request must include a `key` parameter")
		log.Error("HandleGetMetaData: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleGetMetaData: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	result, err := bh.GetMetaData(bucket, key)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			err := fmt.Errorf("object %s not found", key)
			log.Error("HandleGetMetaData: " + err.Error())
			return c.JSON(http.StatusNotFound, err.Error())
		}
		log.Error("HandleGetMetaData: Error getting metadata:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetMetaData: Successfully retrieved metadata for key:", key)
	return c.JSON(http.StatusOK, result)
}

func (bh *BlobHandler) HandleGetObjExist(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("request must include a `key` parameter")
		log.Error("HandleGetObjExist: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleGetObjExist: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	result, err := bh.KeyExists(bucket, key)
	if err != nil {
		log.Error("HandleGetObjExist: " + err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	log.Info("HandleGetObjExist: Successfully retrieved metadata for key:", key)
	return c.JSON(http.StatusOK, result)
}
