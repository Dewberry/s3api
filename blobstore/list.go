package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

// ListByPrefix retrieves a list of object keys in the specified S3 bucket with a given prefix.
// It takes the prefix, recursive flag, and bucket name from the query parameters and returns the object keys as a JSON response.
//
// The prefix parameter represents the prefix for the objects in the S3 bucket.
// The delimiter flag, when set to "false", includes objects from subdirectories as well.
// The bucket parameter represents the name of the S3 bucket. If not provided in the query parameters, it falls back to the S3_BUCKET environment variable.
// getSize returns the total size and file count of objects in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleListByPrefix(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Info("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	delimiterParam := c.QueryParam("delimiter")

	var delimiter bool

	if delimiterParam == "true" || delimiterParam == "false" {
		var err error
		delimiter, err = strconv.ParseBool(c.QueryParam("delimiter"))
		if err != nil {
			log.Info("HandleListByPrefix: Error parsing `delimiter` param:", err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

	} else {
		err := errors.New("request must include a `delimiter`, options are `true` or `false`")
		log.Info("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())

	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleListByPrefix: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	response, err := bh.getList(bucket, prefix, delimiter)
	if err != nil {
		log.Info("HandleListByPrefix: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}

	//return the list of objects as a slice of strings
	var objectKeys []string
	for _, object := range response.Contents {
		objectKeys = append(objectKeys, aws.StringValue(object.Key))
	}

	log.Info("HandleListByPrefix: Successfully retrieved list by prefix:", prefix)
	return c.JSON(http.StatusOK, objectKeys)
}

func (bh *BlobHandler) HandleBucketViewList(c echo.Context) error {
	prefix := c.QueryParam("prefix")

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleBucketViewList: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	delimiterParam := c.QueryParam("delimiter")

	var delimiter bool

	if delimiterParam == "true" || delimiterParam == "false" {
		var err error
		delimiter, err = strconv.ParseBool(c.QueryParam("delimiter"))
		if err != nil {
			log.Info("HandleListByPrefix: Error parsing `delimiter` param:", err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

	} else {
		err := errors.New("request must include a `delimiter`, options are `true` or `false`")
		log.Info("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())

	}
	startIndexParam := c.QueryParam("start_index")
	endIndexParam := c.QueryParam("end_index")

	var startIndex, endIndex int
	var err error

	// Convert start_index and end_index parameters to integers if provided
	if startIndexParam != "" {
		startIndex, err = strconv.Atoi(startIndexParam)
		if err != nil {
			// Handle the error when the "start_index" parameter is invalid
			return c.JSON(http.StatusBadRequest, fmt.Sprintf("Invalid `start_index` parameter: %d", startIndex))
		}
	}

	if endIndexParam != "" {
		endIndex, err = strconv.Atoi(endIndexParam)
		if err != nil {
			// Handle the error when the "end_index" parameter is invalid
			return c.JSON(http.StatusBadRequest, fmt.Sprintf("Invalid `end_index` parameter: %d", endIndex))
		}
	}
	var result *[]ListResult
	result, err = listDir(bucket, prefix, bh.S3Svc, delimiter, startIndex, endIndex)
	if err != nil {
		log.Info("HandleBucketViewList: Error listing bucket:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleBucketViewList: Successfully retrieved bucket view list with prefix:", prefix)
	return c.JSON(http.StatusOK, result)
}
