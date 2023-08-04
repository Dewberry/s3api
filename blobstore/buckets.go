package blobstore

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) HandleListBuckets(c echo.Context) error {
	// Get the list of S3 buckets
	result, err := bh.listBuckets()
	if err != nil {
		log.Info("HandleListBuckets: Error listing buckets:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	// Return the list of bucket names as a slice of strings
	var bucketNames []string
	for _, bucket := range result.Buckets {
		bucketNames = append(bucketNames, aws.StringValue(bucket.Name))
	}

	log.Info("HandleListBuckets: Successfully retrieved list of buckets")
	return c.JSON(http.StatusOK, bucketNames)
}

func (bh *BlobHandler) HandleCreateBucket(c echo.Context) error {
	bucketName := c.QueryParam("name")

	if bucketName == "" {
		err := errors.New("request must include a `name` parameter")
		log.Info("HandleCreateBucket: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	// Check if the bucket already exists
	buckets, err := bh.listBuckets()
	if err != nil {
		log.Info("HandleCreateBucket: Error listing buckets:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	for _, b := range buckets.Buckets {
		if aws.StringValue(b.Name) == bucketName {
			err := fmt.Errorf("bucket with the name `%s` already exists", bucketName)
			log.Info("HandleCreateBucket: " + err.Error())
			return c.JSON(http.StatusConflict, err.Error())
		}
	}

	// Create the S3 bucket
	err = bh.createBucket(bucketName)
	if err != nil {
		log.Info("HandleCreateBucket: Error creating bucket:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleCreateBucket: Successfully created bucket:", bucketName)
	return c.JSON(http.StatusOK, "Bucket created successfully")
}

func (bh *BlobHandler) HandleDeleteBucket(c echo.Context) error {
	bucketName := c.QueryParam("name")

	if bucketName == "" {
		err := errors.New("request must include a `name` parameter")
		log.Info("HandleDeleteBucket: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	// Delete the S3 bucket
	err := bh.deleteBucket(bucketName)
	if err != nil {
		log.Info("HandleDeleteBucket: Error deleting bucket:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleDeleteBucket: Successfully deleted bucket:", bucketName)
	return c.JSON(http.StatusOK, "Bucket deleted successfully")
}

func (bh *BlobHandler) HandleGetBucketACL(c echo.Context) error {
	bucketName := c.QueryParam("name")

	if bucketName == "" {
		err := errors.New("request must include a `name` parameter")
		log.Info("HandleGetBucketACL: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	// Get the bucket ACL
	acl, err := bh.getBucketACL(bucketName)
	if err != nil {
		log.Info("HandleGetBucketACL: Error getting bucket ACL:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetBucketACL: Successfully retrieved ACL for bucket:", bucketName)
	return c.JSON(http.StatusOK, acl)
}
