package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) HandleGetPresignedURL(c echo.Context) error {
	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleGetPresignedURL: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter `key` is required")
		log.Info("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleGetPresignedURL: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found", key)
		log.Info("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	// Set the expiration time for the pre-signed URL
	expirationStr := c.QueryParam("expiration")
	var expiration time.Duration
	if expirationStr == "" {
		expiration = time.Minute // Default value set to 1 minute
	} else {
		duration, err := time.ParseDuration(expirationStr)
		if err != nil {
			// Handle error when parsing the expiration string
			errorMsg := fmt.Errorf("invalid expiration value %s Please provide a valid duration format", expirationStr)
			log.Info("HandleGetPresignedURL: " + errorMsg.Error())
			return c.JSON(http.StatusBadRequest, errorMsg)
		}
		expiration = duration
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, _ := bh.S3Svc.GetObjectRequest(input)
	url, err := req.Presign(expiration)
	if err != nil {
		log.Info("HandleGetPresignedURL: Error generating presigned URL:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetPresignedURL: Successfully generated presigned URL for key:", key)
	return c.JSON(http.StatusOK, url)
}

func (bh *BlobHandler) HandleGetPresignedURLMultiObj(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Info("HandleGetPresignedURLMultiObj: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleGetPresignedURLMultiObj: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	response, err := bh.getList(bucket, prefix, false)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}
	//check if size is below 5GB
	size, fileCount, err := bh.getSize(response)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error getting size:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	limit := uint64(1024 * 1024 * 1024 * 5)
	if size > limit {
		log.Printf("HandleGetPresignedURLMultiObj: Request entity is larger than %v GB, current file size is: %d, and current file count is: %d", float64(limit)/(1024*1024*1024), size, fileCount)
		return c.JSON(http.StatusRequestEntityTooLarge, fmt.Sprintf("request entity is larger than %v GB, current file size is: %d, and current file count is: %d", float64(limit)/(1024*1024*1024), size, fileCount))
	}

	if *response.KeyCount == 0 {
		errMsg := fmt.Errorf("the specified prefix %s does not exist in S3", prefix)
		log.Info("HandleGetPresignedURLMultiObj: " + errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error())
	}

	ext := filepath.Ext(prefix)
	base := strings.TrimSuffix(prefix, ext)
	uuid := GenerateRandomString()
	filename := fmt.Sprintf("%s-%s.%s", filepath.Base(base), uuid, "tar.gz")
	outputFile := filepath.Join(TEMP_PREFIX, filename)

	err = bh.tarS3Files(response, bucket, outputFile, prefix)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error tarring S3 files:", err.Error())
		return err
	}

	href, err := getPresignedURL(bucket, outputFile, URL_EXP_DAYS)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error getting presigned URL:", err.Error())
		return err
	}

	log.Info("HandleGetPresignedURLMultiObj: Successfully generated presigned URL for prefix:", prefix)
	return c.JSON(http.StatusOK, string(href))
}
