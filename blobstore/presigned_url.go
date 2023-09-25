package blobstore

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) GetPresignedURL(bucket, key string, expDays int) (string, error) {
	duration := time.Duration(expDays) * 24 * time.Hour
	req, _ := bh.S3Svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(duration)
}

func (bh *BlobHandler) tarS3Files(r *s3.ListObjectsV2Output, bucket string, outputFile string, prefix string) (err error) {
	uploader := s3manager.NewUploader(bh.Sess)
	pr, pw := io.Pipe()

	gzipWriter := gzip.NewWriter(pw)
	tarWriter := tar.NewWriter(gzipWriter)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		log.Debug("start writing files to:", outputFile)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(outputFile),
			Body:   pr,
		})
		if err != nil {
			log.Error("Failed to upload tar.gz file to S3:", err)
			return
		}
		log.Debug("completed writing files to:", outputFile)
	}()

	for _, item := range r.Contents {
		filePath := filepath.Join(strings.TrimPrefix(aws.StringValue(item.Key), prefix))
		copyObj := aws.StringValue(item.Key)
		log.Debugf("Copying %s to %s", copyObj, outputFile)

		getResp, err := bh.S3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(copyObj),
		})
		if err != nil {
			log.Error("Failed to download file:", copyObj)
			return err
		}
		defer getResp.Body.Close()

		header := &tar.Header{
			Name: filePath,
			Size: *getResp.ContentLength,
			Mode: int64(0644),
		}

		err = tarWriter.WriteHeader(header)
		if err != nil {
			log.Error("Failed to write tar header for file:", copyObj)
			return err
		}

		_, err = io.Copy(tarWriter, getResp.Body)
		if err != nil {
			log.Error("Failed to write file content to tar:", copyObj)
			return err
		}
		log.Debug("Complete copying...", copyObj)
	}

	err = tarWriter.Close()
	if err != nil {
		log.Error("tar close failure")
		return err
	}

	gzipWriter.Close()
	if err != nil {
		log.Error("gzip close failure")
		return err
	}

	pw.Close()
	if err != nil {
		log.Error("pw close failure")
		return err
	}

	wg.Wait()

	log.Debugf("completed Tar of file succesfully")
	return nil
}

func (bh *BlobHandler) HandleGetPresignedURL(c echo.Context) error {
	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter `key` is required")
		log.Error("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	keyExist, err := bh.KeyExists(bucket, key)
	if err != nil {
		log.Error("HandleGetPresignedURL: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found", key)
		log.Error("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusNotFound, err.Error())
	}
	// Set the expiration time for the pre-signed URL
	expPeriod, err := strconv.Atoi(os.Getenv("URL_EXP_DAYS"))
	if err != nil {
		log.Error("HandleGetPresignedURL: Error getting `URL_EXP_DAYS` from env file:", err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	url, err := bh.GetPresignedURL(bucket, key, expPeriod)
	if err != nil {
		log.Error("HandleGetPresignedURL: Error getting presigned URL:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetPresignedURL: Successfully generated presigned URL for key:", key)
	return c.JSON(http.StatusOK, url)
}

func (bh *BlobHandler) HandleGetPresignedURLMultiObj(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Error("HandleGetPresignedURLMultiObj: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	response, err := bh.GetList(bucket, prefix, false)
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	if *response.KeyCount == 0 {
		errMsg := fmt.Errorf("the specified prefix %s does not exist in S3", prefix)
		log.Error("HandleGetPresignedURLMultiObj: " + errMsg.Error())
		return c.JSON(http.StatusNotFound, errMsg.Error())
	}
	//check if size is below 5GB
	size, fileCount, err := bh.GetSize(response)
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error getting size:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	limit := uint64(1024 * 1024 * 1024 * 5)
	if size > limit {
		err := fmt.Errorf("HandleGetPresignedURLMultiObj: Request entity is larger than %v GB, current file size is: %d, and current file count is: %d", float64(limit)/(1024*1024*1024), size, fileCount)
		log.Error("HandleGetPresignedURLMultiObj: ", err.Error())
		return c.JSON(http.StatusRequestEntityTooLarge, err.Error())
	}

	ext := filepath.Ext(prefix)
	base := strings.TrimSuffix(prefix, ext)
	uuid := generateRandomString()
	filename := fmt.Sprintf("%s-%s.%s", filepath.Base(base), uuid, "tar.gz")
	outputFile := filepath.Join(os.Getenv("TEMP_PREFIX"), filename)

	err = bh.tarS3Files(response, bucket, outputFile, prefix)
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error tarring S3 files:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	expPeriod, err := strconv.Atoi(os.Getenv("URL_EXP_DAYS"))
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error getting `URL_EXP_DAYS` from env file:", err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	href, err := bh.GetPresignedURL(bucket, outputFile, expPeriod)
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error getting presigned URL:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetPresignedURLMultiObj: Successfully generated presigned URL for prefix:", prefix)
	return c.JSON(http.StatusOK, string(href))
}
