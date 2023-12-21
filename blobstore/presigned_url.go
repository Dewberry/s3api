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
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) GetDownloadPresignedURL(bucket, key string, expDays int) (string, error) {
	duration := time.Duration(expDays) * 24 * time.Hour
	req, _ := s3Ctrl.S3Svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(duration)
}

func (s3Ctrl *S3Controller) tarS3Files(r *s3.ListObjectsV2Output, bucket string, outputFile string, prefix string) (err error) {
	uploader := s3manager.NewUploader(s3Ctrl.Sess)
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
			log.Errorf("failed to upload tar.gz file to S3: %s", err)
			return
		}
		log.Debug("completed writing files to:", outputFile)
	}()

	for _, item := range r.Contents {
		filePath := filepath.Join(strings.TrimPrefix(aws.StringValue(item.Key), prefix))
		copyObj := aws.StringValue(item.Key)
		log.Debugf("copying %s to %s", copyObj, outputFile)

		getResp, err := s3Ctrl.S3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(copyObj),
		})
		if err != nil {
			log.Errorf("failed to download file: %s, error: %s", copyObj, err)
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
			log.Errorf("failed to write tar header for file: %s, error: %s", copyObj, err)
			return err
		}

		_, err = io.Copy(tarWriter, getResp.Body)
		if err != nil {
			log.Errorf("failed to write file content to tar for file: %s, error: %s", copyObj, err)
			return err
		}
		log.Debugf("completed copying: %s", copyObj)
	}

	err = tarWriter.Close()
	if err != nil {
		log.Error("tar close failure:", err)
		return err
	}

	err = gzipWriter.Close()
	if err != nil {
		log.Error("gzip close failure:", err)
		return err
	}

	err = pw.Close()
	if err != nil {
		log.Error("pipe writer close failure:", err)
		return err
	}

	wg.Wait()

	log.Debug("completed tar of file successfully")
	return nil
}

func (bh *BlobHandler) HandleGetPresignedDownloadURL(c echo.Context) error {
	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("bucket %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter `key` is required")
		log.Error("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	keyExist, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		log.Error("HandleGetPresignedURL: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
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
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	url, err := s3Ctrl.GetDownloadPresignedURL(bucket, key, expPeriod)
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

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("bucket %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	response, err := s3Ctrl.GetList(bucket, prefix, false)
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
	if size >= limit {
		err := fmt.Errorf("request entity is larger than %v GB, current file size is: %v GB, and current file count is: %d", float64(limit)/(1024*1024*1024), float64(size)/(1024*1024*1024), fileCount)
		log.Error("HandleGetPresignedURLMultiObj: ", err.Error())
		return c.JSON(http.StatusRequestEntityTooLarge, err.Error())
	}

	filename := fmt.Sprintf("%s.%s", strings.TrimSuffix(prefix, "/"), "tar.gz")
	outputFile := filepath.Join(os.Getenv("TEMP_PREFIX"), filename)
	expPeriod, err := strconv.Atoi(os.Getenv("URL_EXP_DAYS"))
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error getting `URL_EXP_DAYS` from env file:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	// Check if the tar.gz file already exists in S3
	tarFileResponse, err := s3Ctrl.GetList(bucket, outputFile, false)
	if err != nil {
		log.Error("Error checking if tar.gz file exists in S3:", err)
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	if len(tarFileResponse.Contents) > 0 {
		log.Debug("the prefix was once downloaded, checking if it is outdated")
		// Tar.gz file exists, now compare modification dates
		mostRecentModTime, err := s3Ctrl.getMostRecentModTime(bucket, prefix)
		if err != nil {
			log.Error("Error getting most recent modification time:", err)
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

		if tarFileResponse.Contents[0].LastModified.After(mostRecentModTime) {
			log.Debug("folder already downloaded and is current")

			// Existing tar.gz file is up-to-date, return pre-signed URL
			href, err := s3Ctrl.GetDownloadPresignedURL(bucket, outputFile, expPeriod)
			if err != nil {
				log.Error("Error getting presigned:", err)
				return c.JSON(http.StatusInternalServerError, err.Error())
			}
			return c.JSON(http.StatusOK, string(href))
		}
		log.Debug("folder already downloaded but is outdated starting the zip process")
	}

	err = s3Ctrl.tarS3Files(response, bucket, outputFile, prefix)
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error tarring S3 files:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	href, err := s3Ctrl.GetDownloadPresignedURL(bucket, outputFile, expPeriod)
	if err != nil {
		log.Error("HandleGetPresignedURLMultiObj: Error getting presigned URL:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetPresignedURLMultiObj: Successfully generated presigned URL for prefix:", prefix)
	return c.JSON(http.StatusOK, string(href))
}
