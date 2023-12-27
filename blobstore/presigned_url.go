package blobstore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	size, _, err := bh.GetSize(response)
	if err != nil {
		errMsg := fmt.Errorf("error getting size: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	var downloadLimit int
	downloadLimit, err = strconv.Atoi(os.Getenv("ZIP_DOWNLOAD_SIZE_LIMIT"))
	if err != nil {
		log.Debugf("size download limit defaulted to %v", DEFAULT_ZIP_DOWNLOAD_SIZE_LIMIT)
		downloadLimit = DEFAULT_ZIP_DOWNLOAD_SIZE_LIMIT
	}
	limit := uint64(1024 * 1024 * 1024 * downloadLimit)
	if size >= limit {
		err := fmt.Errorf("request entity is larger than %v GB, current prefix size is: %v GB", float64(limit)/(1024*1024*1024), float64(size)/(1024*1024*1024))
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

func (bh *BlobHandler) HandleGenerateDownloadScript(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	bucket := c.QueryParam("bucket")
	if prefix == "" || bucket == "" {
		errMsg := fmt.Errorf("`prefix` and `bucket` query params are required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	prefix = strings.TrimSuffix(prefix, "/")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("error getting controller for bucket %s: %s", bucket, err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	//list objects within the prefix and test if empty
	response, err := s3Ctrl.GetList(bucket, prefix, false)
	if err != nil {
		errMsg := fmt.Errorf("error listing objects in bucket %s with prefix %s: %s", bucket, prefix, err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	if len(response.Contents) == 0 {
		errMsg := fmt.Errorf("prefix %s is empty or does not exist", prefix)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error())
	}
	size, _, err := bh.GetSize(response)
	if err != nil {
		errMsg := fmt.Errorf("error retrieving size of prefix: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	var downloadLimit int
	downloadLimit, err = strconv.Atoi(os.Getenv("SCRIPT_DOWNLOAD_SIZE_LIMIT"))
	if err != nil {
		log.Debugf("size download limit defaulted to %v", DEFAULT_SCRIPT_DOWNLOAD_SIZE_LIMIT)
		downloadLimit = DEFAULT_SCRIPT_DOWNLOAD_SIZE_LIMIT
	}
	limit := uint64(1024 * 1024 * 1024 * downloadLimit)
	if size > limit {
		errMsg := fmt.Errorf("request entity is larger than %v GB, current prefix size is: %v GB", float64(limit)/(1024*1024*1024), float64(size)/(1024*1024*1024))
		log.Error(errMsg.Error())
		return c.JSON(http.StatusRequestEntityTooLarge, errMsg.Error())
	}

	//expiration period from the env
	expPeriod, err := strconv.Atoi(os.Getenv("URL_EXP_DAYS"))
	if err != nil {
		errMsg := fmt.Errorf("error getting `URL_EXP_DAYS` from env file: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	var scriptBuilder strings.Builder
	createdDirs := make(map[string]bool)
	// Add download instructions at the beginning of the script
	scriptBuilder.WriteString("REM Download Instructions\n")
	scriptBuilder.WriteString("REM Thank you for downloading the script! To use it, follow these steps:\n\n")
	scriptBuilder.WriteString("REM 1. Locate the Downloaded File: Find the file you just downloaded. It should have a .txt file extension.\n")
	scriptBuilder.WriteString("REM 2. Script Location Adjustment: For flexibility in file uploads, relocate the script to the target directory where you want the files to be uploaded. This can be done by moving the script file to the desired directory in your file system.\n")
	scriptBuilder.WriteString("REM 3. Rename the File: Right-click on the file, select \"Rename,\" and change the file extension from \".txt\" to \".bat.\" For example, if the file is named \"script.txt,\" rename it to \"script.bat.\"\n")
	scriptBuilder.WriteString("REM 4. Initiate the Download: Double-click the renamed \".bat\" file to initiate the download process. Windows might display a warning message to protect your PC.\n")
	scriptBuilder.WriteString("REM 5. Windows Defender SmartScreen (Optional): If you see a message like \"Windows Defender SmartScreen prevented an unrecognized app from starting,\" click \"More info\" and then click \"Run anyway\" to proceed with the download.\n\n")
	//iterate over every object and check if it has any sub-prefixes to maintain a directory structure
	//lastPrefixSegment := filepath.Base(prefix)
	basePrefix := filepath.Base(prefix)

	for _, item := range response.Contents {
		// Remove the prefix up to the base, keeping the structure under the base prefix
		relativePath := strings.TrimPrefix(*item.Key, filepath.Dir(prefix)+"/")

		// Calculate the directory path for the relative path
		dirPath := filepath.Dir(relativePath)

		// Create directory if it does not exist and is not the root
		if _, exists := createdDirs[dirPath]; !exists && dirPath != "." && dirPath != basePrefix {
			scriptBuilder.WriteString(fmt.Sprintf("mkdir \"%s\"\n", dirPath))
			createdDirs[dirPath] = true
		}
		presignedURL, err := s3Ctrl.GetDownloadPresignedURL(bucket, *item.Key, expPeriod)
		if err != nil {
			errMsg := fmt.Errorf("error generating presigned URL for %s: %s", *item.Key, err)
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}
		url, err := url.QueryUnescape(presignedURL) //to remove url encoding which causes errors when executed in terminal
		if err != nil {
			errMsg := fmt.Errorf("error Unescaping url encoding: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}
		encodedURL := strings.ReplaceAll(url, " ", "%20")
		scriptBuilder.WriteString(fmt.Sprintf("if exist \"%s\" (echo skipping existing file) else (curl -v -o \"%s\" \"%s\")\n", relativePath, relativePath, encodedURL))
	}

	txtBatFileName := fmt.Sprintf("%s_download_script.txt", strings.TrimSuffix(prefix, "/"))
	outputFile := filepath.Join(os.Getenv("TEMP_PREFIX"), "download_scripts", txtBatFileName)

	//upload script to s3
	uploader := s3manager.NewUploader(s3Ctrl.Sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(outputFile),
		Body:        bytes.NewReader([]byte(scriptBuilder.String())),
		ContentType: aws.String("binary/octet-stream"),
	})
	if err != nil {
		errMsg := fmt.Errorf("error uploading %s to S3: %s", txtBatFileName, err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	href, err := s3Ctrl.GetDownloadPresignedURL(bucket, outputFile, 1)
	if err != nil {
		errMsg := fmt.Errorf("error generating presigned URL for %s: %s", txtBatFileName, err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Infof("Successfully generated download script for prefix %s in bucket %s", prefix, bucket)
	return c.JSON(http.StatusOK, href)
}
