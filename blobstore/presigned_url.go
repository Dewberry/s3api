package blobstore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
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
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("parameter `key` is required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	keyExist, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		errMsg := fmt.Errorf("checking if key exists: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	if !keyExist {
		errMsg := fmt.Errorf("object %s not found", key)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusNotFound, errMsg.Error())
	}
	// Set the expiration time for the pre-signed URL

	url, err := s3Ctrl.GetDownloadPresignedURL(bucket, key, bh.Config.DefaultDownloadPresignedUrlExpiration)
	if err != nil {
		errMsg := fmt.Errorf("error getting presigned URL: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Info("successfully generated presigned URL for key:", key)
	return c.JSON(http.StatusOK, url)
}

func (bh *BlobHandler) HandleGetPresignedURLMultiObj(c echo.Context) error {
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

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	response, err := s3Ctrl.GetList(bucket, prefix, false)
	if err != nil {
		errMsg := fmt.Errorf("error getting list: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	if *response.KeyCount == 0 {
		errMsg := fmt.Errorf("the specified prefix %s does not exist in S3", prefix)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusNotFound, errMsg.Error())
	}
	//check if size is below 5GB
	var size, fileCount uint64
	err = bh.GetSize(response, &size, &fileCount)
	if err != nil {
		errMsg := fmt.Errorf("error getting size: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	limit := uint64(1024 * 1024 * 1024 * bh.Config.DefaultZipDownloadSizeLimit)
	if size >= limit {
		errMsg := fmt.Errorf("request entity is larger than %v GB, current prefix size is: %v GB", bh.Config.DefaultZipDownloadSizeLimit, float64(size)/(1024*1024*1024))
		log.Error(errMsg.Error())
		return c.JSON(http.StatusRequestEntityTooLarge, errMsg.Error())
	}

	filename := fmt.Sprintf("%s.%s", strings.TrimSuffix(prefix, "/"), "tar.gz")
	outputFile := filepath.Join(bh.Config.DefaultTempPrefix, filename)

	// Check if the tar.gz file already exists in S3
	tarFileResponse, err := s3Ctrl.GetList(bucket, outputFile, false)
	if err != nil {
		errMsg := fmt.Errorf("error checking if tar.gz file exists in S3: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	if len(tarFileResponse.Contents) > 0 {
		log.Debug("the prefix was once downloaded, checking if it is outdated")
		// Tar.gz file exists, now compare modification dates
		mostRecentModTime, err := s3Ctrl.getMostRecentModTime(bucket, prefix)
		if err != nil {
			errMsg := fmt.Errorf("error getting most recent modification time: %s", err)
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}

		if tarFileResponse.Contents[0].LastModified.After(mostRecentModTime) {
			log.Debug("folder already downloaded and is current")

			// Existing tar.gz file is up-to-date, return pre-signed URL
			href, err := s3Ctrl.GetDownloadPresignedURL(bucket, outputFile, bh.Config.DefaultDownloadPresignedUrlExpiration)
			if err != nil {
				errMsg := fmt.Errorf("error getting presigned: %s", err)
				log.Error(errMsg.Error())
				return c.JSON(http.StatusInternalServerError, errMsg.Error())
			}
			return c.JSON(http.StatusOK, string(href))
		}
		log.Debug("folder already downloaded but is outdated starting the zip process")
	}

	err = s3Ctrl.tarS3Files(response, bucket, outputFile, prefix)
	if err != nil {
		errMsg := fmt.Errorf("error tarring S3 files: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	href, err := s3Ctrl.GetDownloadPresignedURL(bucket, outputFile, bh.Config.DefaultDownloadPresignedUrlExpiration)
	if err != nil {
		errMsg := fmt.Errorf("error getting presigned URL: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Info("successfully generated presigned URL for prefix:", prefix)
	return c.JSON(http.StatusOK, string(href))
}

func (bh *BlobHandler) HandleGenerateDownloadScript(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		errMsg := fmt.Errorf("`prefix` and `bucket` query params are required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("error getting controller for bucket %s: %s", bucket, err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	var totalSize uint64
	var scriptBuilder strings.Builder
	createdDirs := make(map[string]bool)
	basePrefix := filepath.Base(strings.TrimSuffix(prefix, "/"))
	scriptBuilder.WriteString(fmt.Sprintf("mkdir \"%s\"\n", basePrefix))
	scriptBuilder.WriteString("REM Download Instructions\n")
	scriptBuilder.WriteString("REM To download the selected directory or file, please follow these steps:\n\n")
	scriptBuilder.WriteString("REM 1. Locate the Downloaded File: Find the file you just downloaded. It should have a .txt file extension.\n")
	scriptBuilder.WriteString("REM 2. Script Location Adjustment: For flexibility in file downloads, relocate the script to the target directory where you want the files to be downloaded. This can be done by moving the script file to the desired directory in your file system.\n")
	scriptBuilder.WriteString("REM 3. Rename the File: Right-click on the file, select \"Rename,\" and change the file extension from \".txt\" to \".bat.\" For example, if the file is named \"script.txt,\" rename it to \"script.bat.\"\n")
	scriptBuilder.WriteString("REM 4. Initiate the Download: Double-click the renamed \".bat\" file to initiate the download process. Windows might display a warning message to protect your PC.\n")
	scriptBuilder.WriteString("REM 5. Windows Defender SmartScreen (Optional): If you see a message like \"Windows Defender SmartScreen prevented an unrecognized app from starting,\" click \"More info\" and then click \"Run anyway\" to proceed with the download.\n\n")
	scriptBuilder.WriteString(fmt.Sprintf("mkdir \"%s\"\n", basePrefix))

	// Define the processPage function
	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, item := range page.Contents {
			// Size checking
			if item.Size != nil {
				totalSize += uint64(*item.Size)
				if totalSize > uint64(bh.Config.DefaultScriptDownloadSizeLimit*1024*1024*1024) {
					return fmt.Errorf("size limit of %d GB exceeded", bh.Config.DefaultScriptDownloadSizeLimit)
				}

			}

			// Script generation logic (replicating your directory creation and URL logic)
			relativePath := strings.TrimPrefix(*item.Key, filepath.Dir(prefix)+"/")
			dirPath := filepath.Join(basePrefix, filepath.Dir(relativePath))
			if _, exists := createdDirs[dirPath]; !exists && dirPath != basePrefix {
				scriptBuilder.WriteString(fmt.Sprintf("mkdir \"%s\"\n", dirPath))
				createdDirs[dirPath] = true
			}

			fullPath := filepath.Join(basePrefix, relativePath)
			presignedURL, err := s3Ctrl.GetDownloadPresignedURL(bucket, *item.Key, bh.Config.DefaultDownloadPresignedUrlExpiration)
			if err != nil {
				return fmt.Errorf("error generating presigned URL for object %s: %v", *item.Key, err)
			}
			url, err := url.QueryUnescape(presignedURL)
			if err != nil {
				return fmt.Errorf("error unescaping URL encoding: %v", err)
			}
			encodedURL := strings.ReplaceAll(url, " ", "%20")
			scriptBuilder.WriteString(fmt.Sprintf("if exist \"%s\" (echo skipping existing file) else (curl -v -o \"%s\" \"%s\")\n", fullPath, fullPath, encodedURL))
		}
		return nil
	}

	// Call GetList with the processPage function
	err = s3Ctrl.GetListWithCallBack(bucket, prefix, false, processPage)
	if err != nil {
		errMsg := fmt.Errorf("error processing objects: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	txtBatFileName := fmt.Sprintf("%s_download_script.txt", strings.TrimSuffix(prefix, "/"))
	outputFile := filepath.Join(bh.Config.DefaultTempPrefix, "download_scripts", txtBatFileName)

	//upload script to s3
	uploader := s3manager.NewUploader(s3Ctrl.Sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(outputFile),
		Body:        bytes.NewReader([]byte(scriptBuilder.String())),
		ContentType: aws.String("binary/octet-stream"),
	})
	if err != nil {
		errMsg := fmt.Errorf("error uploading %s to S3: %s", txtBatFileName, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	href, err := s3Ctrl.GetDownloadPresignedURL(bucket, outputFile, 1)
	if err != nil {
		errMsg := fmt.Errorf("error generating presigned URL for %s: %s", txtBatFileName, err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Infof("successfully generated download script for prefix %s in bucket %s", prefix, bucket)
	return c.JSON(http.StatusOK, href)
}
