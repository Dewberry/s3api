package blobstore

import (
	"bytes"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) GetDownloadPresignedURL(bucket, key string, expDays int) (string, error) {
	duration := time.Duration(expDays) * 24 * time.Hour
	if _, err := s3Ctrl.GetMetaData(bucket, key); err != nil { //this is to check if the object exists or not, it will return an AWS error
		return "", fmt.Errorf("error checking if object %s exists, %w", key, err)
	}
	req, _ := s3Ctrl.S3Svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(duration)
}

func (bh *BlobHandler) HandleGetPresignedDownloadURL(c echo.Context) error {
	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, "parameter `key` is required", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	permissions, fullAccess, appErr := bh.getS3ReadPermissions(c, bucket)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if !fullAccess && !isPermittedPrefix(bucket, key, permissions) {
		appErr := configberry.NewAppError(configberry.ForbiddenError, fmt.Sprintf("user does not have permission to read the %s key", key), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	url, err := s3Ctrl.GetDownloadPresignedURL(bucket, key, bh.Config.DefaultDownloadPresignedUrlExpiration)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error getting presigned download URL for object %s", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Info("successfully generated presigned URL for key:", key)
	return configberry.HandleSuccessfulResponse(c, url)
}

func (bh *BlobHandler) HandleGenerateDownloadScript(c echo.Context) error {
	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	prefix := c.QueryParam("prefix")
	if prefix == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, "parameter `prefix` is required", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	adjustedPrefix, appErr := s3Ctrl.checkAndAdjustPrefix(bucket, prefix)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	prefix = adjustedPrefix

	var totalSize uint64
	var scriptBuilder strings.Builder
	createdDirs := make(map[string]bool)
	basePrefix := filepath.Base(strings.TrimSuffix(prefix, "/"))
	scriptBuilder.WriteString("REM Download Instructions\n")
	scriptBuilder.WriteString("REM To download the selected directory or file, please follow these steps:\n\n")
	scriptBuilder.WriteString("REM 1. Locate the Downloaded File: Find the file you just downloaded. It should have a .txt file extension.\n")
	scriptBuilder.WriteString("REM 2. Script Location Adjustment: For flexibility in file downloads, relocate the script to the target directory where you want the files to be downloaded. This can be done by moving the script file to the desired directory in your file system.\n")
	scriptBuilder.WriteString("REM 3. Rename the File: Right-click on the file, select \"Rename,\" and change the file extension from \".txt\" to \".bat.\" For example, if the file is named \"script.txt,\" rename it to \"script.bat.\"\n")
	scriptBuilder.WriteString("REM 4. Initiate the Download: Double-click the renamed \".bat\" file to initiate the download process. Windows might display a warning message to protect your PC.\n")
	scriptBuilder.WriteString("REM 5. Windows Defender SmartScreen (Optional): If you see a message like \"Windows Defender SmartScreen prevented an unrecognized app from starting,\" click \"More info\" and then click \"Run anyway\" to proceed with the download.\n\n")
	scriptBuilder.WriteString(fmt.Sprintf("mkdir \"%s\"\n", basePrefix))

	permissions, fullAccess, appErr := bh.getS3ReadPermissions(c, bucket)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	// Define the processPage function
	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, item := range page.Contents {
			if fullAccess || isPermittedPrefix(bucket, *item.Key, permissions) {

				// Size checking
				if item.Size != nil {
					totalSize += uint64(*item.Size)
					if totalSize > uint64(bh.Config.DefaultScriptDownloadSizeLimit*1024*1024*1024) {
						return awserr.New("EntityTooLarge", "Script size exceeds limit", fmt.Errorf("size limit of %d GB exceeded", bh.Config.DefaultScriptDownloadSizeLimit))
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
				if err != nil { //ServiceInternalError
					return awserr.New("ServiceInternalError", "Script size exceeds limit", fmt.Errorf("error unescaping URL encoding: %v", err))
				}
				encodedURL := strings.ReplaceAll(url, " ", "%20")
				scriptBuilder.WriteString(fmt.Sprintf("if exist \"%s\" (echo skipping existing file) else (curl -v -o \"%s\" \"%s\")\n", fullPath, fullPath, encodedURL))
			}
		}
		return nil
	}

	// Call GetList with the processPage function
	err = s3Ctrl.GetListWithCallBack(bucket, prefix, false, processPage)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error processing objects")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
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
		appErr := configberry.NewAppError(configberry.InternalServerError, fmt.Sprintf("error uploading %s to S3", txtBatFileName), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	href, err := s3Ctrl.GetDownloadPresignedURL(bucket, outputFile, 1)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error generating presigned URL for %s", txtBatFileName))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("successfully generated download script for prefix %s in bucket %s", prefix, bucket)
	return configberry.HandleSuccessfulResponse(c, href)
}
