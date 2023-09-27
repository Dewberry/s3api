package blobstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) UploadS3Obj(bucket string, key string, body io.ReadCloser) error {
	// Initialize the multipart upload to S3
	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	resp, err := bh.S3Svc.CreateMultipartUpload(params)
	if err != nil {
		return fmt.Errorf("uploadS3Obj: error initializing multipart upload. %s", err.Error())
	}

	// Create the variables that will track upload progress
	var totalBytes int64 = 0
	var partNumber int64 = 1
	completedParts := []*s3.CompletedPart{}
	buffer := bytes.NewBuffer(nil)

	for {
		// Read from the request body into the buffer
		chunkSize := 1024 * 1024 * 5
		buf := make([]byte, chunkSize)
		n, err := body.Read(buf)

		// This would be a true error while reading
		if err != nil && err != io.EOF {
			return fmt.Errorf("uploadS3Obj: error copying POST body to S3. %s", err.Error())
		}

		// Add the buffer data to the buffer
		buffer.Write(buf[:n])

		// Upload a part if the buffer contains more than 5mb of data to avoid AWS EntityTooSmall error
		if buffer.Len() > chunkSize {
			params := &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   resp.UploadId,
				PartNumber: aws.Int64(partNumber),
				Body:       bytes.NewReader(buffer.Bytes()),
			}

			result, err := bh.S3Svc.UploadPart(params)
			if err != nil {
				return fmt.Errorf("uploadS3Obj: error streaming POST body to S3. %s, %+v", err.Error(), result)
			}

			totalBytes += int64(buffer.Len())
			completedParts = append(completedParts, &s3.CompletedPart{
				ETag:       result.ETag,
				PartNumber: aws.Int64(partNumber),
			})

			buffer.Reset()
			partNumber++
		}

		if err == io.EOF {
			break
		}
	}

	// Upload the remaining data as the last part
	params2 := &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   resp.UploadId,
		PartNumber: aws.Int64(partNumber),
		Body:       bytes.NewReader(buffer.Bytes()),
	}

	result, err := bh.S3Svc.UploadPart(params2)
	if err != nil {
		return fmt.Errorf("uploadS3Obj: error streaming POST body to S3. %s, %+v", err.Error(), result)
	}

	totalBytes += int64(buffer.Len())
	completedParts = append(completedParts, &s3.CompletedPart{
		ETag:       result.ETag,
		PartNumber: aws.Int64(partNumber),
	})

	// Complete the multipart upload
	completeParams := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
	}
	_, err = bh.S3Svc.CompleteMultipartUpload(completeParams)
	if err != nil {
		return fmt.Errorf("uploadS3Obj: error completing multipart upload. %s", err.Error())
	}

	return nil
}

func (bh *BlobHandler) HandleMultipartUpload(c echo.Context) error {
	// Add overwrite check and parameter
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Error("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	overrideParam := c.QueryParam("override")

	var override bool

	if overrideParam == "true" || overrideParam == "false" {
		var err error
		override, err = strconv.ParseBool(c.QueryParam("override"))
		if err != nil {
			log.Errorf("HandleMultipartUpload: Error parsing 'override' parameter: %s", err.Error())
			return c.JSON(http.StatusUnprocessableEntity, err.Error())
		}

	} else {
		err := errors.New("request must include a `override`, options are `true` or `false`")
		log.Errorf("HandleMultipartUpload: %s", err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	keyExist, err := bh.KeyExists(bucket, key)
	if err != nil {
		log.Errorf("HandleMultipartUpload: Error checking if key exists: %s", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}
	if keyExist && !override {
		err := fmt.Errorf("object %s already exists and override is set to %t", key, override)
		log.Errorf("HandleMultipartUpload: %s" + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	body := c.Request().Body
	defer body.Close()

	err = bh.UploadS3Obj(bucket, key, body)
	if err != nil {
		log.Errorf("HandleMultipartUpload: Error uploading S3 object: %s", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Infof("HandleMultipartUpload: Successfully uploaded file with key: %s", key)
	return c.JSON(http.StatusOK, "Successfully uploaded file")
}
