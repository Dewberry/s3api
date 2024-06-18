package blobstore

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"

	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) UploadS3Obj(bucket string, key string, body io.ReadCloser) error {
	// Initialize the multipart upload to S3
	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	resp, err := s3Ctrl.S3Svc.CreateMultipartUpload(params)
	if err != nil {
		return fmt.Errorf("error initializing multipart upload. %s", err.Error())
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
			return fmt.Errorf("error copying POST body to S3. %s", err.Error())
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

			result, err := s3Ctrl.S3Svc.UploadPart(params)
			if err != nil {
				return fmt.Errorf("error streaming POST body to S3. %s, %+v", err.Error(), result)
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

	result, err := s3Ctrl.S3Svc.UploadPart(params2)
	if err != nil {
		return fmt.Errorf("error streaming POST body to S3. %s, %+v", err.Error(), result)
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
	_, err = s3Ctrl.S3Svc.CompleteMultipartUpload(completeParams)
	if err != nil {
		return fmt.Errorf("error completing multipart upload. %s", err.Error())
	}

	return nil
}

func (bh *BlobHandler) HandleMultipartUpload(c echo.Context) error {
	// Add overwrite check and parameter
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("parameter 'key' is required")
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

	httpCode, err := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if err != nil {
		errMsg := fmt.Errorf("error while checking for user permission: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(httpCode, errMsg.Error())
	}

	overrideParam := c.QueryParam("override")

	var override bool

	if overrideParam == "true" || overrideParam == "false" {
		var err error
		override, err = strconv.ParseBool(c.QueryParam("override"))
		if err != nil {
			errMsg := fmt.Errorf("error parsing 'override' parameter: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
		}

	} else {
		errMsg := fmt.Errorf("request must include a `override`, options are `true` or `false`")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	// Check if the request body is empty
	buf := make([]byte, 1)
	_, err = c.Request().Body.Read(buf)
	if err == io.EOF {
		errMsg := fmt.Errorf("no file provided in the request body")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error()) // Return 400 Bad Request
	}

	// Reset the request body to its original state
	c.Request().Body = io.NopCloser(io.MultiReader(bytes.NewReader(buf), c.Request().Body))

	keyExist, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		errMsg := fmt.Errorf("error checking if object exists: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	if keyExist && !override {
		errMsg := fmt.Errorf("object %s already exists and override is set to %t", key, override)
		log.Errorf(errMsg.Error())
		return c.JSON(http.StatusConflict, errMsg.Error())
	}

	body := c.Request().Body
	defer body.Close()

	err = s3Ctrl.UploadS3Obj(bucket, key, body)
	if err != nil {
		errMsg := fmt.Errorf("error uploading S3 object: %s", err.Error())
		log.Errorf(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Infof("Successfully uploaded file with key: %s", key)
	return c.JSON(http.StatusOK, "Successfully uploaded file")
}

// function to retrieve presigned url for a normal one time upload. You can only upload 5GB files at a time.
func (s3Ctrl *S3Controller) GetUploadPresignedURL(bucket string, key string, expMin int) (string, error) {
	duration := time.Duration(expMin) * time.Minute
	req, _ := s3Ctrl.S3Svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	urlStr, err := req.Presign(duration)
	if err != nil {
		return "", err
	}

	return urlStr, nil
}

// function to retrieve presigned url for a multipart upload part.
func (s3Ctrl *S3Controller) GetUploadPartPresignedURL(bucket string, key string, uploadID string, partNumber int64, expMin int) (string, error) {
	duration := time.Duration(expMin) * time.Minute
	var urlStr string
	var err error
	if s3Ctrl.S3Mock {
		// Create a temporary S3 client with the modified endpoint
		//this is done  so that the presigned url starts with localhost:9000 instead of
		//minio:9000 which would cause an error due to cors origin policy
		tempS3Svc, err := session.NewSession(&aws.Config{
			Endpoint:         aws.String("http://localhost:9000"),
			Region:           s3Ctrl.S3Svc.Config.Region,
			Credentials:      s3Ctrl.S3Svc.Config.Credentials,
			S3ForcePathStyle: aws.Bool(true),
		})
		if err != nil {
			return "", fmt.Errorf("error creating temporary s3 session: %s", err.Error())
		}

		// Generate the request using the temporary client
		req, _ := s3.New(tempS3Svc).UploadPartRequest(&s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int64(partNumber),
		})
		urlStr, err = req.Presign(duration)
		if err != nil {
			return "", err
		}
	} else {
		// Generate the request using the original client
		req, _ := s3Ctrl.S3Svc.UploadPartRequest(&s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int64(partNumber),
		})
		urlStr, err = req.Presign(duration)
		if err != nil {
			return "", err
		}
	}

	return urlStr, nil
}

// enpoint handler that will either return a one time presigned upload URL or multipart upload url
func (bh *BlobHandler) HandleGetPresignedUploadURL(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("`key` parameters are required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	bucket := c.QueryParam("bucket")
	//get controller for bucket
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	httpCode, err := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if err != nil {
		errMsg := fmt.Errorf("error while checking for user permission: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(httpCode, errMsg.Error())
	}
	uploadID := c.QueryParam("upload_id")
	partNumberStr := c.QueryParam("part_number")

	if uploadID != "" && partNumberStr != "" {
		//if the user provided both upload_id and part_number then we return a part presigned URL
		partNumber, err := strconv.Atoi(partNumberStr)
		if err != nil {
			errMsg := fmt.Errorf("error parsing int from `part_number`: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}
		presignedURL, err := s3Ctrl.GetUploadPartPresignedURL(bucket, key, uploadID, int64(partNumber), bh.Config.DefaultUploadPresignedUrlExpiration)
		if err != nil {
			errMsg := fmt.Errorf("error generating presigned part URL: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}
		log.Infof("successfully generated presigned part URL for key: %s", key)
		return c.JSON(http.StatusOK, presignedURL)
	} else if (uploadID == "" && partNumberStr != "") || (uploadID != "" && partNumberStr == "") {
		errMsg := fmt.Errorf("both 'uploadID' and 'partNumber' must be provided together for a multipart upload, or neither for a standard upload")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	//if the user did not provided both upload_id and part_number then we returned normal presigned URL
	presignedURL, err := s3Ctrl.GetUploadPresignedURL(bucket, key, bh.Config.DefaultUploadPresignedUrlExpiration)
	if err != nil {
		log.Errorf("error generating presigned URL: %s", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Infof("successfully generated presigned URL for key: %s", key)
	return c.JSON(http.StatusOK, presignedURL)
}

// function that will return a multipart upload ID
func (s3Ctrl *S3Controller) GetMultiPartUploadID(bucket string, key string) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	result, err := s3Ctrl.S3Svc.CreateMultipartUpload(input)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

// endpoint handler that will return a multipart upload ID
func (bh *BlobHandler) HandleGetMultipartUploadID(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("`key` parameters are required")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	bucket := c.QueryParam("bucket")
	//get controller for bucket
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	httpCode, err := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if err != nil {
		errMsg := fmt.Errorf("error while checking for user permission: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(httpCode, errMsg.Error())
	}

	uploadID, err := s3Ctrl.GetMultiPartUploadID(bucket, key)
	if err != nil {
		errMsg := fmt.Errorf("error retrieving multipart Upload ID: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	log.Infof("successfully generated multipart Upload ID for key: %s", key)
	return c.JSON(http.StatusOK, uploadID)
}

// function that will complete a multipart upload ID when all parts are completely uploaded
func (s3Ctrl *S3Controller) CompleteMultipartUpload(bucket string, key string, uploadID string, parts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	}
	result, err := s3Ctrl.S3Svc.CompleteMultipartUpload(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// endpoint handler that will complete a multipart upload
func (bh *BlobHandler) HandleCompleteMultipartUpload(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("`key` parameters are required")
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
	httpCode, err := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if err != nil {
		errMsg := fmt.Errorf("error while checking for user permission: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(httpCode, errMsg.Error())
	}
	type part struct {
		PartNumber int    `json:"partNumber"`
		ETag       string `json:"eTag"`
	}
	type completeUploadRequest struct {
		UploadID string `json:"uploadId"`
		Parts    []part `json:"parts"`
	}
	var req completeUploadRequest
	if err := c.Bind(&req); err != nil {
		errMsg := fmt.Errorf("error parsing request body: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error())
	}

	s3Parts := make([]*s3.CompletedPart, len(req.Parts))
	for i, part := range req.Parts {
		s3Parts[i] = &s3.CompletedPart{
			PartNumber: aws.Int64(int64(part.PartNumber)),
			ETag:       aws.String(part.ETag),
		}
	}

	_, err = s3Ctrl.CompleteMultipartUpload(bucket, key, req.UploadID, s3Parts)
	if err != nil {
		errMsg := fmt.Errorf("error completing the multipart Upload for key %s, %s", key, err)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	log.Infof("succesfully completed multipart upload for key %s", key)
	return c.JSON(http.StatusOK, "succesfully completed multipart upload")
}

// function that will abort a multipart upload in progress
func (s3Ctrl *S3Controller) AbortMultipartUpload(bucket string, key string, uploadID string) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}
	_, err := s3Ctrl.S3Svc.AbortMultipartUpload(input)
	if err != nil {
		return err
	}
	return nil
}

// endpoint handler that will abort a multipart upload in progress
func (bh *BlobHandler) HandleAbortMultipartUpload(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		errMsg := fmt.Errorf("`key` parameter is required")
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
	httpCode, err := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if err != nil {
		errMsg := fmt.Errorf("error while checking for user permission: %s", err)
		log.Error(errMsg.Error())
		return c.JSON(httpCode, errMsg.Error())
	}

	uploadID := c.QueryParam("upload_id")
	if uploadID == "" {
		errMsg := fmt.Errorf("`upload_id` param is requires")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	err = s3Ctrl.AbortMultipartUpload(bucket, key, uploadID)
	if err != nil {
		errMsg := fmt.Errorf("error aborting the multipart Upload for key %s, %s", key, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	log.Infof("succesfully aborted multipart upload for key %s", key)
	return c.JSON(http.StatusOK, "succesfully aborted multipart upload")
}
