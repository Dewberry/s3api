package blobstore

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"

	log "github.com/sirupsen/logrus"
)

type part struct {
	PartNumber int    `json:"partNumber"`
	ETag       string `json:"eTag"`
}
type completeUploadRequest struct {
	UploadID string `json:"uploadId"`
	Parts    []part `json:"parts"`
}

func (s3Ctrl *S3Controller) UploadS3Obj(bucket string, key string, body io.ReadCloser) error {
	// Initialize the multipart upload to S3
	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	resp, err := s3Ctrl.S3Svc.CreateMultipartUpload(params)
	if err != nil {
		return fmt.Errorf("error creating multipart upload for object with `key` %s, %w", key, err)
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
			return err
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
				return fmt.Errorf("error creating uploading part %v, %w", params, err)
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
		return fmt.Errorf("error creating uploading part %v, %w", params2, err)
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
		return fmt.Errorf("error completing multipart upload %w", err)
	}

	return nil
}

// function to retrieve presigned url for a normal one time upload. You can only upload 5GB files at a time.
func (s3Ctrl *S3Controller) GetUploadPresignedURL(bucket string, key string, expMin int) (string, error) {
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
		req, _ := s3.New(tempS3Svc).PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		urlStr, err = req.Presign(duration)
		if err != nil {
			return "", err
		}
	} else {
		// Generate the request using the original client
		req, _ := s3Ctrl.S3Svc.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		urlStr, err = req.Presign(duration)
		if err != nil {
			return "", err
		}
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
			Region:           s3Ctrl.Sess.Config.Region,
			Credentials:      s3Ctrl.Sess.Config.Credentials,
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

func (bh *BlobHandler) HandleMultipartUpload(c echo.Context) error {
	// Add overwrite check and parameter
	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, parameterKeyRequired, nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	appErr := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	overrideParam := c.QueryParam("override")
	var override bool
	if overrideParam == "true" || overrideParam == "false" {
		var err error
		override, err = strconv.ParseBool(c.QueryParam("override"))
		if err != nil {
			appErr := configberry.NewAppError(configberry.InternalServerError, "error parsing `override` parameter", err)
			log.Error(configberry.LogErrorFormatter(appErr, true))
			return configberry.HandleErrorResponse(c, appErr)
		}
	} else {
		appErr := configberry.NewAppError(configberry.ValidationError, "request must include a `override`, options are `true` or `false`", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	// Check if the request body is empty
	buf := make([]byte, 1)
	_, err = c.Request().Body.Read(buf)
	if err == io.EOF {
		appErr := configberry.NewAppError(configberry.ValidationError, "no file provided in the request body`", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	// Reset the request body to its original state
	c.Request().Body = io.NopCloser(io.MultiReader(bytes.NewReader(buf), c.Request().Body))

	keyExist, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error checking if object with `key` %s already exists`", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	if keyExist && !override {
		appErr := configberry.NewAppError(configberry.ConflictError, fmt.Sprintf("object with `key` %s already exists and `override` is set to %t", key, override), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	body := c.Request().Body
	defer body.Close()

	err = s3Ctrl.UploadS3Obj(bucket, key, body)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error uploading S3 object")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)

	}

	log.Infof("Successfully uploaded object with `key`: %s", key)
	return configberry.HandleSuccessfulResponse(c, "Successfully uploaded file")
}

// enpoint handler that will either return a one time presigned upload URL or multipart upload url
func (bh *BlobHandler) HandleGetPresignedUploadURL(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, parameterKeyRequired, nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	appErr := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	uploadID := c.QueryParam("upload_id")
	partNumberStr := c.QueryParam("part_number")

	if uploadID != "" && partNumberStr != "" {
		//if the user provided both upload_id and part_number then we return a part presigned URL
		partNumber, err := strconv.Atoi(partNumberStr)
		if err != nil {
			appErr := configberry.NewAppError(configberry.InternalServerError, "error parsing int from `part_number`", err)
			log.Error(configberry.LogErrorFormatter(appErr, true))
			return configberry.HandleErrorResponse(c, appErr)
		}
		presignedURL, err := s3Ctrl.GetUploadPartPresignedURL(bucket, key, uploadID, int64(partNumber), bh.Config.DefaultUploadPresignedUrlExpiration)
		if err != nil {
			appErr := configberry.HandleAWSError(err, fmt.Sprintf("error generating presigned part URL for object with `key` %s", key))
			log.Error(configberry.LogErrorFormatter(appErr, true))
			return configberry.HandleErrorResponse(c, appErr)
		}
		log.Infof("successfully generated presigned part URL for key: %s", key)
		return configberry.HandleSuccessfulResponse(c, presignedURL)
	} else if (uploadID == "" && partNumberStr != "") || (uploadID != "" && partNumberStr == "") {
		appErr := configberry.NewAppError(configberry.ValidationError, "both `uploadID` and `partNumber` must be provided together for a multipart upload, or neither for a standard upload", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	//if the user did not provided both upload_id and part_number then we returned normal presigned URL
	presignedURL, err := s3Ctrl.GetUploadPresignedURL(bucket, key, bh.Config.DefaultUploadPresignedUrlExpiration)
	if err != nil {
		appErr := configberry.HandleAWSError(err, (fmt.Sprintf("error generating presigned URL for object with `key` %s", key)))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("successfully generated presigned URL for object with `key`: %s", key)
	return configberry.HandleSuccessfulResponse(c, presignedURL)
}

// endpoint handler that will return a multipart upload ID
func (bh *BlobHandler) HandleGetMultipartUploadID(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, parameterKeyRequired, nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	appErr := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	uploadID, err := s3Ctrl.GetMultiPartUploadID(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error retrieving multipart Upload ID")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	log.Infof("successfully generated multipart Upload ID for key: %s", key)
	return configberry.HandleSuccessfulResponse(c, uploadID)
}

// endpoint handler that will complete a multipart upload
func (bh *BlobHandler) HandleCompleteMultipartUpload(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, parameterKeyRequired, nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	appErr := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	var req completeUploadRequest

	if err := c.Bind(&req); err != nil {
		appErr := configberry.NewAppError(configberry.BadRequestError, parseingBodyRequestError, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
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
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error completing the multipart Upload for object with `key` %s", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	log.Infof("succesfully completed multipart upload for key %s", key)
	return configberry.HandleSuccessfulResponse(c, "succesfully completed multipart upload")
}

// endpoint handler that will abort a multipart upload in progress
func (bh *BlobHandler) HandleAbortMultipartUpload(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, parameterKeyRequired, nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, unableToGetController, err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	appErr := bh.validateUserAccessToPrefix(c, bucket, key, []string{"write"})
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	uploadID := c.QueryParam("upload_id")
	if uploadID == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, "`upload_id` param is requires", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	err = s3Ctrl.AbortMultipartUpload(bucket, key, uploadID)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error aborting the multipart Upload for key %s", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	log.Infof("succesfully aborted multipart upload for object with `key` %s", key)
	return configberry.HandleSuccessfulResponse(c, "succesfully aborted multipart upload")
}
