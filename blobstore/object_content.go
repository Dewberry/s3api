package blobstore

import (
	"fmt"
	"io"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) FetchObjectContent(bucket string, key string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := s3Ctrl.S3Svc.GetObject(input)
	if err != nil {
		return nil, err
	}

	return output.Body, nil
}

func (bh *BlobHandler) HandleObjectContents(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, "parameter `key` is required", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
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

	outPutBody, err := s3Ctrl.FetchObjectContent(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error fetching object's content")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	body, err := io.ReadAll(outPutBody)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "error reading objects body", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	log.Info("Successfully fetched object data for key:", key)
	//TODO: add contentType
	return configberry.HandleSuccessfulResponse(c, body)
}
