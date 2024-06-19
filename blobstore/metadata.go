package blobstore

import (
	"fmt"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (s3Ctrl *S3Controller) GetMetaData(bucket, key string) (*s3.HeadObjectOutput, error) {
	// Set up the input parameters for the list objects operation
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Ctrl.S3Svc.HeadObject(input)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s3Ctrl *S3Controller) KeyExists(bucket string, key string) (bool, error) {

	_, err := s3Ctrl.S3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
				return false, nil
			default:
				return false, err
			}
		}
		return false, err
	}
	return true, nil
}

// HandleGetSize retrieves the total size and the number of files in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleGetSize(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		appErr := configberry.NewAppError(configberry.ValidationError, "parameter `prefix` is required", nil)
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

	if !fullAccess && !isPermittedPrefix(bucket, prefix, permissions) {
		appErr := configberry.NewAppError(configberry.ForbiddenError, fmt.Sprintf("user does not have permission to read the %s prefix", prefix), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	// Check if the prefix points directly to an object
	isObject, err := s3Ctrl.KeyExists(bucket, prefix)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "error checking if prefix is an object", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if isObject {
		appErr := configberry.NewAppError(configberry.TeapotError, fmt.Sprintf("the provided prefix %s points to a single object rather than a collection", prefix), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	var totalSize uint64
	var fileCount uint64
	err = s3Ctrl.GetListWithCallBack(bucket, prefix, false, func(page *s3.ListObjectsV2Output) error {
		return GetListSize(page, &totalSize, &fileCount)
	})

	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "error processing objects", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	if fileCount == 0 {
		appErr := configberry.NewAppError(configberry.NotFoundError, fmt.Sprintf("prefix %s not found", prefix), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)

	}
	response := struct {
		Size      uint64 `json:"size"`
		FileCount uint64 `json:"file_count"`
		Prefix    string `json:"prefix"`
	}{
		Size:      totalSize,
		FileCount: fileCount,
		Prefix:    prefix,
	}

	log.Info("Successfully retrieved size for prefix:", prefix)
	return configberry.HandleSuccessfulResponse(c, response)
}

func (bh *BlobHandler) HandleGetMetaData(c echo.Context) error {
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

	result, err := s3Ctrl.GetMetaData(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error getting metadata")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Info("successfully retrieved metadata for key:", key)
	return configberry.HandleSuccessfulResponse(c, result)
}

func (bh *BlobHandler) HandleGetObjExist(c echo.Context) error {
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

	result, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error checking if object exists")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Info("successfully retrieved metadata for key:", key)
	return configberry.HandleSuccessfulResponse(c, result)
}
