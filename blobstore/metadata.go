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
		appErr := configberry.NewAppError(configberry.ValidationError, parameterPrefixRequired, nil)
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
	adjustedPrefix, appErr := s3Ctrl.checkAndAdjustPrefix(bucket, prefix)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	prefix = adjustedPrefix
	permissions, fullAccess, appErr := bh.getS3ReadPermissions(c, bucket)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if !fullAccess && !isPermittedPrefix(bucket, prefix, permissions) {
		appErr := configberry.NewAppError(configberry.ForbiddenError, fmt.Sprintf("user does not have permission to read the `prefix` %s", prefix), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	var totalSize uint64
	var fileCount uint64
	err = s3Ctrl.GetListWithCallBack(bucket, prefix, false, func(page *s3.ListObjectsV2Output) error {
		return GetListSize(page, &totalSize, &fileCount)
	})

	if err != nil {
		appErr := configberry.HandleAWSError(err, listingObjectsAndPrefixError)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	if fileCount == 0 {
		appErr := configberry.NewAppError(configberry.NotFoundError, fmt.Sprintf("`prefix` %s not found", prefix), err)
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

	log.Infof("Successfully retrieved size for `prefix` %s", prefix)
	return configberry.HandleSuccessfulResponse(c, response)
}

func (bh *BlobHandler) HandleGetMetaData(c echo.Context) error {
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

	permissions, fullAccess, appErr := bh.getS3ReadPermissions(c, bucket)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if !fullAccess && !isPermittedPrefix(bucket, key, permissions) {
		appErr := configberry.NewAppError(configberry.ForbiddenError, fmt.Sprintf("user does not have permission to read object with `key` %s ", key), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	result, err := s3Ctrl.GetMetaData(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error getting metadata for %s", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("successfully retrieved metadata for `key` %s exists", key)
	return configberry.HandleSuccessfulResponse(c, result)
}

func (bh *BlobHandler) HandleGetObjExist(c echo.Context) error {
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

	permissions, fullAccess, appErr := bh.getS3ReadPermissions(c, bucket)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	if !fullAccess && !isPermittedPrefix(bucket, key, permissions) {
		appErr := configberry.NewAppError(configberry.ForbiddenError, fmt.Sprintf("user does not have permission to read object with `key` %s", key), err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	result, err := s3Ctrl.KeyExists(bucket, key)
	if err != nil {
		appErr := configberry.HandleAWSError(err, fmt.Sprintf("error checking if object with `key` %s exists", key))
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Infof("successfully checked if object with `key` %s exists", key)
	return configberry.HandleSuccessfulResponse(c, result)
}
