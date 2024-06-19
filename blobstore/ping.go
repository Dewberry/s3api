package blobstore

import (
	"os"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (bh *BlobHandler) HandlePing(c echo.Context) error {
	return configberry.HandleSuccessfulResponse(c, "connection without Auth is healthy")
}

func (bh *BlobHandler) HandlePingWithAuth(c echo.Context) error {
	// Perform a HeadBucket operation to check the health of the S3 connection
	initAuth := os.Getenv("INIT_AUTH")
	if initAuth == "0" {
		appErr := configberry.NewAppError(configberry.ForbiddenError, "this endpoint requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality", nil)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	bucketHealth := make(map[string]string)
	var valid string

	for _, s3Ctrl := range bh.S3Controllers {
		for _, b := range s3Ctrl.Buckets {
			_, err := s3Ctrl.S3Svc.HeadBucket(&s3.HeadBucketInput{
				Bucket: aws.String(b),
			})
			if err != nil {
				valid = "unhealthy"
			} else {
				valid = "healthy"
			}
			log.Debugf("Ping operation preformed succesfully, connection to `%s` is %s", b, valid)

			bucketHealth[b] = valid
			print(b, valid)
		}
	}

	return configberry.HandleSuccessfulResponse(c, bucketHealth)
}
