package blobstore

import (
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (bh *BlobHandler) HandlePing(c echo.Context) error {
	return c.JSON(http.StatusOK, "connection without Auth is healthy")
}

func (bh *BlobHandler) HandlePingWithAuth(c echo.Context) error {
	// Perform a HeadBucket operation to check the health of the S3 connection
	initAuth := os.Getenv("INIT_AUTH")
	if initAuth == "0" {
		errMsg := fmt.Errorf("this requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality")
		return c.JSON(http.StatusForbidden, errMsg.Error())
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

	return c.JSON(http.StatusOK, bucketHealth)
}
