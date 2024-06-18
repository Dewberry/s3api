package blobstore

import (
	"fmt"
	"net/http"
	"os"

	"github.com/Dewberry/s3api/auth"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

func (bh *BlobHandler) Ping(c echo.Context) error {
	return c.JSON(http.StatusOK, "connection without Auth is healthy")
}

func (bh *BlobHandler) PingWithAuth(c echo.Context) error {
	// Perform a HeadBucket operation to check the health of the S3 connection
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

func (bh *BlobHandler) HandleCheckS3UserPermission(c echo.Context) error {
	if bh.Config.AuthLevel == 0 {
		log.Info("Checked user permissions successfully")
		return c.JSON(http.StatusOK, true)
	}
	initAuth := os.Getenv("INIT_AUTH")
	if initAuth == "0" {
		errMsg := fmt.Errorf("this endpoint requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusForbidden, errMsg.Error())
	}
	prefix := c.QueryParam("prefix")
	bucket := c.QueryParam("bucket")
	operation := c.QueryParam("operation")
	claims, ok := c.Get("claims").(*auth.Claims)
	if !ok {
		errMsg := fmt.Errorf("could not get claims from request context")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	userEmail := claims.Email
	if operation == "" || prefix == "" || bucket == "" {
		errMsg := fmt.Errorf("`prefix`,  `operation` and 'bucket are required params")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	isAllowed := bh.DB.CheckUserPermission(userEmail, bucket, prefix, []string{operation})
	log.Info("Checked user permissions successfully")
	return c.JSON(http.StatusOK, isAllowed)
}
