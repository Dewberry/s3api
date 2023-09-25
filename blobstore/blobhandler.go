package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

// Store configuration for the handler
type BlobHandler struct {
	Sess   *session.Session
	S3Svc  *s3.S3
	Bucket string
}

// Initializes resources and return a new handler
// errors are fatal
func NewBlobHandler() (*BlobHandler, error) {
	config := BlobHandler{}

	// Set up a session with AWS credentials and region
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	config.S3Svc = s3.New(sess)
	config.Sess = sess

	if os.Getenv("S3_BUCKET") == "" {
		return nil, errors.New("S3_BUCKET environment variable is not set")
	}
	config.Bucket = os.Getenv("S3_BUCKET")

	return &config, nil
}

func (bh *BlobHandler) Ping(c echo.Context) error {
	return c.JSON(http.StatusOK, "connection without Auth is healthy")
}
func (bh *BlobHandler) PingWithAuth(c echo.Context) error {
	// Perform a HeadBucket operation to check the health of the S3 connection
	_, err := bh.S3Svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bh.Bucket),
	})
	if err != nil {
		log.Errorf("Error connecting to S3 Bucket `%s`: %s ", bh.Bucket, err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	log.Infof("Ping operation preformed succesfully, connection to `%s` is healthy", bh.Bucket)
	return c.JSON(http.StatusOK, "connection is healthy to s3 bucket")
}
