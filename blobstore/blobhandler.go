package blobstore

import (
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
)

// Store configuration for the handler
type BlobHandler struct {
	Sess  *session.Session
	S3Svc *s3.S3
}

// Initializes resources and return a new handler
// errors are fatal
func NewBlobHandler() *BlobHandler {
	// working with pointers here so as not to copy large templates, yamls, and ActiveJobs
	config := BlobHandler{}

	// Set up a session with AWS credentials and region
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	config.S3Svc = s3.New(sess)
	config.Sess = sess
	return &config
}

func (bh *BlobHandler) Ping(c echo.Context) error {
	// Try to list buckets to test the connection
	_, err := bh.S3Svc.ListBuckets(nil)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, fmt.Sprintf("Error connecting to S3, %s", err.Error()))
	}

	return c.JSON(http.StatusOK, "connection is healthy")
}
