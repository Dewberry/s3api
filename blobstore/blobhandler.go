package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
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
	s3SVC, sess, err := SessionManager()
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	config.S3Svc = s3SVC
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

func SessionManager() (*s3.S3, *session.Session, error) {
	region := os.Getenv("AWS_REGION")
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")

	//extracts the bool string from env, if not set it will default to false
	s3MockStr := os.Getenv("S3_MOCK")
	s3Mock := s3MockStr == "true"

	if s3Mock {
		minioSecretAccessKey := os.Getenv("MINIO_SECRET_ACCESS_KEY")
		fmt.Println("Using minio to mock s3")
		endpoint := os.Getenv("MINIO_S3_ENDPOINT")
		if endpoint == "" {
			return nil, nil, errors.New("`MINIO_S3_ENDPOINT` env var required if using Minio (S3_MOCK). Set `S3_MOCK` to false or add an `MINIO_S3_ENDPOINT` to the env")
		}

		sess, err := session.NewSession(&aws.Config{
			Endpoint:         aws.String(endpoint),
			Region:           aws.String(region),
			Credentials:      credentials.NewStaticCredentials(accessKeyID, minioSecretAccessKey, ""),
			S3ForcePathStyle: aws.Bool(true),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error connecting to minio session: %s", err.Error())
		}

		return s3.New(sess), sess, nil

	} else {
		awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		fmt.Println("Using AWS S3")
		sess, err := session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(accessKeyID, awsSecretAccessKey, ""),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error creating s3 session: %s", err.Error())
		}

		return s3.New(sess), sess, nil

	}
}
