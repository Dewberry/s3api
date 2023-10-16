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

type S3Controller struct {
	Sess    *session.Session
	S3Svc   *s3.S3
	Buckets []string
}

// Store configuration for the handler
type BlobHandler struct {
	S3Controllers []S3Controller
}

// Initializes resources and return a new handler
// errors are fatal
func NewBlobHandler() (*BlobHandler, error) {
	var s3Controllers []S3Controller

	config := BlobHandler{}

	// Set up a session with AWS credentials and region
	s3SVC, sess, err := SessionManager()
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	S3Ctrl := S3Controller{Sess: sess, S3Svc: s3SVC}

	result, err := S3Ctrl.listBuckets()
	if err != nil {
		return nil, err
	}
	var bucketNames []string
	// Return the list of bucket names as a slice of strings
	for _, bucket := range result.Buckets {
		bucketNames = append(bucketNames, aws.StringValue(bucket.Name))
	}

	S3Ctrl.Buckets = bucketNames
	s3Controllers = append(s3Controllers, S3Ctrl)

	config.S3Controllers = s3Controllers

	return &config, nil
}

func (bh *BlobHandler) GetController(bucket string) (*S3Controller, error) {
	var s3Ctrl S3Controller
	for _, controller := range bh.S3Controllers {
		for _, b := range controller.Buckets {
			if b == bucket {
				s3Ctrl = controller
				return &s3Ctrl, nil
			}
		}
	}
	return &s3Ctrl, errors.New("bucket not fond")
}

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
				log.Debugf("Ping operation preformed succesfully, connection to `%s` is unhealthy", b)
			} else {
				valid = "healthy"
				log.Debugf("Ping operation preformed succesfully, connection to `%s` is healthy", b)
			}

			bucketHealth[b] = valid
			print(b, valid)
		}
	}

	return c.JSON(http.StatusOK, bucketHealth)
}

func SessionManager() (*s3.S3, *session.Session, error) {
	region := os.Getenv("AWS_REGION")

	//extracts the bool string from env, if not set it will default to false
	s3MockStr := os.Getenv("S3_MOCK")
	s3Mock := s3MockStr == "true"

	if s3Mock {
		minioAccessKey := os.Getenv("MINIO_ACCESS_KEY_ID")
		minioSecretAccessKey := os.Getenv("MINIO_SECRET_ACCESS_KEY")
		endpoint := os.Getenv("MINIO_S3_ENDPOINT")
		if endpoint == "" {
			return nil, nil, errors.New("`MINIO_S3_ENDPOINT` env var required if using Minio (S3_MOCK). Set `S3_MOCK` to false or add an `MINIO_S3_ENDPOINT` to the env")
		}

		sess, err := session.NewSession(&aws.Config{
			Endpoint:         aws.String(endpoint),
			Region:           aws.String(region),
			Credentials:      credentials.NewStaticCredentials(minioAccessKey, minioSecretAccessKey, ""),
			S3ForcePathStyle: aws.Bool(true),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error connecting to minio session: %s", err.Error())
		}
		log.Info("Using minio to mock s3")

		bucketName := os.Getenv("S3_BUCKET")

		// Check if the bucket exists
		s3SVC := s3.New(sess)
		_, err = s3SVC.HeadBucket(&s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			// Bucket does not exist, create it
			_, err := s3SVC.CreateBucket(&s3.CreateBucketInput{
				Bucket: aws.String(bucketName),
			})
			if err != nil {
				log.Errorf("Error creating bucket:", err)
				return nil, nil, nil
			}
			log.Info("Bucket created successfully")
		} else {
			log.Info("Bucket already exists")
		}

		return s3SVC, sess, nil
	} else {
		awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
		awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		log.Info("Using AWS S3")
		sess, err := session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error creating s3 session: %s", err.Error())
		}

		return s3.New(sess), sess, nil

	}
}
