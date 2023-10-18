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
	S3Controllers   []S3Controller
	NamedBucketOnly bool
}

// Initializes resources and return a new handler (errors are fatal)
func NewBlobHandler(envJson string) (*BlobHandler, error) {

	config := BlobHandler{}
	if os.Getenv("S3_MOCK") == "true" {
		log.Info("Using MinIO")

		creds := NewMinioConfig()
		s3SVC, sess, err := minIOSessionManager(creds)
		if err != nil {
			return nil, fmt.Errorf("failed to create MinIO session: %v", err)
		}
		buckets := []string{os.Getenv("S3_BUCKET")}
		s3Ctrl := S3Controller{Sess: sess, S3Svc: s3SVC, Buckets: buckets}
		config.S3Controllers = []S3Controller{s3Ctrl}
		config.NamedBucketOnly = true
		return &config, nil
	}

	log.Info("Looking for env.json credentials")

	awsConfig, err := NewAWSConfig(envJson)

	if err != nil {
		log.Warn("No env.json credentials found, attmepting to retreive from environment")
		creds := AWSFromENV()
		s3SVC, sess, err := aWSSessionManager(creds)
		if err != nil {
			return nil, fmt.Errorf("failed to create AWS session: %v", err)
		}

		bucket := os.Getenv("S3_BUCKET")
		buckets := []string{bucket}
		s3Ctrl := S3Controller{Sess: sess, S3Svc: s3SVC, Buckets: buckets}
		config.S3Controllers = []S3Controller{s3Ctrl}
		config.NamedBucketOnly = true
		return &config, nil
	}

	var s3Controllers []S3Controller
	for _, creds := range awsConfig.Accounts {
		var bucketNames []string
		// Set up a session with AWS credentials and region
		s3SVC, sess, err := aWSSessionManager(creds)
		if err != nil {
			return nil, fmt.Errorf("failed to create AWS session: %v", err)
		}
		S3Ctrl := S3Controller{Sess: sess, S3Svc: s3SVC}

		result, err := S3Ctrl.listBuckets()
		if err != nil {
			return nil, err
		}
		// Return the list of bucket names as a slice of strings
		for _, bucket := range result.Buckets {
			bucketNames = append(bucketNames, aws.StringValue(bucket.Name))
		}

		S3Ctrl.Buckets = bucketNames
		s3Controllers = append(s3Controllers, S3Ctrl)
	}

	config.S3Controllers = s3Controllers
	config.NamedBucketOnly = false

	return &config, nil
}

func (bh *BlobHandler) getController(bucket string) (*S3Controller, error) {
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

func aWSSessionManager(creds AWSCreds) (*s3.S3, *session.Session, error) {
	log.Info("Using AWS S3")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(creds.AWS_REGION),
		Credentials: credentials.NewStaticCredentials(creds.AWS_ACCESS_KEY_ID, creds.AWS_SECRET_ACCESS_KEY, ""),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating s3 session: %s", err.Error())
	}
	return s3.New(sess), sess, nil
}

func minIOSessionManager(mc MinioConfig) (*s3.S3, *session.Session, error) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(mc.S3Endpoint),
		Region:           aws.String(mc.S3Region),
		Credentials:      credentials.NewStaticCredentials(mc.AccessKeyID, mc.SecretAccessKey, ""),
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
}
