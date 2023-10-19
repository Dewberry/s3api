package blobstore

import (
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
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
	// Create a new BlobHandler configuration
	config := BlobHandler{}

	// Check if the S3_MOCK environment variable is set to "true"
	if os.Getenv("S3_MOCK") == "true" {
		log.Info("Using MinIO")

		// Load MinIO credentials from environment
		creds := newMinioConfig()

		// Validate MinIO credentials, check if they are missing or incomplete
		// if not then the s3api won't start
		if err := creds.validateMinioConfig(); err != nil {
			log.Fatalf("MINIO credentials are either not provided or contain missing variables: %s", err.Error())
		}

		// Create a MinIO session and S3 client
		s3SVC, sess, err := minIOSessionManager(creds)
		if err != nil {
			log.Fatalf("failed to create MinIO session: %s", err.Error())
		}

		// Configure the BlobHandler with MinIO session and bucket information
		config.S3Controllers = []S3Controller{{Sess: sess, S3Svc: s3SVC, Buckets: []string{creds.Bucket}}}
		config.NamedBucketOnly = true

		// Return the configured BlobHandler
		return &config, nil
	}

	// Using AWS S3

	// Load AWS credentials from the provided .env.json file
	awsConfig, err := newAWSConfig(envJson)

	// Check if loading AWS credentials from .env.json failed
	if err != nil {
		log.Warnf("env.json credentials extraction failed, attmepting to retreive from environment, %s", err.Error())
		creds := awsFromENV()

		// Validate AWS credentials, check if they are missing or incomplete
		// if not then the s3api won't start
		if err := creds.validateAWSCreds(); err != nil {
			log.Fatalf("AWS credentials are either not provided or contain missing variables: %s", err.Error())
		}

		// Create an AWS session and S3 client
		s3SVC, sess, err := aWSSessionManager(creds)
		if err != nil {
			log.Fatalf("failed to create AWS session: %s", err.Error())
		}

		// Configure the BlobHandler with AWS session and bucket information
		config.S3Controllers = []S3Controller{{Sess: sess, S3Svc: s3SVC, Buckets: []string{os.Getenv("S3_BUCKET")}}}
		config.NamedBucketOnly = true

		// Return the configured BlobHandler
		return &config, nil
	}

	// Using AWS S3 with multiple accounts

	// Load AWS credentials for multiple accounts from .env.json
	for _, creds := range awsConfig.Accounts {
		// Create an AWS session and S3 client for each account
		s3SVC, sess, err := aWSSessionManager(creds)
		if err != nil {
			return nil, fmt.Errorf("failed to create AWS session: %s", err.Error())
		}

		S3Ctrl := S3Controller{Sess: sess, S3Svc: s3SVC}

		// Retrieve the list of buckets for each account
		result, err := S3Ctrl.listBuckets()
		if err != nil {
			return nil, err
		}

		// Extract and store bucket names associated with the account
		var bucketNames []string
		for _, bucket := range result.Buckets {
			bucketNames = append(bucketNames, aws.StringValue(bucket.Name))
		}

		// Configure the BlobHandler with AWS sessions and associated bucket information
		config.S3Controllers = append(config.S3Controllers, S3Controller{Sess: sess, S3Svc: s3SVC, Buckets: bucketNames})
	}

	// Indicate that the BlobHandler can access multiple buckets under different AWS accounts
	config.NamedBucketOnly = false

	// Return the configured BlobHandler
	return &config, nil
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

	// Check if the bucket exists
	s3SVC := s3.New(sess)
	_, err = s3SVC.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(mc.Bucket),
	})
	if err != nil {
		// Bucket does not exist, create it
		_, err := s3SVC.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(mc.Bucket),
		})
		if err != nil {
			log.Errorf("Error creating bucket: %s", err.Error())
			return nil, nil, nil
		}
		log.Info("Bucket created successfully")
	} else {
		log.Info("Bucket already exists")
	}

	return s3SVC, sess, nil
}

func (bh *BlobHandler) GetController(bucket string) (*S3Controller, error) {
	if bucket == "" {
		err := fmt.Errorf("parameter 'bucket' is required")
		log.Error(err.Error())
		return nil, err
	}
	var s3Ctrl S3Controller
	for _, controller := range bh.S3Controllers {
		for _, b := range controller.Buckets {
			if b == bucket {
				s3Ctrl = controller
				return &s3Ctrl, nil
			}
		}
	}
	return &s3Ctrl, fmt.Errorf("bucket '%s' not found", bucket)
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
