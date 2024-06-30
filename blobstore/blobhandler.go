package blobstore

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/Dewberry/s3api/auth"
	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
)

type S3Controller struct {
	Sess    *session.Session
	S3Svc   s3iface.S3API
	Buckets []string
	S3Mock  bool
}

// Config holds the configuration settings for the REST API server.
type Config struct {
	// Only settings that are typically environment-specific and can be loaded from
	// external sources like configuration files, environment variables should go here.
	AuthLevel                             int
	LimitedWriterRoleName                 string
	LimitedReaderRoleName                 string
	DefaultTempPrefix                     string
	DefaultDownloadPresignedUrlExpiration int
	DefaultUploadPresignedUrlExpiration   int
	DefaultScriptDownloadSizeLimit        int
	DefaultZipDownloadSizeLimit           int
}

// Store configuration for the handler
type BlobHandler struct {
	S3Controllers   []S3Controller
	Mu              sync.Mutex
	AllowAllBuckets bool
	DB              auth.Database
	Config          *Config
}

// Initializes resources and return a new handler (errors are fatal)
func NewBlobHandler(envJson string, authLvl int) (*BlobHandler, error) {
	// Create a new BlobHandler configuration
	config := BlobHandler{
		Config: newConfig(authLvl),
	}

	if authLvl > 0 {
		if err := configberry.CheckEnvVariablesExist([]string{"AUTH_LIMITED_WRITER_ROLE"}); err != nil {
			return &config, err
		}
		config.Config.AuthLevel = authLvl
		db, err := auth.NewPostgresDB()
		if err != nil {
			return &config, err
		}
		config.DB = db
	}
	s3MockStr := os.Getenv("S3_MOCK")
	if s3MockStr == "" {
		s3MockStr = "0"
	}
	s3Mock, err := strconv.Atoi(s3MockStr)
	if err != nil {
		errMsg := fmt.Errorf("could not convert `S3_MOCK` env variable to integer: %v", err)
		return &config, errMsg
	}
	// Check if the S3_MOCK environment variable is set to "true"
	if s3Mock == 1 {
		log.Info("using MinIO")

		// Load MinIO credentials from environment
		mc := newMinioConfig()

		// Validate MinIO credentials, check if they are missing or incomplete
		// if not then the s3api won't start
		if err := mc.validateMinioConfig(); err != nil {
			return &config, err
		}

		// Create a MinIO session and S3 client
		s3SVC, sess, err := mc.minIOSessionManager()
		if err != nil {
			return &config, err
		}

		// Configure the BlobHandler with MinIO session and bucket information
		config.S3Controllers = []S3Controller{{Sess: sess, S3Svc: s3SVC, Buckets: []string{mc.Bucket}, S3Mock: true}}
		// Return the configured BlobHandler
		return &config, nil
	}

	// Using AWS S3
	// Load AWS credentials from the provided .env.json file
	log.Debug("looking for `.env.json`")
	awsConfig, err := newAWSConfig(envJson)

	// Check if loading AWS credentials from .env.json failed
	if err != nil {
		errMsg := fmt.Errorf("`env.json` credentials extraction failed, please check `.env.json.example` for reference on formatting, %s", err.Error())
		return &config, errMsg
	}

	//does it contain "*"
	config.AllowAllBuckets = arrayContains("*", awsConfig.BucketAllowList)

	// Convert allowed buckets to a map for efficient lookup
	allowedBucketsMap := make(map[string]struct{})
	for _, bucket := range awsConfig.BucketAllowList {
		allowedBucketsMap[bucket] = struct{}{}
	}

	// Load AWS credentials for multiple accounts from .env.json
	for _, ac := range awsConfig.Accounts {
		// Create an AWS session and S3 client for each account
		s3SVC, sess, err := ac.aWSSessionManager()
		if err != nil {
			errMsg := fmt.Errorf("failed to create AWS session: %s", err.Error())
			return &config, errMsg
		}

		S3Ctrl := S3Controller{Sess: sess, S3Svc: s3SVC}
		// Retrieve the list of buckets for each account
		result, err := S3Ctrl.ListBuckets()
		if err != nil {
			errMsg := fmt.Errorf("failed to retrieve list of buckets with access key: %s, error: %s", ac.AWS_ACCESS_KEY_ID, err.Error())
			return &config, errMsg
		}

		var bucketNames []string
		if config.AllowAllBuckets {
			// Directly add all bucket names if allowAllBucket is true
			for _, bucket := range result.Buckets {
				bucketNames = append(bucketNames, aws.StringValue(bucket.Name))
			}
		} else {
			// Filter and add only the allowed buckets
			for _, bucket := range result.Buckets {
				if _, exists := allowedBucketsMap[*bucket.Name]; exists {
					bucketNames = append(bucketNames, aws.StringValue(bucket.Name))
					// Remove this bucket from the allowed list map
					delete(allowedBucketsMap, *bucket.Name)
				}
			}
		}

		if len(bucketNames) > 0 {
			config.S3Controllers = append(config.S3Controllers, S3Controller{Sess: sess, S3Svc: s3SVC, Buckets: bucketNames, S3Mock: false})
		}
	}

	if !config.AllowAllBuckets && len(allowedBucketsMap) > 0 {
		missingBuckets := make([]string, 0, len(allowedBucketsMap))
		for bucket := range allowedBucketsMap {
			missingBuckets = append(missingBuckets, bucket)
		}
		errMsg := fmt.Errorf("some buckets in the `bucket_allow_list` were not found: %v", missingBuckets)
		return &config, errMsg
	}

	// Return the configured BlobHandler
	return &config, nil
}

func (s3Ctrl *S3Controller) getBucketRegion(bucketName string) (string, error) {
	req, output := s3Ctrl.S3Svc.GetBucketLocationRequest(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucketName),
	})

	err := req.Send()
	if err != nil {
		return "", err
	}

	if output.LocationConstraint == nil {
		return "us-east-1", nil
	}

	return *output.LocationConstraint, nil
}

func (bh *BlobHandler) GetController(bucket string) (*S3Controller, error) {
	if bucket == "" {
		err := errors.New("parameter `bucket` is required")
		return nil, err
	}
	var s3Ctrl S3Controller
	for i := 0; i < len(bh.S3Controllers); i++ {
		for _, b := range bh.S3Controllers[i].Buckets {
			if b == bucket {
				s3Ctrl = bh.S3Controllers[i]

				// Detect the bucket's region
				region, err := s3Ctrl.getBucketRegion(b)
				if err != nil {
					log.Errorf("Failed to get region for bucket '%s': %s", b, err.Error())
					continue
				}
				// Check if the region is the same. If not, update the session and client
				currentRegion := *s3Ctrl.Sess.Config.Region
				if currentRegion != region {
					log.Debugf("current region: %s, region of bucket: %s, attempting to create a new controller", currentRegion, region)

					newSession, err := session.NewSession(&aws.Config{
						Region:      aws.String(region),
						Credentials: s3Ctrl.Sess.Config.Credentials,
					})
					if err != nil {
						log.Errorf("Failed to create a new session for region '%s': %s", region, err.Error())
						continue
					}
					s3Ctrl.Sess = newSession
					s3Ctrl.S3Svc = s3.New(s3Ctrl.Sess)
					bh.Mu.Lock()
					bh.S3Controllers[i] = s3Ctrl
					bh.Mu.Unlock()
				}

				return &s3Ctrl, nil
			}
		}
	}
	errMsg := fmt.Errorf("`bucket` '%s' not found", bucket)
	return &s3Ctrl, errMsg
}
