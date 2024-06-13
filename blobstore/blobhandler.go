package blobstore

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/Dewberry/s3api/auth"
	envcheck "github.com/Dewberry/s3api/env-checker"
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
		if err := envcheck.CheckEnvVariablesExist([]string{"AUTH_LIMITED_WRITER_ROLE"}); err != nil {
			log.Fatal(err)
		}
		config.Config.AuthLevel = authLvl
		db, err := auth.NewPostgresDB()
		if err != nil {
			log.Fatal(err)
		}
		config.DB = db
	}
	s3MockStr := os.Getenv("S3_MOCK")
	var s3Mock int
	if s3MockStr == "" {
		s3Mock = 0
	}
	s3Mock, err := strconv.Atoi(s3MockStr)
	if err != nil {
		log.Fatalf("could not convert S3_MOCK env variable to integer: %v", err)
	}
	// Check if the S3_MOCK environment variable is set to "true"
	if s3Mock == 1 {
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
		config.S3Controllers = []S3Controller{{Sess: sess, S3Svc: s3SVC, Buckets: []string{creds.Bucket}, S3Mock: true}}
		// Return the configured BlobHandler
		return &config, nil
	}

	// Using AWS S3
	// Load AWS credentials from the provided .env.json file
	log.Debug("looking for .env.json")
	awsConfig, err := newAWSConfig(envJson)

	// Check if loading AWS credentials from .env.json failed
	if err != nil {
		return nil, fmt.Errorf("env.json credentials extraction failed, please check `.env.json.example` for reference on formatting, %s", err.Error())
	}

	//does it contain "*"
	config.AllowAllBuckets = arrayContains("*", awsConfig.BucketAllowList)

	// Convert allowed buckets to a map for efficient lookup
	allowedBucketsMap := make(map[string]struct{})
	for _, bucket := range awsConfig.BucketAllowList {
		allowedBucketsMap[bucket] = struct{}{}
	}

	// Load AWS credentials for multiple accounts from .env.json
	for _, creds := range awsConfig.Accounts {
		// Create an AWS session and S3 client for each account
		s3SVC, sess, err := aWSSessionManager(creds)
		if err != nil {
			errMsg := fmt.Errorf("failed to create AWS session: %s", err.Error())
			log.Error(errMsg.Error())
			return nil, errMsg
		}

		S3Ctrl := S3Controller{Sess: sess, S3Svc: s3SVC}
		// Retrieve the list of buckets for each account
		result, err := S3Ctrl.ListBuckets()
		if err != nil {
			errMsg := fmt.Errorf("failed to retrieve list of buckets with access key: %s, error: %s", creds.AWS_ACCESS_KEY_ID, err.Error())
			return nil, errMsg
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
		return nil, fmt.Errorf("some buckets in the allow list were not found: %v", missingBuckets)
	}

	// Return the configured BlobHandler
	return &config, nil
}

func aWSSessionManager(creds AWSCreds) (*s3.S3, *session.Session, error) {
	log.Info("Using AWS S3")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
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
		Region:           aws.String("us-east-1"),
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
	for i := 0; i < len(bh.S3Controllers); i++ {
		for _, b := range bh.S3Controllers[i].Buckets {
			if b == bucket {
				s3Ctrl = bh.S3Controllers[i]

				// Detect the bucket's region
				region, err := getBucketRegion(bh.S3Controllers[i].S3Svc, b)
				if err != nil {
					log.Errorf("Failed to get region for bucket '%s': %s", b, err.Error())
					continue
				}
				// Check if the region is the same. If not, update the session and client
				currentRegion := *s3Ctrl.Sess.Config.Region
				if currentRegion != region {
					log.Debugf("current region: %s region of bucket: %s, attempting to create a new controller", currentRegion, region)

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
	return &s3Ctrl, fmt.Errorf("bucket '%s' not found", bucket)
}

func getBucketRegion(S3Svc *s3.S3, bucketName string) (string, error) {
	req, output := S3Svc.GetBucketLocationRequest(&s3.GetBucketLocationInput{
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

func (bh *BlobHandler) GetS3ReadPermissions(c echo.Context, bucket string) ([]string, bool, int, error) {
	permissions, fullAccess, err := bh.GetUserS3ReadListPermission(c, bucket)
	if err != nil {
		return nil, false, http.StatusInternalServerError, fmt.Errorf("error fetching user permissions: %s", err.Error())
	}
	if !fullAccess && len(permissions) == 0 {
		return nil, false, http.StatusForbidden, fmt.Errorf("user does not have read permission to read the %s bucket", bucket)
	}
	return permissions, fullAccess, http.StatusOK, nil
}

func (bh *BlobHandler) HandleCheckS3UserPermission(c echo.Context) error {
	if bh.Config.AuthLevel == 0 {
		log.Info("Checked user permissions successfully")
		return c.JSON(http.StatusOK, true)
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
