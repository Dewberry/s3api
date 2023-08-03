package blobstore

import (
	"encoding/json"
	"errors"
	"fmt"

	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
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

// ListByPrefix retrieves a list of object keys in the specified S3 bucket with a given prefix.
// It takes the prefix, recursive flag, and bucket name from the query parameters and returns the object keys as a JSON response.
//
// The prefix parameter represents the prefix for the objects in the S3 bucket.
// The delimiter flag, when set to "false", includes objects from subdirectories as well.
// The bucket parameter represents the name of the S3 bucket. If not provided in the query parameters, it falls back to the S3_BUCKET environment variable.
// getSize returns the total size and file count of objects in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleListByPrefix(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Info("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	delimiterParam := c.QueryParam("delimiter")

	var delimiter bool

	if delimiterParam == "true" || delimiterParam == "false" {
		var err error
		delimiter, err = strconv.ParseBool(c.QueryParam("delimiter"))
		if err != nil {
			log.Info("HandleListByPrefix: Error parsing `delimiter` param:", err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

	} else {
		err := errors.New("request must include a `delimiter`, options are `true` or `false`")
		log.Info("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())

	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleListByPrefix: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	response, err := bh.getList(bucket, prefix, delimiter)
	if err != nil {
		log.Info("HandleListByPrefix: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}

	//return the list of objects as a slice of strings
	var objectKeys []string
	for _, object := range response.Contents {
		objectKeys = append(objectKeys, aws.StringValue(object.Key))
	}

	log.Info("HandleListByPrefix: Successfully retrieved list by prefix:", prefix)
	return c.JSON(http.StatusOK, objectKeys)
}

func (bh *BlobHandler) HandleBucketViewList(c echo.Context) error {
	prefix := c.QueryParam("prefix")

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleBucketViewList: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	var result *[]ListResult
	var err error
	if prefix == "" || prefix == "/" {
		result, err = listRoot(bucket, bh.S3Svc)
	} else {
		result, err = listDir(bucket, prefix, bh.S3Svc)
	}
	if err != nil {
		log.Info("HandleBucketViewList: Error listing bucket:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleBucketViewList: Successfully retrieved bucket view list with prefix:", prefix)
	return c.JSON(http.StatusOK, result)
}

// HandleGetSize retrieves the total size and the number of files in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleGetSize(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Info("HandleGetSize: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleGetSize: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	list, err := bh.getList(bucket, prefix, false)
	if err != nil {
		log.Info("HandleGetSize: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	size, fileCount, err := bh.getSize(list)
	if err != nil {
		log.Info("HandleGetSize: Error getting size:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	response := struct {
		Size      uint64 `json:"size"`
		FileCount uint32 `json:"file_count"`
		Prefix    string `json:"prefix"`
	}{
		Size:      size,
		FileCount: fileCount,
		Prefix:    prefix,
	}

	log.Debug("HandleGetSize: Successfully retrieved size for prefix:", prefix)
	return c.JSON(http.StatusOK, response)
}

// HandleGetMetaData retrieves the metadata of an object from the specified S3 bucket.
// It takes the object key as a parameter and returns the metadata or an error as a JSON response.
//
// The key parameter represents the key of the object in the S3 bucket.
// If the key ends with '/', it indicates a prefix instead of an object key and returns an error.
// The S3_BUCKET environment variable is used as the bucket name.
func (bh *BlobHandler) HandleGetMetaData(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("request must include a `key` parameter")
		log.Info("HandleGetMetaData: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter  was not provided by the user and is not defined in .env")
			log.Info("HandleGetMetaData: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	// Set up the input parameters for the list objects operation
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := bh.S3Svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			err := fmt.Errorf("object %s not found", key)
			log.Info("HandleGetMetaData: " + err.Error())
			return c.JSON(http.StatusBadRequest, err.Error())
		}
		log.Info("HandleGetMetaData: Error getting metadata:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetMetaData: Successfully retrieved metadata for key:", key)
	return c.JSON(http.StatusOK, result)
}

func (bh *BlobHandler) HandleGetPresignedURL(c echo.Context) error {
	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleGetPresignedURL: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter `key` is required")
		log.Info("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleGetPresignedURL: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found", key)
		log.Info("HandleGetPresignedURL: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	// Set the expiration time for the pre-signed URL
	expirationStr := c.QueryParam("expiration")
	var expiration time.Duration
	if expirationStr == "" {
		expiration = time.Minute // Default value set to 1 minute
	} else {
		duration, err := time.ParseDuration(expirationStr)
		if err != nil {
			// Handle error when parsing the expiration string
			errorMsg := fmt.Errorf("invalid expiration value %s Please provide a valid duration format", expirationStr)
			log.Info("HandleGetPresignedURL: " + errorMsg.Error())
			return c.JSON(http.StatusBadRequest, errorMsg)
		}
		expiration = duration
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, _ := bh.S3Svc.GetObjectRequest(input)
	url, err := req.Presign(expiration)
	if err != nil {
		log.Info("HandleGetPresignedURL: Error generating presigned URL:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleGetPresignedURL: Successfully generated presigned URL for key:", key)
	return c.JSON(http.StatusOK, url)
}

func (bh *BlobHandler) HandleGetPresignedURLMultiObj(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Info("HandleGetPresignedURLMultiObj: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleGetPresignedURLMultiObj: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	response, err := bh.getList(bucket, prefix, false)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}
	//check if size is below 5GB
	size, fileCount, err := bh.getSize(response)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error getting size:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	limit := uint64(1024 * 1024 * 1024 * 5)
	if size > limit {
		log.Printf("HandleGetPresignedURLMultiObj: Request entity is larger than %v GB, current file size is: %d, and current file count is: %d", float64(limit)/(1024*1024*1024), size, fileCount)
		return c.JSON(http.StatusRequestEntityTooLarge, fmt.Sprintf("request entity is larger than %v GB, current file size is: %d, and current file count is: %d", float64(limit)/(1024*1024*1024), size, fileCount))
	}

	if *response.KeyCount == 0 {
		errMsg := fmt.Errorf("the specified prefix %s does not exist in S3", prefix)
		log.Info("HandleGetPresignedURLMultiObj: " + errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error())
	}

	ext := filepath.Ext(prefix)
	base := strings.TrimSuffix(prefix, ext)
	uuid := GenerateRandomString()
	filename := fmt.Sprintf("%s-%s.%s", filepath.Base(base), uuid, "tar.gz")
	outputFile := filepath.Join(TEMP_PREFIX, filename)

	err = bh.tarS3Files(response, bucket, outputFile, prefix)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error tarring S3 files:", err.Error())
		return err
	}

	href, err := getPresignedURL(bucket, outputFile, URL_EXP_DAYS)
	if err != nil {
		log.Info("HandleGetPresignedURLMultiObj: Error getting presigned URL:", err.Error())
		return err
	}

	log.Info("HandleGetPresignedURLMultiObj: Successfully generated presigned URL for prefix:", prefix)
	return c.JSON(http.StatusOK, string(href))
}

func (bh *BlobHandler) HandleMultipartUpload(c echo.Context) error {
	// Add overwrite check and parameter
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Info("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleMultipartUpload: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	overrideParam := c.QueryParam("override")

	var override bool

	if overrideParam == "true" || overrideParam == "false" {
		var err error
		override, err = strconv.ParseBool(c.QueryParam("override"))
		if err != nil {
			log.Info("HandleMultipartUpload: Error parsing 'override' parameter:", err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

	} else {
		err := errors.New("request must include a `override`, options are `true` or `false`")
		log.Info("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleMultipartUpload: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if keyExist && !override {
		err := fmt.Errorf("object %s already exists and override is set to %t", key, override)
		log.Info("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	body := c.Request().Body
	defer func() { _ = body.Close() }()

	err = bh.UploadS3Obj(bucket, key, body)
	if err != nil {
		log.Info("HandleMultipartUpload: Error uploading S3 object:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleMultipartUpload: Successfully uploaded file with key:", key)
	return c.JSON(http.StatusOK, "Successfully uploaded file")
}

// HandleDeleteObject handles the API endpoint for deleting an object/s from an S3 bucket.
// It expects the 'key' query parameter to specify the object key and the 'bucket' query parameter to specify the bucket name (optional, falls back to environment variable 'S3_BUCKET').
// It returns an appropriate JSON response indicating the success or failure of the deletion.
func (bh *BlobHandler) HandleDeleteObjects(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Info("HandleDeleteObjects: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleDeleteObjects: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}
	// Split the key into segments
	segments := strings.Split(key, "/")

	// Check if the key is two levels deep
	if len(segments) < 3 {
		errMsg := fmt.Errorf("invalid key: %s. Only objects three levels deep can be deleted", key)
		log.Info("HandleDeleteObjects: " + errMsg.Error())
		return c.JSON(http.StatusBadRequest, errMsg.Error())
	}

	// Check if the key represents a prefix
	if strings.HasSuffix(key, "/") {
		response, err := bh.getList(bucket, key, false)
		if err != nil {
			log.Info("HandleDeleteObjects: Error getting list:", err.Error())
			return c.JSON(http.StatusInternalServerError, err)
		}
		if *response.KeyCount == 0 {
			errMsg := fmt.Errorf("the specified prefix %s does not exist in S3", key)
			log.Info("HandleDeleteObjects: " + errMsg.Error())
			return c.JSON(http.StatusBadRequest, errMsg.Error())
		}
		// This will recursively delete all objects with the specified prefix
		err = RecursivelyDeleteObjects(bh.S3Svc, bucket, key)
		if err != nil {
			msg := fmt.Sprintf("error deleting objects. %s", err.Error())
			log.Info("HandleDeleteObjects: " + msg)
			return c.JSON(http.StatusInternalServerError, msg)
		}

		log.Info("HandleDeleteObjects: Successfully deleted folder and its contents for prefix:", key)
		return c.JSON(http.StatusOK, "Successfully deleted folder and its contents")
	}

	// If the key is not a folder, proceed with deleting a single object
	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleDeleteObjects: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found, add a trailing `/` if you want to delete multiple files", key)
		log.Info("HandleDeleteObjects: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err = bh.S3Svc.DeleteObject(deleteInput)
	if err != nil {
		msg := fmt.Sprintf("error deleting object. %s", err.Error())
		log.Info("HandleDeleteObjects: " + msg)
		return c.JSON(http.StatusInternalServerError, msg)
	}

	log.Info("HandleDeleteObjects: Successfully deleted file with key:", key)
	return c.JSON(http.StatusOK, "Successfully deleted file")
}

func (bh *BlobHandler) HandleObjectContents(c echo.Context) error {
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleObjectContents: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	fileEXT := strings.ToLower(filepath.Ext(key))

	if fileEXT == "" {
		err := errors.New("file has no extension")
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleObjectContents: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if !keyExist {
		err := fmt.Errorf("object %s not found", key)
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	var contentType string
	switch fileEXT {
	case ".csv", ".txt", ".py":
		contentType = "text/plain"
	case ".png":
		contentType = "image/png"
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".html", ".log":
		contentType = "text/html"
	case ".json":
		contentType = "application/json"
	default:
		err := fmt.Errorf("file of type `%s` cannot be viewed", filepath.Ext(key))
		log.Info("HandleObjectContents: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	output, err := bh.S3Svc.GetObject(input)
	if err != nil {
		log.Info("HandleObjectContents: Error getting object from S3:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	var data []byte
	err = json.NewDecoder(output.Body).Decode(&data)
	if err != nil {
		log.Info("HandleObjectContents: Error reading object data:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleObjectContents: Successfully fetched object data for key:", key)
	return c.Blob(http.StatusOK, contentType, data)
}

// function now returns an error when the prefix provided does not exist
// earlier behavior was to return JSON null
func (bh *BlobHandler) HandleHUC12ChildrenExist(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Info("HandleHUC12ChildrenExist: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleHUC12ChildrenExist: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	resp, err := bh.getList(bucket, prefix, true)
	if err != nil {
		log.Info("HandleHUC12ChildrenExist: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err)
	}
	if *resp.KeyCount == 0 {
		log.Info("HandleHUC12ChildrenExist: No children exist for prefix:", prefix)
		return c.JSON(http.StatusNoContent, nil)
	}

	//check if provided prefix is a valid huc12 pattern
	// base := path.Base(prefix)
	// huc12Regex := regexp.MustCompile(huc12Pattern)
	// if !huc12Regex.MatchString(base) {
	// 	errMsg := fmt.Errorf("The specified prefix/huc12 %s does not match the following pattern %s. please provide a valid huc12", base, huc12Pattern)
	// 	return c.JSON(http.StatusBadRequest, errMsg.Error())
	// }

	var folders []string
	for _, commonPrefix := range resp.CommonPrefixes {
		folder := *commonPrefix.Prefix
		folder = strings.TrimSuffix(folder, "/") // Remove trailing slash
		subfolder := path.Base(folder)

		folders = append(folders, subfolder)
	}

	log.Info("HandleHUC12ChildrenExist: Found children for prefix:", prefix)
	return c.JSON(http.StatusOK, folders)
}
