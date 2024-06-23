package blobstore

import (
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

// ListResult is the result struct for listing objects with additional details.
type ListResult struct {
	ID         int       `json:"id"`
	Bucket     string    `json:"bucket"`
	Name       string    `json:"filename"`
	Size       string    `json:"size"`
	Path       string    `json:"filepath"`
	Type       string    `json:"type"`
	IsDir      bool      `json:"isdir"`
	Modified   time.Time `json:"modified"`
	ModifiedBy string    `json:"modified_by"`
}

// GetList retrieves a list of objects in the specified S3 bucket with the given prefix.
// If delimiter is set to true, it will search for any objects within the prefix provided.
// If no objects are found, it will return null even if there were prefixes within the user-provided prefix.
// If delimiter is set to false, it will look for all prefixes that start with the user-provided prefix.
func (s3Ctrl *S3Controller) GetList(bucket, prefix string, delimiter bool) (*s3.ListObjectsV2Output, error) {
	// Set up input parameters for the ListObjectsV2 API
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000), // Set the desired maximum keys per request
	}
	if delimiter {
		input.SetDelimiter("/")
	}
	// Retrieve the list of objects in the bucket with the specified prefix
	var response *s3.ListObjectsV2Output
	err := s3Ctrl.S3Svc.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		if response == nil {
			response = page
		} else {
			response.Contents = append(response.Contents, page.Contents...)
		}

		// Check if there are more pages to retrieve
		if *page.IsTruncated {
			// Set the continuation token for the next request
			input.ContinuationToken = page.NextContinuationToken
			return true // Continue to the next page
		}

		return false // Stop pagination
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

// GetListWithCallBack is the same as GetList, except instead of returning the entire list at once, it allows processing page by page.
// This method is safer than GetList as it avoids memory overload for large datasets by processing data on the go.
func (s3Ctrl *S3Controller) GetListWithCallBack(bucket, prefix string, delimiter bool, processPage func(*s3.ListObjectsV2Output) error) error {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000), // Adjust the MaxKeys as needed
	}

	if delimiter {
		input.SetDelimiter("/")
	}

	var lastError error // Variable to capture the last error

	// Iterate over the pages of results
	err := s3Ctrl.S3Svc.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		lastError = processPage(page)
		return lastError == nil && *page.IsTruncated // Continue if no error and more pages are available
	})

	if lastError != nil {
		return lastError // Return the last error encountered in the processPage function
	}
	return err // Return any errors encountered in the pagination process
}

// HandleListByPrefix handles the API endpoint for listing objects by prefix in an S3 bucket.
func (bh *BlobHandler) HandleListByPrefix(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	adjustedPrefix, appErr := s3Ctrl.checkAndAdjustPrefix(bucket, prefix)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	prefix = adjustedPrefix

	delimiterParam := c.QueryParam("delimiter")
	delimiter := true
	if delimiterParam != "" {
		delimiter, err = strconv.ParseBool(delimiterParam)
		if err != nil {
			appErr := configberry.NewAppError(configberry.ValidationError, "error parsing `delimiter` param", nil)
			log.Error(configberry.LogErrorFormatter(appErr, true))
			return configberry.HandleErrorResponse(c, appErr)
		}

	}

	if delimiter && prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	var results []string
	permissions, fullAccess, appErr := bh.getS3ReadPermissions(c, bucket)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, cp := range page.CommonPrefixes {
			// Handle directories (common prefixes)
			if fullAccess || isPermittedPrefix(bucket, *cp.Prefix, permissions) {
				results = append(results, aws.StringValue(cp.Prefix))

			}
		}
		for _, object := range page.Contents {
			// Handle files
			if fullAccess || isPermittedPrefix(bucket, *object.Key, permissions) {
				results = append(results, aws.StringValue(object.Key))
			}

		}
		return nil
	}

	err = s3Ctrl.GetListWithCallBack(bucket, prefix, delimiter, processPage)
	if err != nil {
		appErr := configberry.HandleAWSError(err, "error processing objects")
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Info("Successfully retrieved list by prefix:", prefix)
	return configberry.HandleSuccessfulResponse(c, results)
}

// HandleListByPrefixWithDetail retrieves a detailed list of objects in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleListByPrefixWithDetail(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "unable to get S3 controller", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	adjustedPrefix, appErr := s3Ctrl.checkAndAdjustPrefix(bucket, prefix)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}
	prefix = adjustedPrefix

	delimiterParam := c.QueryParam("delimiter")
	delimiter := true
	if delimiterParam != "" {
		delimiter, err = strconv.ParseBool(delimiterParam)
		if err != nil {
			appErr := configberry.NewAppError(configberry.ValidationError, "error parsing `delimiter` param", nil)
			log.Error(configberry.LogErrorFormatter(appErr, true))
			return configberry.HandleErrorResponse(c, appErr)
		}

	}

	if delimiter && prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	var results []ListResult
	var count int
	permissions, fullAccess, appErr := bh.getS3ReadPermissions(c, bucket)
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, cp := range page.CommonPrefixes {
			// Handle directories (common prefixes)
			if fullAccess || isPermittedPrefix(bucket, *cp.Prefix, permissions) {
				dir := ListResult{
					ID:         count,
					Name:       filepath.Base(*cp.Prefix),
					Size:       "",
					Path:       *cp.Prefix,
					Type:       "",
					IsDir:      true,
					ModifiedBy: "",
				}
				results = append(results, dir)
				count++
			}

		}

		for _, object := range page.Contents {
			// Handle files
			if fullAccess || isPermittedPrefix(bucket, *object.Key, permissions) {
				file := ListResult{
					ID:         count,
					Name:       filepath.Base(*object.Key),
					Size:       strconv.FormatInt(*object.Size, 10),
					Path:       filepath.Dir(*object.Key),
					Type:       filepath.Ext(*object.Key),
					IsDir:      false,
					Modified:   *object.LastModified,
					ModifiedBy: "",
				}
				results = append(results, file)
				count++
			}

		}
		return nil
	}
	err = s3Ctrl.GetListWithCallBack(bucket, prefix, delimiter, processPage)
	if err != nil {
		appErr := configberry.NewAppError(configberry.InternalServerError, "error processing objects", err)
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	log.Info("Successfully retrieved list by prefix:", prefix)
	return configberry.HandleSuccessfulResponse(c, results)
}
