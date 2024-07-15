package blobstore

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

// CheckAndAdjustPrefix checks if the prefix is an object and adjusts the prefix accordingly.
// Returns the adjusted prefix, an error message (if any), and the HTTP status code.
func CheckAndAdjustPrefix(s3Ctrl *S3Controller, bucket, prefix string) (string, string, int) {
	// As of 6/12/24, unsure why ./ is included here, may be needed for an edge case, but could also cause problems
	if prefix != "" && prefix != "./" && prefix != "/" {
		isObject, err := s3Ctrl.KeyExists(bucket, prefix)
		if err != nil {
			return "", fmt.Sprintf("error checking if object exists: %s", err.Error()), http.StatusInternalServerError
		}
		if isObject {
			objMeta, err := s3Ctrl.GetMetaData(bucket, prefix)
			if err != nil {
				return "", fmt.Sprintf("error checking for object's metadata: %s", err.Error()), http.StatusInternalServerError
			}
			// This is because AWS considers empty prefixes with a .keep as an object, so we ignore and log
			if *objMeta.ContentLength == 0 {
				log.Infof("detected a zero byte directory marker within prefix: %s", prefix)
			} else {
				return "", fmt.Sprintf("`%s` is an object, not a prefix. Please see options for keys or pass a prefix", prefix), http.StatusTeapot
			}
		}
		prefix = strings.Trim(prefix, "/") + "/"
	}
	return prefix, "", http.StatusOK
}

// HandleListByPrefix handles the API endpoint for listing objects by prefix in an S3 bucket.
func (bh *BlobHandler) HandleListByPrefix(c echo.Context) error {
	prefix := c.QueryParam("prefix")

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	adjustedPrefix, errMsg, statusCode := CheckAndAdjustPrefix(s3Ctrl, bucket, prefix)
	if errMsg != "" {
		log.Error(errMsg)
		return c.JSON(statusCode, errMsg)
	}
	prefix = adjustedPrefix

	delimiterParam := c.QueryParam("delimiter")
	delimiter := true
	if delimiterParam != "" {
		delimiter, err = strconv.ParseBool(delimiterParam)
		if err != nil {
			errMsg := fmt.Errorf("error parsing `delimiter` param: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
		}

	}

	if delimiter && prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	var result []string
	permissions, fullAccess, statusCode, err := bh.GetS3ReadPermissions(c, bucket)
	if err != nil {
		log.Error(err.Error())
		return c.JSON(statusCode, err.Error())
	}
	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, cp := range page.CommonPrefixes {
			// Handle directories (common prefixes)
			if fullAccess || IsPermittedPrefix(bucket, *cp.Prefix, permissions) {
				result = append(result, aws.StringValue(cp.Prefix))

			}
		}
		for _, object := range page.Contents {
			// Handle files
			// Skip zero-byte objects that match a common prefix with a trailing slash
			if *object.Size == 0 && strings.HasSuffix(*object.Key, "/") {
				continue
			}
			if fullAccess || IsPermittedPrefix(bucket, *object.Key, permissions) {
				result = append(result, aws.StringValue(object.Key))
			}

		}
		return nil
	}
	err = s3Ctrl.GetListWithCallBack(bucket, prefix, delimiter, processPage)
	if err != nil {
		errMsg := fmt.Errorf("error processing objects: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Info("Successfully retrieved list by prefix:", prefix)
	return c.JSON(http.StatusOK, result)
}

// HandleListByPrefixWithDetail retrieves a detailed list of objects in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleListByPrefixWithDetail(c echo.Context) error {
	prefix := c.QueryParam("prefix")

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	adjustedPrefix, errMsg, statusCode := CheckAndAdjustPrefix(s3Ctrl, bucket, prefix)
	if errMsg != "" {
		log.Error(errMsg)
		return c.JSON(statusCode, errMsg)
	}
	prefix = adjustedPrefix

	delimiterParam := c.QueryParam("delimiter")
	delimiter := true
	if delimiterParam != "" {
		delimiter, err = strconv.ParseBool(delimiterParam)
		if err != nil {
			errMsg := fmt.Errorf("error parsing `delimiter` param: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
		}

	}

	if delimiter && prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	var results []ListResult
	var count int
	permissions, fullAccess, statusCode, err := bh.GetS3ReadPermissions(c, bucket)
	if err != nil {
		log.Error(err.Error())
		return c.JSON(statusCode, err.Error())
	}
	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, cp := range page.CommonPrefixes {
			// Handle directories (common prefixes)
			if fullAccess || IsPermittedPrefix(bucket, *cp.Prefix, permissions) {
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
			// Skip zero-byte objects that match a common prefix with a trailing slash
			if *object.Size == 0 && strings.HasSuffix(*object.Key, "/") {
				continue
			}
			if fullAccess || IsPermittedPrefix(bucket, *object.Key, permissions) {
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
		errMsg := fmt.Errorf("error processing objects: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Info("Successfully retrieved detailed list by prefix:", prefix)
	return c.JSON(http.StatusOK, results)
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

// IsPermittedPrefix checks if the prefix is within the user's permissions.
func IsPermittedPrefix(bucket, prefix string, permissions []string) bool {
	prefixForChecking := fmt.Sprintf("/%s/%s", bucket, prefix)

	// Check if any of the permissions indicate the prefixForChecking is a parent directory
	for _, perm := range permissions {
		// Add a trailing slash to permission if it represents a directory
		if !strings.HasSuffix(perm, "/") {
			perm += "/"
		}
		// Split the paths into components
		prefixComponents := strings.Split(prefixForChecking, "/")
		permComponents := strings.Split(perm, "/")

		// Compare each component
		match := true
		for i := 1; i < len(prefixComponents) && i < len(permComponents); i++ {
			if permComponents[i] == "" || prefixComponents[i] == "" {
				break
			}
			if prefixComponents[i] != permComponents[i] {
				match = false
				break
			}
		}

		// If all components match up to the length of the permission path,
		// and the permission path has no additional components, return true
		if match {
			return true
		}
	}
	return false
}
