package blobstore

import (
	"errors"
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

// HandleListByPrefix handles the API endpoint for listing objects by prefix in S3 bucket.
func (bh *BlobHandler) HandleListByPrefix(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Error("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("bucket %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	delimiterParam := c.QueryParam("delimiter")
	var delimiter bool
	if delimiterParam == "true" || delimiterParam == "false" {
		var err error
		delimiter, err = strconv.ParseBool(delimiterParam)
		if err != nil {
			log.Error("HandleListByPrefix: Error parsing `delimiter` param:", err.Error())
			return c.JSON(http.StatusUnprocessableEntity, err.Error())
		}

	} else {
		err := errors.New("request must include a `delimiter`, options are `true` or `false`")
		log.Error("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())

	}
	if delimiter {
		if !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}
	}

	isObject, err := s3Ctrl.KeyExists(bucket, prefix)
	if err != nil {
		log.Error("HandleListByPrefix: can't find bucket or object " + err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	if isObject {
		objMeta, err := s3Ctrl.GetMetaData(bucket, prefix)
		if err != nil {
			log.Error("HandleListByPrefix: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		if *objMeta.ContentLength == 0 {
			log.Infof("HandleListByPrefix: Detected a zero byte directory marker within prefix: %s", prefix)
		} else {
			err = fmt.Errorf("`%s` is an object, not a prefix. please see options for keys or pass a prefix", prefix)
			log.Error("HandleListByPrefix: " + err.Error())
			return c.JSON(http.StatusTeapot, err.Error())
		}
	}

	listOutput, err := s3Ctrl.GetList(bucket, prefix, delimiter)
	if err != nil {
		log.Error("HandleListByPrefix: Error getting list:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	// Convert the list of object keys to strings
	var objectKeys []string
	for _, object := range listOutput.Contents {
		objectKeys = append(objectKeys, aws.StringValue(object.Key))
	}

	log.Info("HandleListByPrefix: Successfully retrieved list by prefix:", prefix)
	return c.JSON(http.StatusOK, objectKeys)
}

// HandleListByPrefixWithDetail retrieves a detailed list of objects in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) HandleListByPrefixWithDetail(c echo.Context) error {
	prefix := c.QueryParam("prefix")

	bucket := c.QueryParam("bucket")

	// Fetch user permissions and full access status
	permissions, fullAccess, err := bh.GetUserS3ReadListPermission(c, bucket)
	if err != nil {
		errMsg := fmt.Errorf("error fetching user permissions: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	if !fullAccess && len(permissions) == 0 {
		errMsg := fmt.Errorf("user does not have read permission to read the %s bucket", bucket)
		log.Error(errMsg.Error())
		return c.JSON(http.StatusForbidden, errMsg.Error())
	}

	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("bucket %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	if prefix != "" && prefix != "./" && prefix != "/" {
		isObject, err := s3Ctrl.KeyExists(bucket, prefix)
		if err != nil {
			log.Error("HandleListByPrefixWithDetail: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		if isObject {
			objMeta, err := s3Ctrl.GetMetaData(bucket, prefix)
			if err != nil {
				log.Error("HandleListByPrefixWithDetail: " + err.Error())
				return c.JSON(http.StatusInternalServerError, err.Error())
			}
			if *objMeta.ContentLength == 0 {
				log.Infof("HandleListByPrefixWithDetail: Detected a zero byte directory marker within prefix: %s", prefix)
			} else {
				err = fmt.Errorf("`%s` is an object, not a prefix. please see options for keys or pass a prefix", prefix)
				log.Error("HandleListByPrefixWithDetail: " + err.Error())
				return c.JSON(http.StatusTeapot, err.Error())
			}
		}
		prefix = strings.Trim(prefix, "/") + "/"
	}

	query := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(1000),
	}

	result := []ListResult{}
	truncatedListing := true
	var count int
	for truncatedListing {

		resp, err := s3Ctrl.S3Svc.ListObjectsV2(query)
		if err != nil {
			log.Error("HandleListByPrefixWithDetail: error retrieving list with the following query ", err)
			errMsg := fmt.Errorf("HandleListByPrefixWithDetail: error retrieving list, %s", err.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}

		if fullAccess {
			// Append all results directly if user has full access
			for _, cp := range resp.CommonPrefixes {
				w := ListResult{
					ID:    count,
					Name:  filepath.Base(*cp.Prefix),
					Path:  *cp.Prefix,
					IsDir: true,
				}
				count++
				result = append(result, w)
			}

			for _, object := range resp.Contents {
				w := ListResult{
					ID:       count,
					Name:     filepath.Base(*object.Key),
					Size:     strconv.FormatInt(*object.Size, 10),
					Path:     filepath.Dir(*object.Key),
					Type:     filepath.Ext(*object.Key),
					IsDir:    false,
					Modified: *object.LastModified,
				}
				count++
				result = append(result, w)
			}
		} else {
			// Filter results based on permissions
			for _, cp := range resp.CommonPrefixes {
				if isPermittedPrefix(bucket, *cp.Prefix, permissions) {
					w := ListResult{
						ID:    count,
						Name:  filepath.Base(*cp.Prefix),
						Path:  *cp.Prefix,
						IsDir: true,
					}
					count++
					result = append(result, w)
				}
			}

			for _, object := range resp.Contents {
				if isPermittedPrefix(bucket, *object.Key, permissions) {
					w := ListResult{
						ID:       count,
						Name:     filepath.Base(*object.Key),
						Size:     strconv.FormatInt(*object.Size, 10),
						Path:     filepath.Dir(*object.Key),
						Type:     filepath.Ext(*object.Key),
						IsDir:    false,
						Modified: *object.LastModified,
					}
					count++
					result = append(result, w)
				}
			}
		}

		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}

	log.Info("HandleListByPrefix: Successfully retrieved list by prefix with detail:", prefix)
	return c.JSON(http.StatusOK, result)
}

func isPermittedPrefix(bucket, prefix string, permissions []string) bool {
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

// GetList retrieves a list of objects in the specified S3 bucket with the given prefix.
// if delimiter is set to true then it is going to search for any objects within the prefix provided, if no object sare found it will
// return null even if there was prefixes within the user provided prefix. If delimiter is set to false then it will look for all prefixes
// that start with the user provided prefix.
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
