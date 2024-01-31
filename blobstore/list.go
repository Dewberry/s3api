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

// HandleListByPrefix handles the API endpoint for listing objects by prefix in S3 bucket.
func (bh *BlobHandler) HandleListByPrefix(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		errMsg := fmt.Errorf("request must include a `prefix` parameter")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	bucket := c.QueryParam("bucket")
	s3Ctrl, err := bh.GetController(bucket)
	if err != nil {
		errMsg := fmt.Errorf("`bucket` %s is not available, %s", bucket, err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}

	delimiterParam := c.QueryParam("delimiter")
	var delimiter bool
	if delimiterParam == "true" || delimiterParam == "false" {
		var err error
		delimiter, err = strconv.ParseBool(delimiterParam)
		if err != nil {
			errMsg := fmt.Errorf("error parsing `delimiter` param: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
		}

	} else {
		errMsg := fmt.Errorf("request must include a `delimiter`, options are `true` or `false`")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())

	}
	if delimiter {
		if !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}
	}

	isObject, err := s3Ctrl.KeyExists(bucket, prefix)
	if err != nil {
		errMsg := fmt.Errorf("can't find bucket or object %s" + err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	if isObject {
		objMeta, err := s3Ctrl.GetMetaData(bucket, prefix)
		if err != nil {
			errMsg := fmt.Errorf("error getting metadata: %s" + err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}
		if *objMeta.ContentLength == 0 {
			log.Infof("Detected a zero byte directory marker within prefix: %s", prefix)
		} else {
			errMsg := fmt.Errorf("`%s` is an object, not a prefix. please see options for keys or pass a prefix", prefix)
			log.Error(errMsg.Error())
			return c.JSON(http.StatusTeapot, errMsg.Error())
		}
	}

	var objectKeys []string
	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, object := range page.Contents {
			objectKeys = append(objectKeys, aws.StringValue(object.Key))
		}
		return nil
	}

	err = s3Ctrl.GetListWithCallBack(bucket, prefix, delimiter, processPage)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, fmt.Sprintf("Error processing objects: %v", err))
	}

	log.Info("Successfully retrieved list by prefix:", prefix)
	return c.JSON(http.StatusOK, objectKeys)
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

	if prefix != "" && prefix != "./" && prefix != "/" {
		isObject, err := s3Ctrl.KeyExists(bucket, prefix)
		if err != nil {
			errMsg := fmt.Errorf("error checking if key exists: %s", err.Error())
			log.Error(errMsg.Error())
			return c.JSON(http.StatusInternalServerError, errMsg.Error())
		}
		if isObject {
			objMeta, err := s3Ctrl.GetMetaData(bucket, prefix)
			if err != nil {
				errMsg := fmt.Errorf("error checking for object's metadata: %s", err.Error())
				log.Error(errMsg.Error())
				return c.JSON(http.StatusInternalServerError, errMsg.Error())
			}
			if *objMeta.ContentLength == 0 {
				log.Infof("detected a zero byte directory marker within prefix: %s", prefix)
			} else {
				errMsg := fmt.Errorf("`%s` is an object, not a prefix. please see options for keys or pass a prefix", prefix)
				log.Error(errMsg.Error())
				return c.JSON(http.StatusTeapot, errMsg.Error())
			}
		}
		prefix = strings.Trim(prefix, "/") + "/"
	}

	var results []ListResult
	var count int

	processPage := func(page *s3.ListObjectsV2Output) error {
		for _, cp := range page.CommonPrefixes {
			// Handle directories (common prefixes)
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

		for _, object := range page.Contents {
			// Handle files
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
		return nil
	}

	err = s3Ctrl.GetListWithCallBack(bucket, prefix, true, processPage)
	if err != nil {
		errMsg := fmt.Errorf("error processing objects: %s", err.Error())
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}

	log.Info("successfully retrieved detailed list by prefix:", prefix)
	return c.JSON(http.StatusOK, results)
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

// GetListWithCallBack is the same as GetList, except instead of returning the entire list at once, it gives you the option of processing page by page
// this method is safer than GetList as it avoid memory overload for large datasets since it does not store the entire list in memory but rather processes it on the go.
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
