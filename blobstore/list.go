package blobstore

import (
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

// HandleListByPrefix handles the API endpoint for listing objects by prefix in S3 bucket.
//it will handle two endpoints one that returns a list without detail for /prefix/list and one
//that returns a list with additional details for /prefix/list_with_detail
func (bh *BlobHandler) HandleListByPrefix(c echo.Context) error {
	prefix := c.QueryParam("prefix")
	if prefix == "" {
		err := errors.New("request must include a `prefix` parameter")
		log.Error("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
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
	bucket, err := getBucketParam(c, bh.Bucket)
	if err != nil {
		log.Error("HandleListByPrefix: " + err.Error())
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}
	var result interface{}
	isDetail := strings.Contains(c.Request().URL.String(), "list_with_details")
	log.Debug("list_with_detail parameter  bool value:", isDetail)
	if !isDetail {
		listOutput, err := bh.getList(bucket, prefix, delimiter)
		if err != nil {
			log.Error("HandleListByPrefix: Error getting list:", err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

		// Convert the list of object keys to strings
		var objectKeys []string
		for _, object := range listOutput.Contents {
			objectKeys = append(objectKeys, aws.StringValue(object.Key))
		}
		result = objectKeys
	} else {
		detailedList, err := bh.listDir(bucket, prefix, delimiter)
		if err != nil {
			log.Error("HandleListByPrefix: Error getting list_with_detail:", err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		result = detailedList
	}

	log.Info("HandleListByPrefix: Successfully retrieved list by prefix:", prefix)
	return c.JSON(http.StatusOK, result)
}

// getList retrieves a list of objects in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) getList(bucket, prefix string, delimiter bool) (*s3.ListObjectsV2Output, error) {
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
	err := bh.S3Svc.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
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

// ListResult is the result struct for listing objects with additional details.
type ListResult struct {
	ID         int       `json:"id"`
	Name       string    `json:"filename"`
	Size       string    `json:"size"`
	Path       string    `json:"filepath"`
	Type       string    `json:"type"`
	IsDir      bool      `json:"isdir"`
	Modified   time.Time `json:"modified"`
	ModifiedBy string    `json:"modified_by"`
}

// listDir retrieves a detailed list of objects in the specified S3 bucket with the given prefix.
func (bh *BlobHandler) listDir(bucket, prefix string, delimiter bool) (*[]ListResult, error) {
	if prefix != "" {
		prefix = strings.Trim(prefix, "/") + "/"
	}
	query := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000),
	}
	if delimiter {
		query.SetDelimiter("/")
	}
	result := []ListResult{}
	truncatedListing := true
	var count int
	for truncatedListing {

		resp, err := bh.S3Svc.ListObjectsV2(query)
		if err != nil {
			return nil, err
		}

		for _, cp := range resp.CommonPrefixes {
			w := ListResult{
				ID:         count,
				Name:       filepath.Base(*cp.Prefix),
				Size:       "",
				Path:       *cp.Prefix,
				Type:       "",
				IsDir:      true,
				ModifiedBy: "",
			}
			count++
			result = append(result, w)
		}

		for _, object := range resp.Contents {
			parts := strings.Split(filepath.Dir(*object.Key), "/")
			isSelf := filepath.Base(*object.Key) == parts[len(parts)-1]

			if !isSelf {
				w := ListResult{
					ID:         count,
					Name:       filepath.Base(*object.Key),
					Size:       strconv.FormatInt(*object.Size, 10),
					Path:       filepath.Dir(*object.Key),
					Type:       filepath.Ext(*object.Key),
					IsDir:      false,
					Modified:   *object.LastModified,
					ModifiedBy: "",
				}

				count++
				result = append(result, w)
			}
		}

		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}

	return &result, nil
}
