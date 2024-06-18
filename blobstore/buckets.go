package blobstore

// Not implemented

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

type BucketInfo struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	CanRead bool   `json:"can_read"`
}

// listBuckets returns the list of all S3 buckets.
func (s3Ctrl *S3Controller) ListBuckets() (*s3.ListBucketsOutput, error) {
	// Set up input parameters for the ListBuckets API
	var result *s3.ListBucketsOutput
	var err error
	input := &s3.ListBucketsInput{}

	// Retrieve the list of buckets
	result, err = s3Ctrl.S3Svc.ListBuckets(input)
	if err != nil {
		errMsg := fmt.Errorf("failed to call ListBuckets: %s", err.Error())
		return nil, errMsg
	}
	return result, nil
}

func (bh *BlobHandler) HandleListBuckets(c echo.Context) error {
	var allBuckets []BucketInfo

	bh.Mu.Lock()
	defer bh.Mu.Unlock()

	// Check user's overall read access level
	_, fullAccess, err := bh.getUserS3ReadListPermission(c, "")
	if err != nil {
		return c.JSON(http.StatusInternalServerError, fmt.Errorf("error fetching user permissions: %s", err.Error()))
	}

	for _, controller := range bh.S3Controllers {
		if bh.AllowAllBuckets {
			result, err := controller.ListBuckets()
			if err != nil {
				errMsg := fmt.Errorf("error returning list of buckets, error: %s", err)
				log.Error(errMsg)
				return c.JSON(http.StatusInternalServerError, errMsg)
			}
			var mostRecentBucketList []string
			for _, b := range result.Buckets {
				mostRecentBucketList = append(mostRecentBucketList, *b.Name)
			}
			if !isIdenticalArray(controller.Buckets, mostRecentBucketList) {
				controller.Buckets = mostRecentBucketList
			}
		}

		// Extract the bucket names from the response and append to allBuckets
		for i, bucket := range controller.Buckets {
			canRead := fullAccess
			if !fullAccess {
				permissions, _, err := bh.getUserS3ReadListPermission(c, bucket)
				if err != nil {
					return c.JSON(http.StatusInternalServerError, fmt.Errorf("error fetching user permissions: %s", err.Error()))
				}
				canRead = len(permissions) > 0
			}
			allBuckets = append(allBuckets, BucketInfo{
				ID:      i,
				Name:    bucket,
				CanRead: canRead,
			})
		}
	}

	// Sorting allBuckets slice by CanRead true first and then by Name field alphabetically
	sort.Slice(allBuckets, func(i, j int) bool {
		if allBuckets[i].CanRead == allBuckets[j].CanRead {
			return allBuckets[i].Name < allBuckets[j].Name
		}
		return allBuckets[i].CanRead && !allBuckets[j].CanRead
	})

	log.Info("Successfully retrieved list of buckets")

	return c.JSON(http.StatusOK, allBuckets)
}
