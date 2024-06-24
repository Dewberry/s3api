package blobstore

// Not implemented

import (
	"fmt"
	"sort"

	"github.com/Dewberry/s3api/configberry"
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
		return nil, fmt.Errorf("error listing buckets: %w", err)
	}
	return result, nil
}

func (bh *BlobHandler) HandleListBuckets(c echo.Context) error {
	var allBuckets []BucketInfo

	bh.Mu.Lock()
	defer bh.Mu.Unlock()

	// Check user's overall read access level
	_, fullAccess, appErr := bh.getUserS3ReadListPermission(c, "")
	if appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, true))
		return configberry.HandleErrorResponse(c, appErr)
	}

	for _, controller := range bh.S3Controllers {
		if bh.AllowAllBuckets {
			result, err := controller.ListBuckets()
			if err != nil {
				appErr := configberry.HandleAWSError(err, "error retunring list of buckets")
				log.Error(configberry.LogErrorFormatter(appErr, true))
				return configberry.HandleErrorResponse(c, appErr)
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
				permissions, _, appErr := bh.getUserS3ReadListPermission(c, bucket)
				if appErr != nil {
					log.Error(configberry.LogErrorFormatter(appErr, true))
					return configberry.HandleErrorResponse(c, appErr)
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
	return configberry.HandleSuccessfulResponse(c, allBuckets)
}
