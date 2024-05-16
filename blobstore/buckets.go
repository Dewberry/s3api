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

// func (bh *BlobHandler) createBucket(bucketName string) error {
// 	// Set up input parameters for the CreateBucket API
// 	input := &s3.CreateBucketInput{
// 		Bucket: aws.String(bucketName),
// 	}

// 	// Create the bucket
// 	_, err := bh.S3Svc.CreateBucket(input)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// // deleteBucket deletes the specified S3 bucket.
// func (bh *BlobHandler) deleteBucket(bucketName string) error {
// 	// Set up input parameters for the DeleteBucket API
// 	input := &s3.DeleteBucketInput{
// 		Bucket: aws.String(bucketName),
// 	}

// 	// Delete the bucket
// 	_, err := bh.S3Svc.DeleteBucket(input)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// // getBucketACL retrieves the ACL (Access Control List) for the specified bucket.
// func (bh *BlobHandler) getBucketACL(bucketName string) (*s3.GetBucketAclOutput, error) {
// 	// Set up input parameters for the GetBucketAcl API
// 	input := &s3.GetBucketAclInput{
// 		Bucket: aws.String(bucketName),
// 	}

// 	// Get the bucket ACL
// 	result, err := bh.S3Svc.GetBucketAcl(input)
// 	if err != nil {
// 		return nil, err
// 	}

//		return result, nil
//	}

type BucketInfo struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	CanRead bool   `json:"can_read"`
}

func (bh *BlobHandler) HandleListBuckets(c echo.Context) error {
	var allBuckets []BucketInfo

	bh.Mu.Lock()
	defer bh.Mu.Unlock()

	fullAccess := false

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
			permissions, fullAccessTmp, err := bh.GetUserS3ReadListPermission(c, bucket)
			if err != nil {
				return c.JSON(http.StatusInternalServerError, fmt.Errorf("error fetching user permissions: %s", err.Error()))
			}
			fullAccess = fullAccess || fullAccessTmp // Update full access based on any bucket returning full access

			canRead := len(permissions) > 0 || fullAccessTmp // Set canRead based on permissions or full access
			allBuckets = append(allBuckets, BucketInfo{
				ID:      i,
				Name:    bucket,
				CanRead: canRead,
			})
		}
	}

	if fullAccess { // If full access is true, set CanRead to true for all buckets
		for i := range allBuckets {
			allBuckets[i].CanRead = true
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

// func (bh *BlobHandler) HandleCreateBucket(c echo.Context) error {
// 	bucketName := c.QueryParam("name")

// 	if bucketName == "" {
// 		err := errors.New("request must include a `name` parameter")
// 		log.Info("HandleCreateBucket: " + err.Error())
// 		return c.JSON(http.StatusBadRequest, err.Error())
// 	}

// 	// Check if the bucket already exists
// 	buckets, err := bh.listBuckets()
// 	if err != nil {
// 		log.Info("HandleCreateBucket: Error listing buckets:", err.Error())
// 		return c.JSON(http.StatusInternalServerError, err.Error())
// 	}

// 	for _, b := range buckets.Buckets {
// 		if aws.StringValue(b.Name) == bucketName {
// 			err := fmt.Errorf("bucket with the name `%s` already exists", bucketName)
// 			log.Info("HandleCreateBucket: " + err.Error())
// 			return c.JSON(http.StatusConflict, err.Error())
// 		}
// 	}

// 	// Create the S3 bucket
// 	err = bh.createBucket(bucketName)
// 	if err != nil {
// 		log.Info("HandleCreateBucket: Error creating bucket:", err.Error())
// 		return c.JSON(http.StatusInternalServerError, err.Error())
// 	}

// 	log.Info("HandleCreateBucket: Successfully created bucket:", bucketName)
// 	return c.JSON(http.StatusOK, "Bucket created successfully")
// }

// func (bh *BlobHandler) HandleDeleteBucket(c echo.Context) error {
// 	bucketName := c.QueryParam("name")

// 	if bucketName == "" {
// 		err := errors.New("request must include a `name` parameter")
// 		log.Info("HandleDeleteBucket: " + err.Error())
// 		return c.JSON(http.StatusBadRequest, err.Error())
// 	}

// 	// Delete the S3 bucket
// 	err := bh.deleteBucket(bucketName)
// 	if err != nil {
// 		log.Info("HandleDeleteBucket: Error deleting bucket:", err.Error())
// 		return c.JSON(http.StatusInternalServerError, err.Error())
// 	}

// 	log.Info("HandleDeleteBucket: Successfully deleted bucket:", bucketName)
// 	return c.JSON(http.StatusOK, "Bucket deleted successfully")
// }

// func (bh *BlobHandler) HandleGetBucketACL(c echo.Context) error {
// 	bucketName := c.QueryParam("name")

// 	if bucketName == "" {
// 		err := errors.New("request must include a `name` parameter")
// 		log.Info("HandleGetBucketACL: " + err.Error())
// 		return c.JSON(http.StatusBadRequest, err.Error())
// 	}

// 	// Get the bucket ACL
// 	acl, err := bh.getBucketACL(bucketName)
// 	if err != nil {
// 		log.Info("HandleGetBucketACL: Error getting bucket ACL:", err.Error())
// 		return c.JSON(http.StatusInternalServerError, err.Error())
// 	}

// 	log.Info("HandleGetBucketACL: Successfully retrieved ACL for bucket:", bucketName)
// 	return c.JSON(http.StatusOK, acl)
// }
