package blobstore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/labstack/gommon/log"
)

const (
	TEMP_PREFIX  string = "downloads-temp" // temp prefix prepended to targzs created by HandleGetPresignedURLMultiObj
	URL_EXP_DAYS int    = 7                // default validity period (expiration in days) for urls made in HandleGetPresignedURLMultiObj
)

func (bh *BlobHandler) keyExists(bucket string, key string) (bool, error) {
	_, err := bh.S3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
				return false, nil
			default:
				return false, err
			}
		}
		return false, err
	}
	return true, nil
}

func (bh *BlobHandler) getSize(list *s3.ListObjectsV2Output) (uint64, uint32, error) {
	var size uint64 = 0
	fileCount := uint32(len(list.Contents))

	for _, file := range list.Contents {
		size += uint64(*file.Size)
	}

	return size, fileCount, nil
}

// getList returns the list of object keys in the specified S3 bucket with the given prefix.
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

func listDir(bucket, key string, svc *s3.S3) (*[]ListResult, error) {
	s3Path := strings.Trim(key, "/") + "/"

	query := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(s3Path),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(1000),
	}

	result := []ListResult{}
	truncatedListing := true
	var count int
	for truncatedListing {

		resp, err := svc.ListObjectsV2(query)
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

func listRoot(bucket string, svc *s3.S3) (*[]ListResult, error) {
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(""),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(1000),
	}

	result := []ListResult{}
	truncatedListing := true
	var count int
	for truncatedListing {

		resp, err := svc.ListObjectsV2(params)
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

		params.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}

	return &result, nil
}

const (
	chars       = "abcdefghijklmnopqrstuvwxyz"
	randomChars = 6
)

func GenerateRandomString() string {
	rand.Seed(time.Now().UnixNano())

	b := make([]byte, randomChars)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}

	return string(b)
}

func getPresignedURL(bucket, key string, expDays int) (string, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := s3.New(sess)

	duration := time.Duration(expDays) * 24 * time.Hour
	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(duration)
}

func (bh *BlobHandler) tarS3Files(r *s3.ListObjectsV2Output, bucket string, outputFile string, prefix string) (err error) {
	uploader := s3manager.NewUploader(bh.Sess)
	pr, pw := io.Pipe()

	gzipWriter := gzip.NewWriter(pw)
	tarWriter := tar.NewWriter(gzipWriter)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		log.Debug("start writing files to:", outputFile)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(outputFile),
			Body:   pr,
		})
		if err != nil {
			log.Error("Failed to upload tar.gz file to S3:", err)
			return
		}
		log.Debug("completed writing files to:", outputFile)
	}()

	for _, item := range r.Contents {
		filePath := filepath.Join(strings.TrimPrefix(aws.StringValue(item.Key), prefix))
		copyObj := aws.StringValue(item.Key)
		log.Infof("Copying %s to %s", copyObj, outputFile)

		getResp, err := bh.S3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(copyObj),
		})
		if err != nil {
			log.Error("Failed to download file:", copyObj)
			return err
		}
		defer getResp.Body.Close()

		header := &tar.Header{
			Name: filePath,
			Size: *getResp.ContentLength,
			Mode: int64(0644),
		}

		err = tarWriter.WriteHeader(header)
		if err != nil {
			log.Error("Failed to write tar header for file:", copyObj)
			return err
		}

		_, err = io.Copy(tarWriter, getResp.Body)
		if err != nil {
			log.Error("Failed to write file content to tar:", copyObj)
			return err
		}
		log.Debug("Complete copying...", copyObj)
	}

	err = tarWriter.Close()
	if err != nil {
		log.Error("tar close failure")
		return err
	}

	gzipWriter.Close()
	if err != nil {
		log.Error("gzip close failure")
		return err
	}

	pw.Close()
	if err != nil {
		log.Error("pw close failure")
		return err
	}

	wg.Wait()

	log.Info("Done!")
	return nil
}

// func getFileModificationTime(filePath string) int64 {
// 	fi2, err := os.Open(filePath)
// 	fi, err := os.Stat(filePath)
// 	fmt.Println(fi2)
// 	if err != nil {
// 		return 0
// 	}
// 	return fi.ModTime().Unix()
// }

func RecursivelyDeleteObjects(client *s3.S3, bucket, folderPath string) error {
	s3Path := strings.Trim(folderPath, "/") + "/"
	if strings.Count(s3Path, "/") < 3 {
		return fmt.Errorf("prefix %q too shallow, must be at least 2 levels deep, e.g. 'abc/def/'", folderPath)
	}

	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(s3Path),
	}
	resp, err := client.ListObjectsV2(query)
	if err != nil {
		return err
	}
	if len(resp.Contents) > 0 {
		var objectsToDelete []*s3.ObjectIdentifier

		for _, obj := range resp.Contents {
			objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		if len(objectsToDelete) > 0 {
			_, err = client.DeleteObjects(&s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3.Delete{
					Objects: objectsToDelete,
				},
			})

			if err != nil {
				return err
			}
		}
	} else {
		return errors.New("object not found and no objects were deleted")
	}

	return nil
}

func (bh *BlobHandler) UploadS3Obj(bucket string, key string, body io.ReadCloser) error {
	// Initialize the multipart upload to S3
	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	resp, err := bh.S3Svc.CreateMultipartUpload(params)
	if err != nil {
		return fmt.Errorf("error initializing multipart upload. %s", err.Error())
	}

	// Create the variables that will track upload progress
	var totalBytes int64 = 0
	var partNumber int64 = 1
	completedParts := []*s3.CompletedPart{}
	buffer := bytes.NewBuffer(nil)

	for {
		// Read from the request body into the buffer
		chunkSize := 1024 * 1024 * 5
		buf := make([]byte, chunkSize)
		n, err := body.Read(buf)

		// This would be a true error while reading
		if err != nil && err != io.EOF {
			return fmt.Errorf("error copying POST body to S3. %s", err.Error())
		}

		// Add the buffer data to the buffer
		buffer.Write(buf[:n])

		// Upload a part if the buffer contains more than 5mb of data to avoid AWS EntityTooSmall error
		if buffer.Len() > chunkSize {
			params := &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   resp.UploadId,
				PartNumber: aws.Int64(partNumber),
				Body:       bytes.NewReader(buffer.Bytes()),
			}

			result, err := bh.S3Svc.UploadPart(params)
			if err != nil {
				return fmt.Errorf("error streaming POST body to S3. %s, %+v", err.Error(), result)
			}

			totalBytes += int64(buffer.Len())
			completedParts = append(completedParts, &s3.CompletedPart{
				ETag:       result.ETag,
				PartNumber: aws.Int64(partNumber),
			})

			buffer.Reset()
			partNumber++
		}

		if err == io.EOF {
			break
		}
	}

	// Upload the remaining data as the last part
	params2 := &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   resp.UploadId,
		PartNumber: aws.Int64(partNumber),
		Body:       bytes.NewReader(buffer.Bytes()),
	}

	result, err := bh.S3Svc.UploadPart(params2)
	if err != nil {
		return fmt.Errorf("error streaming POST body to S3. %s, %+v", err.Error(), result)
	}

	totalBytes += int64(buffer.Len())
	completedParts = append(completedParts, &s3.CompletedPart{
		ETag:       result.ETag,
		PartNumber: aws.Int64(partNumber),
	})

	// Complete the multipart upload
	completeParams := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
	}
	_, err = bh.S3Svc.CompleteMultipartUpload(completeParams)
	if err != nil {
		return fmt.Errorf("error completing multipart upload. %s", err.Error())
	}

	return nil
}
