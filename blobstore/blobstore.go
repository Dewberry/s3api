package blobstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Dewberry/s3api/configberry"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

func arrayContains(a string, arr []string) bool {
	for _, b := range arr {
		if b == a {
			return true
		}
	}
	return false
}

func isIdenticalArray(array1, array2 []string) bool {
	if len(array1) != len(array2) {
		return false
	}

	set := make(map[string]bool)

	for _, str := range array1 {
		set[str] = true
	}

	for _, str := range array2 {
		if !set[str] {
			return false
		}
	}

	return true
}

// isPermittedPrefix checks if the prefix is within the user's permissions.
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

// checkAndAdjustPrefix checks if the prefix is an object and adjusts the prefix accordingly.
// Returns the adjusted prefix, an error message (if any), and the HTTP status code.
// Methods defined on `S3Ctrl` that return return a ConfigBerry `AppError`
func (s3Ctrl *S3Controller) checkAndAdjustPrefix(bucket, prefix string) (string, *configberry.AppError) {
	// As of 6/12/24, unsure why ./ is included here, may be needed for an edge case, but could also cause problems
	if prefix != "" && prefix != "./" && prefix != "/" {
		isObject, err := s3Ctrl.KeyExists(bucket, prefix)
		if err != nil {
			return "", configberry.HandleAWSError(err, "error checking if prefix is an object")
		}
		if isObject {
			objMeta, err := s3Ctrl.GetMetaData(bucket, prefix)
			if err != nil {
				return "", configberry.HandleAWSError(err, "error checking for object's metadata")
			}
			// This is because AWS considers empty prefixes with a .keep as an object, so we ignore and log
			if *objMeta.ContentLength == 0 {
				log.Infof("detected a zero byte directory marker within prefix: %s", prefix)
			} else {
				return "", configberry.NewAppError(configberry.TeapotError, fmt.Sprintf("`%s` is an object, not a prefix", prefix), nil)
			}
		}
		prefix = strings.Trim(prefix, "/") + "/"
	}
	return prefix, nil
}

func validateEnvJSON(filePath string) error {
	// Read the contents of the .env.json file
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading `.env.json`: %s", err.Error())
	}

	// Parse the JSON data into the AWSConfig struct
	var awsConfig AWSConfig
	if err := json.Unmarshal(jsonData, &awsConfig); err != nil {
		return fmt.Errorf("error parsing `.env.json`: %s", err.Error())
	}

	// Check if there is at least one account defined
	if len(awsConfig.Accounts) == 0 {
		return errors.New("no AWS accounts defined in `.env.json`")
	}

	// Check if each account has the required fields
	for i, account := range awsConfig.Accounts {
		missingFields := []string{}
		if account.AWS_ACCESS_KEY_ID == "" {
			missingFields = append(missingFields, "AWS_ACCESS_KEY_ID")
		}
		if account.AWS_SECRET_ACCESS_KEY == "" {
			missingFields = append(missingFields, "AWS_SECRET_ACCESS_KEY")
		}

		if len(missingFields) > 0 {
			return fmt.Errorf("missing fields (%s) for AWS account %d in envJson file", strings.Join(missingFields, ", "), i+1)
		}
	}
	if len(awsConfig.BucketAllowList) == 0 {
		return fmt.Errorf("no buckets in the `bucket_allow_list`, please provide required buckets, or `*` for access to all buckets")
	}
	// If all checks pass, return nil (no error)
	return nil
}

func newAWSConfig(envJson string) (AWSConfig, error) {
	var awsConfig AWSConfig
	err := validateEnvJSON(envJson)
	if err != nil {
		return awsConfig, fmt.Errorf(err.Error())
	}
	jsonData, err := os.ReadFile(envJson)
	if err != nil {
		return awsConfig, err
	}

	if err := json.Unmarshal(jsonData, &awsConfig); err != nil {
		return awsConfig, err
	}
	return awsConfig, nil
}

func newMinioConfig() MinioConfig {
	var mc MinioConfig
	mc.S3Endpoint = os.Getenv("MINIO_S3_ENDPOINT")
	mc.DisableSSL = os.Getenv("MINIO_S3_DISABLE_SSL")
	mc.ForcePathStyle = os.Getenv("MINIO_S3_FORCE_PATH_STYLE")
	mc.AccessKeyID = os.Getenv("MINIO_ACCESS_KEY_ID")
	mc.Bucket = os.Getenv("AWS_S3_BUCKET")
	mc.SecretAccessKey = os.Getenv("MINIO_SECRET_ACCESS_KEY")
	return mc
}

func GetListSize(page *s3.ListObjectsV2Output, totalSize *uint64, fileCount *uint64) error {
	if page == nil {
		return errors.New("input page is nil")
	}

	for _, file := range page.Contents {
		if file.Size == nil {
			return errors.New("file size is nil")
		}
		*totalSize += uint64(*file.Size)
		*fileCount++
	}

	return nil
}

//repetitive errors refactored:

const unableToGetController string = "unable to get `s3controller`"
const parameterKeyRequired string = "parameter `key` is required"
const parameterPrefixRequired string = "parameter `prefix` is required"
const parseingBodyRequestError string = "error parsing request body"
const parsingDelimeterParamError string = "error parsing `delimiter` param"
const listingObjectsAndPrefixError string = "error listing objects and common prefixes"
