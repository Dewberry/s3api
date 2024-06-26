//go:build test
// +build test

package blobstore

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	// Set environment variables for testing
	os.Setenv("AUTH_LIMITED_WRITER_ROLE", "writer_role")
	os.Setenv("AUTH_LIMITED_READER_ROLE", "reader_role")
	os.Setenv("TEMP_PREFIX", "custom-temp")
	os.Setenv("DOWNLOAD_URL_EXP_DAYS", "10")
	os.Setenv("UPLOAD_URL_EXP_MIN", "30")
	os.Setenv("SCRIPT_DOWNLOAD_SIZE_LIMIT", "60")
	os.Setenv("ZIP_DOWNLOAD_SIZE_LIMIT", "10")

	// Reset environment variables after the test
	defer func() {
		os.Unsetenv("AUTH_LIMITED_WRITER_ROLE")
		os.Unsetenv("AUTH_LIMITED_READER_ROLE")
		os.Unsetenv("TEMP_PREFIX")
		os.Unsetenv("DOWNLOAD_URL_EXP_DAYS")
		os.Unsetenv("UPLOAD_URL_EXP_MIN")
		os.Unsetenv("SCRIPT_DOWNLOAD_SIZE_LIMIT")
		os.Unsetenv("ZIP_DOWNLOAD_SIZE_LIMIT")
	}()

	config := newConfig(1)

	require.Equal(t, 1, config.AuthLevel)
	require.Equal(t, "writer_role", config.LimitedWriterRoleName)
	require.Equal(t, "reader_role", config.LimitedReaderRoleName)
	require.Equal(t, "custom-temp", config.DefaultTempPrefix)
	require.Equal(t, 10, config.DefaultDownloadPresignedUrlExpiration)
	require.Equal(t, 30, config.DefaultUploadPresignedUrlExpiration)
	require.Equal(t, 60, config.DefaultScriptDownloadSizeLimit)
	require.Equal(t, 10, config.DefaultZipDownloadSizeLimit)
}

func TestGetEnvOrDefault(t *testing.T) {
	os.Setenv("TEST_ENV_VAR", "test_value")
	defer os.Unsetenv("TEST_ENV_VAR")

	result := getEnvOrDefault("TEST_ENV_VAR", "default_value")
	require.Equal(t, "test_value", result)

	result = getEnvOrDefault("NON_EXISTENT_ENV_VAR", "default_value")
	require.Equal(t, "default_value", result)
}

func TestGetIntEnvOrDefault(t *testing.T) {
	os.Setenv("TEST_INT_ENV_VAR", "20")
	defer os.Unsetenv("TEST_INT_ENV_VAR")

	result := getIntEnvOrDefault("TEST_INT_ENV_VAR", 10)
	require.Equal(t, 20, result)

	result = getIntEnvOrDefault("NON_EXISTENT_INT_ENV_VAR", 10)
	require.Equal(t, 10, result)

	os.Setenv("INVALID_INT_ENV_VAR", "invalid")
	defer os.Unsetenv("INVALID_INT_ENV_VAR")

	hook := test.NewGlobal()
	defer hook.Reset()

	result = getIntEnvOrDefault("INVALID_INT_ENV_VAR", 10)
	require.Equal(t, 10, result)

	// Check the log entry
	logEntry := hook.LastEntry()
	require.NotNil(t, logEntry)
	require.Equal(t, log.ErrorLevel, logEntry.Level)
	require.Contains(t, logEntry.Message, "error parsing INVALID_INT_ENV_VAR, defaulting to 10")
}

func TestMinIOSessionManager(t *testing.T) {
	mc := MinioConfig{
		S3Endpoint:      "http://localhost:9000",
		DisableSSL:      "true",
		ForcePathStyle:  "true",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		Bucket:          "test-bucket",
	}

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(mc.S3Endpoint),
		Region:           aws.String("us-east-1"),
		Credentials:      credentials.NewStaticCredentials(mc.AccessKeyID, mc.SecretAccessKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, sess)
}

func TestValidateMinioConfig(t *testing.T) {
	tests := []struct {
		config        MinioConfig
		expectedError string
	}{
		{
			config: MinioConfig{
				S3Endpoint:      "http://localhost:9000",
				DisableSSL:      "true",
				ForcePathStyle:  "true",
				AccessKeyID:     "minioadmin",
				SecretAccessKey: "minioadmin",
			},
			expectedError: "",
		},
		{
			config: MinioConfig{
				S3Endpoint:      "",
				DisableSSL:      "true",
				ForcePathStyle:  "true",
				AccessKeyID:     "minioadmin",
				SecretAccessKey: "minioadmin",
			},
			expectedError: "missing fields:  [\"S3Endpoint\"]",
		},
		{
			config: MinioConfig{
				S3Endpoint:      "http://localhost:9000",
				DisableSSL:      "",
				ForcePathStyle:  "true",
				AccessKeyID:     "minioadmin",
				SecretAccessKey: "minioadmin",
			},
			expectedError: "missing fields:  [\"DisableSSL\"]",
		},
	}

	for _, test := range tests {
		err := test.config.validateMinioConfig()
		if test.expectedError == "" {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, test.expectedError)
		}
	}
}

func TestAWSSessionManager(t *testing.T) {
	ac := AWSCreds{
		AWS_ACCESS_KEY_ID:     "test_access_key",
		AWS_SECRET_ACCESS_KEY: "test_secret_key",
		AWS_S3_BUCKET:         "test-bucket",
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(ac.AWS_ACCESS_KEY_ID, ac.AWS_SECRET_ACCESS_KEY, ""),
	})
	require.NoError(t, err)
	require.NotNil(t, sess)
}

func TestArrayContains(t *testing.T) {
	arr := []string{"a", "b", "c"}
	require.True(t, arrayContains("a", arr))
	require.False(t, arrayContains("d", arr))
}

func TestIsIdenticalArray(t *testing.T) {
	array1 := []string{"a", "b", "c"}
	array2 := []string{"c", "b", "a"}
	array3 := []string{"a", "b", "c"}
	array4 := []string{"a", "b"}

	require.True(t, isIdenticalArray(array1, array3))
	require.True(t, isIdenticalArray(array1, array2))
	require.False(t, isIdenticalArray(array1, array4))
}

func TestIsPermittedPrefix(t *testing.T) {
	bucket := "test-bucket"
	prefix := "test-prefix"
	permissions := []string{"/test-bucket/test-prefix/", "/test-bucket/another-prefix/"}

	require.True(t, isPermittedPrefix(bucket, prefix, permissions))
	require.False(t, isPermittedPrefix(bucket, "non-permitted-prefix", permissions))
}

func TestValidateEnvJSON(t *testing.T) {
	validJSON := `{
		"accounts": [
			{
				"AWS_ACCESS_KEY_ID": "test_access_key",
				"AWS_SECRET_ACCESS_KEY": "test_secret_key"
			}
		],
		"bucket_allow_list": ["test-bucket"]
	}`

	invalidJSON := `{
		"accounts": [
			{
				"AWS_ACCESS_KEY_ID": "test_access_key"
			}
		],
		"bucket_allow_list": []
	}`

	// Write the valid JSON to a file
	os.WriteFile("valid.env.json", []byte(validJSON), 0644)
	defer os.Remove("valid.env.json")

	// Write the invalid JSON to a file
	os.WriteFile("invalid.env.json", []byte(invalidJSON), 0644)
	defer os.Remove("invalid.env.json")

	require.NoError(t, validateEnvJSON("valid.env.json"))
	require.Error(t, validateEnvJSON("invalid.env.json"))
}

func TestNewAWSConfig(t *testing.T) {
	validJSON := `{
		"accounts": [
			{
				"AWS_ACCESS_KEY_ID": "test_access_key",
				"AWS_SECRET_ACCESS_KEY": "test_secret_key"
			}
		],
		"bucket_allow_list": ["test-bucket"]
	}`

	// Write the valid JSON to a file
	os.WriteFile("valid.env.json", []byte(validJSON), 0644)
	defer os.Remove("valid.env.json")

	awsConfig, err := newAWSConfig("valid.env.json")
	require.NoError(t, err)
	require.NotNil(t, awsConfig)
	require.Equal(t, "test_access_key", awsConfig.Accounts[0].AWS_ACCESS_KEY_ID)
}

func TestNewMinioConfig(t *testing.T) {
	os.Setenv("MINIO_S3_ENDPOINT", "http://localhost:9000")
	os.Setenv("MINIO_S3_DISABLE_SSL", "true")
	os.Setenv("MINIO_S3_FORCE_PATH_STYLE", "true")
	os.Setenv("MINIO_ACCESS_KEY_ID", "minio_access_key")
	os.Setenv("MINIO_SECRET_ACCESS_KEY", "minio_secret_key")
	os.Setenv("AWS_S3_BUCKET", "test-bucket")

	minioConfig := newMinioConfig()

	require.Equal(t, "http://localhost:9000", minioConfig.S3Endpoint)
	require.Equal(t, "true", minioConfig.DisableSSL)
	require.Equal(t, "true", minioConfig.ForcePathStyle)
	require.Equal(t, "minio_access_key", minioConfig.AccessKeyID)
	require.Equal(t, "minio_secret_key", minioConfig.SecretAccessKey)
	require.Equal(t, "test-bucket", minioConfig.Bucket)
}

func TestGetListSize(t *testing.T) {
	page := &s3.ListObjectsV2Output{
		Contents: []*s3.Object{
			{Size: aws.Int64(1024)},
			{Size: aws.Int64(2048)},
		},
	}

	var totalSize uint64
	var fileCount uint64

	err := GetListSize(page, &totalSize, &fileCount)
	require.NoError(t, err)
	require.Equal(t, uint64(3072), totalSize)
	require.Equal(t, uint64(2), fileCount)
}

func TestGetListSizeNilPage(t *testing.T) {
	var totalSize uint64
	var fileCount uint64

	err := GetListSize(nil, &totalSize, &fileCount)
	require.Error(t, err)
	require.Equal(t, "input page is nil", err.Error())
}

func TestGetListSizeNilFileSize(t *testing.T) {
	page := &s3.ListObjectsV2Output{
		Contents: []*s3.Object{
			{Size: nil},
		},
	}

	var totalSize uint64
	var fileCount uint64

	err := GetListSize(page, &totalSize, &fileCount)
	require.Error(t, err)
	require.Equal(t, "file size is nil", err.Error())
}
