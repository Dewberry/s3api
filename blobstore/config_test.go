//go:build test
// +build test

package blobstore

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
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
