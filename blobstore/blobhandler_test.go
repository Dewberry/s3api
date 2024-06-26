//go:build test
// +build test

package blobstore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func setupEnvVariables(vars map[string]string) {
	for key, value := range vars {
		os.Setenv(key, value)
	}
}

func teardownEnvVariables(vars map[string]string) {
	for key := range vars {
		os.Unsetenv(key)
	}
}

func TestNewBlobHandlerAuthLevel(t *testing.T) {
	envVars := map[string]string{
		"AUTH_LIMITED_WRITER_ROLE": "writer_role",
		"USE_MOCK_DB":              "true",
		"USE_MOCK_CHECK_ENV":       "true",
		"S3_MOCK":                  "0",
		"USE_MOCK_AWS":             "true",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	envJsonContent := `{
        "accounts": [
            {
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET": "test-bucket"
            }
        ],
        "bucket_allow_list": ["test-bucket-1", "test-bucket-2"]
    }`
	envJsonFile, err := os.CreateTemp("", "env.json")
	require.NoError(t, err)
	defer os.Remove(envJsonFile.Name())
	_, err = envJsonFile.Write([]byte(envJsonContent))
	require.NoError(t, err)
	envJsonFile.Close()

	handler, err := NewBlobHandler(envJsonFile.Name(), 1)
	require.NoError(t, err)
	require.NotNil(t, handler)
	require.Equal(t, 1, handler.Config.AuthLevel)
	require.NotNil(t, handler.DB)
}

func TestNewBlobHandlerAWSS3(t *testing.T) {
	envVars := map[string]string{
		"S3_MOCK":      "0",
		"USE_MOCK_AWS": "true",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	envJsonContent := `{
        "accounts": [
            {
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET": "test-bucket"
            }
        ],
        "bucket_allow_list": ["test-bucket-1", "test-bucket-2"]
    }`
	envJsonFile, err := os.CreateTemp("", "env.json")
	require.NoError(t, err)
	defer os.Remove(envJsonFile.Name())
	_, err = envJsonFile.Write([]byte(envJsonContent))
	require.NoError(t, err)
	envJsonFile.Close()

	handler, err := NewBlobHandler(envJsonFile.Name(), 0)
	require.NoError(t, err)
	require.NotNil(t, handler)
	require.Len(t, handler.S3Controllers, 1)
	require.False(t, handler.S3Controllers[0].S3Mock)
}

func TestNewBlobHandlerMinIO(t *testing.T) {
	envVars := map[string]string{
		"S3_MOCK":                   "1",
		"MINIO_S3_ENDPOINT":         "http://localhost:9000",
		"MINIO_ACCESS_KEY_ID":       "minio_access_key",
		"MINIO_SECRET_ACCESS_KEY":   "minio_secret_key",
		"MINIO_S3_DISABLE_SSL":      "true",
		"MINIO_S3_FORCE_PATH_STYLE": "true",
		"AWS_S3_BUCKET":             "test-bucket",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	handler, err := NewBlobHandler("", 0)
	require.NoError(t, err)
	require.NotNil(t, handler)
	require.Len(t, handler.S3Controllers, 1)
	require.True(t, handler.S3Controllers[0].S3Mock)
}

func TestNewBlobHandlerAWSS3Error(t *testing.T) {
	envVars := map[string]string{
		"S3_MOCK":      "0",
		"USE_MOCK_AWS": "true",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	envJsonContent := `{`
	envJsonFile, err := os.CreateTemp("", "env.json")
	require.NoError(t, err)
	defer os.Remove(envJsonFile.Name())
	_, err = envJsonFile.Write([]byte(envJsonContent))
	require.NoError(t, err)
	envJsonFile.Close()

	handler, err := NewBlobHandler(envJsonFile.Name(), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "`env.json` credentials extraction failed")
	require.NotNil(t, handler)
}

func TestNewBlobHandlerMissingEnvVars(t *testing.T) {
	envVars := map[string]string{
		"S3_MOCK":      "0",
		"USE_MOCK_AWS": "true",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	envJsonContent := `{
        "accounts": [
            {
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET": "test-bucket"
            }
        ],
        "bucket_allow_list": ["test-bucket-1", "test-bucket-2"]
    }`
	envJsonFile, err := os.CreateTemp("", "env.json")
	require.NoError(t, err)
	defer os.Remove(envJsonFile.Name())
	_, err = envJsonFile.Write([]byte(envJsonContent))
	require.NoError(t, err)
	envJsonFile.Close()

	handler, err := NewBlobHandler(envJsonFile.Name(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "environment variables are missing")
	require.NotNil(t, handler)
}

func TestNewBlobHandlerMixedConfigurations(t *testing.T) {
	envVars := map[string]string{
		"AUTH_LIMITED_WRITER_ROLE":  "writer_role",
		"USE_MOCK_DB":               "true",
		"USE_MOCK_CHECK_ENV":        "true",
		"S3_MOCK":                   "1",
		"MINIO_S3_ENDPOINT":         "http://localhost:9000",
		"MINIO_ACCESS_KEY_ID":       "minio_access_key",
		"MINIO_SECRET_ACCESS_KEY":   "minio_secret_key",
		"MINIO_S3_DISABLE_SSL":      "true",
		"MINIO_S3_FORCE_PATH_STYLE": "true",
		"AWS_S3_BUCKET":             "test-bucket",
		"USE_MOCK_AWS":              "true",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	handler, err := NewBlobHandler("", 1)
	require.NoError(t, err)
	require.NotNil(t, handler)
	require.Equal(t, 1, handler.Config.AuthLevel)
	require.NotNil(t, handler.DB)
	require.Len(t, handler.S3Controllers, 1)
	require.True(t, handler.S3Controllers[0].S3Mock)
}

func TestNewBlobHandlerMissingEnvJson(t *testing.T) {
	envVars := map[string]string{
		"S3_MOCK":      "0",
		"USE_MOCK_AWS": "true",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	handler, err := NewBlobHandler("", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "open : no such file or directory")
	require.NotNil(t, handler)
}
