//go:build test
// +build test

package blobstore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Dewberry/s3api/auth"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
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

func TestNewBlobHandlerAllowListBucketNotFound(t *testing.T) {
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
        "bucket_allow_list": ["test-bucket-1", "test-bucket-3"]
    }`
	envJsonFile, err := os.CreateTemp("", "env.json")
	require.NoError(t, err)
	defer os.Remove(envJsonFile.Name())
	_, err = envJsonFile.Write([]byte(envJsonContent))
	require.NoError(t, err)
	envJsonFile.Close()

	handler, err := NewBlobHandler(envJsonFile.Name(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "some buckets in the `bucket_allow_list` were not found")
	require.NotNil(t, handler)
}

func TestGetController(t *testing.T) {
	envVars := map[string]string{
		"S3_MOCK":      "0",
		"USE_MOCK_AWS": "true",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	mockS3Client := &mockS3Client{
		GetBucketLocationOutput: &s3.GetBucketLocationOutput{
			LocationConstraint: aws.String("us-west-2"),
		},
	}
	mockSession := &session.Session{
		Config: &aws.Config{
			Region: aws.String("us-east-1"),
		},
	}

	handler := &BlobHandler{
		S3Controllers: []S3Controller{
			{
				Sess:    mockSession,
				S3Svc:   mockS3Client,
				Buckets: []string{"test-bucket"},
			},
		},
		Mu: sync.Mutex{},
	}

	// Capture log messages
	hook := test.NewGlobal()
	defer hook.Reset()

	// Test case: Bucket found and region match
	ctrl, err := handler.GetController("test-bucket")
	require.NoError(t, err)
	require.NotNil(t, ctrl)
	require.Equal(t, "us-west-2", *ctrl.Sess.Config.Region)

	// Test case: Bucket not found
	ctrl, err = handler.GetController("non-existent-bucket")
	require.Error(t, err)
	require.Contains(t, err.Error(), "`bucket` 'non-existent-bucket' not found")

	// Test case: Bucket found but getBucketRegion returns error
	mockS3Client.GetBucketLocationError = errors.New("mocked error")
	ctrl, err = handler.GetController("test-bucket")
	require.Error(t, err)
	require.Contains(t, err.Error(), "`bucket` 'test-bucket' not found")

	logEntry := hook.LastEntry()
	require.NotNil(t, logEntry)
	require.Equal(t, log.ErrorLevel, logEntry.Level)
	require.Contains(t, logEntry.Message, "Failed to get region for bucket 'test-bucket'")

	// Reset the error for the next test
	mockS3Client.GetBucketLocationError = nil
}

func TestHandleListBuckets(t *testing.T) {
	envVars := map[string]string{
		"INIT_AUTH": "1",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Mock S3 client
	mockSvc := &mockS3Client{
		ListBucketsOutput: s3.ListBucketsOutput{
			Buckets: []*s3.Bucket{
				{Name: aws.String("bucket1")},
				{Name: aws.String("bucket2")},
			},
		},
		ListBucketsError: nil,
	}

	mockSession := &session.Session{
		Config: &aws.Config{
			Region: aws.String("us-east-1"),
		},
	}

	// Mock database
	db, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &auth.PostgresDB{Handle: db}

	userEmail := "test@example.com"
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	operations := []string{"read", "write"}

	// Mock expected query
	query := regexp.QuoteMeta(`
        WITH unnested_permissions AS (
            SELECT DISTINCT unnest(allowed_s3_prefixes) AS allowed_prefix
            FROM permissions
            WHERE user_email = $1 AND operation = ANY($3)
        )
        SELECT allowed_prefix
        FROM unnested_permissions
        WHERE allowed_prefix LIKE $2 || '/%'
        ORDER BY allowed_prefix;
    `)

	sqlMock.ExpectQuery(query).
		WithArgs(userEmail, "/", pq.Array(operations)).
		WillReturnRows(sqlmock.NewRows([]string{"allowed_prefix"}))

	sqlMock.ExpectQuery(query).
		WithArgs(userEmail, "/"+bucket1, pq.Array(operations)).
		WillReturnRows(sqlmock.NewRows([]string{"allowed_prefix"}).AddRow("/bucket1/prefix1"))

	sqlMock.ExpectQuery(query).
		WithArgs(userEmail, "/"+bucket2, pq.Array(operations)).
		WillReturnRows(sqlmock.NewRows([]string{"allowed_prefix"}))

	handler := &BlobHandler{
		S3Controllers: []S3Controller{
			{
				Sess:    mockSession,
				S3Svc:   mockSvc,
				Buckets: []string{"bucket1", "bucket2"},
			},
		},
		Mu:     sync.Mutex{},
		DB:     pgDB,
		Config: &Config{AuthLevel: 1, LimitedReaderRoleName: "limited_reader"},
	}

	// Set claims in the context
	claims := &auth.Claims{
		Email: "test@example.com",
		RealmAccess: map[string][]string{
			"roles": {"limited_reader"},
		},
	}
	c.Set("claims", claims)

	err = handler.HandleListBuckets(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code)

	var response []BucketInfo
	err = json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(t, err)

	expectedResponse := []BucketInfo{
		{ID: 0, Name: "bucket1", CanRead: true},
		{ID: 1, Name: "bucket2", CanRead: false},
	}
	sort.Slice(expectedResponse, func(i, j int) bool {
		if expectedResponse[i].CanRead == expectedResponse[j].CanRead {
			return expectedResponse[i].Name < expectedResponse[j].Name
		}
		return expectedResponse[i].CanRead && !expectedResponse[j].CanRead
	})
	require.Equal(t, expectedResponse, response)
}

func TestHandleListBucketsError(t *testing.T) {
	envVars := map[string]string{
		"INIT_AUTH": "1",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Mock S3 client with error
	mockSvc := &mockS3Client{
		ListBucketsOutput: s3.ListBucketsOutput{},
		ListBucketsError:  awserr.New("ListBucketsError", "Mocked error", nil),
	}

	mockSession := &session.Session{
		Config: &aws.Config{
			Region: aws.String("us-east-1"),
		},
	}
	// Mock database
	db, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &auth.PostgresDB{Handle: db}
	userEmail := "test@example.com"
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	operations := []string{"read", "write"}

	// Mock expected query
	query := regexp.QuoteMeta(`
        WITH unnested_permissions AS (
            SELECT DISTINCT unnest(allowed_s3_prefixes) AS allowed_prefix
            FROM permissions
            WHERE user_email = $1 AND operation = ANY($3)
        )
        SELECT allowed_prefix
        FROM unnested_permissions
        WHERE allowed_prefix LIKE $2 || '/%'
        ORDER BY allowed_prefix;
    `)

	sqlMock.ExpectQuery(query).
		WithArgs(userEmail, "/", pq.Array(operations)).
		WillReturnRows(sqlmock.NewRows([]string{"allowed_prefix"}))
	sqlMock.ExpectQuery(query).
		WithArgs(userEmail, "/"+bucket1, pq.Array(operations)).
		WillReturnRows(sqlmock.NewRows([]string{"allowed_prefix"}).AddRow("/bucket1/prefix1"))

	sqlMock.ExpectQuery(query).
		WithArgs(userEmail, "/"+bucket2, pq.Array(operations)).
		WillReturnRows(sqlmock.NewRows([]string{"allowed_prefix"}))

	handler := &BlobHandler{
		S3Controllers: []S3Controller{
			{
				Sess:    mockSession,
				S3Svc:   mockSvc,
				Buckets: []string{"bucket1", "bucket2"},
			},
		},
		DB:              pgDB,
		Mu:              sync.Mutex{},
		Config:          &Config{AuthLevel: 1, LimitedReaderRoleName: "limited_reader"},
		AllowAllBuckets: true,
	}

	// Set claims in the context
	claims := &auth.Claims{
		Email: "test@example.com",
		RealmAccess: map[string][]string{
			"roles": {"limited_reader"},
		},
	}
	c.Set("claims", claims)

	if assert.NoError(t, handler.HandleListBuckets(c)) {
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "error listing buckets")
	}
}

func TestHandleListPostgressError(t *testing.T) {
	envVars := map[string]string{
		"INIT_AUTH": "1",
	}
	setupEnvVariables(envVars)
	defer teardownEnvVariables(envVars)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Mock S3 client with error
	mockSvc := &mockS3Client{
		ListBucketsOutput: s3.ListBucketsOutput{},
		ListBucketsError:  awserr.New("ListBucketsError", "Mocked error", nil),
	}

	mockSession := &session.Session{
		Config: &aws.Config{
			Region: aws.String("us-east-1"),
		},
	}
	// Mock database
	db, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &auth.PostgresDB{Handle: db}
	userEmail := "test@example.com"

	operations := []string{"read", "write"}

	// Mock expected query
	query := regexp.QuoteMeta(`
        WITH unnested_permissions AS (
            SELECT DISTINCT unnest(allowed_s3_prefixes) AS allowed_prefix
            FROM permissions
            WHERE user_email = $1 AND operation = ANY($3)
        )
        SELECT allowed_prefix
        FROM unnested_permissions
        WHERE allowed_prefix LIKE $2 || '/%'
        ORDER BY allowed_prefix;
    `)

	sqlMock.ExpectQuery(query).
		WithArgs(userEmail, "/", pq.Array(operations)).
		WillReturnError(sql.ErrConnDone)

	handler := &BlobHandler{
		S3Controllers: []S3Controller{
			{
				Sess:    mockSession,
				S3Svc:   mockSvc,
				Buckets: []string{"bucket1", "bucket2"},
			},
		},
		DB:              pgDB,
		Mu:              sync.Mutex{},
		Config:          &Config{AuthLevel: 1, LimitedReaderRoleName: "limited_reader"},
		AllowAllBuckets: true,
	}

	// Set claims in the context
	claims := &auth.Claims{
		Email: "test@example.com",
		RealmAccess: map[string][]string{
			"roles": {"limited_reader"},
		},
	}
	c.Set("claims", claims)

	if assert.NoError(t, handler.HandleListBuckets(c)) {
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "error getting `prefix` that the user can read and write to")
	}
}
