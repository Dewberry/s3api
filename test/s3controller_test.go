//go:build test
// +build test

package test

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/Dewberry/s3api/blobstore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
)

type mockS3Client struct {
	s3iface.S3API
	ListBucketsOutput             s3.ListBucketsOutput
	ListBucketsError              error
	DeleteObjectError             error
	DeleteObjectsError            error
	HeadObjectError               error
	HeadObjectOutput              *s3.HeadObjectOutput
	CopyObjectOutput              *s3.CopyObjectOutput
	CopyObjectError               error
	GetObjectOutput               *s3.GetObjectOutput
	GetObjectError                error
	CreateMultipartUploadOutput   *s3.CreateMultipartUploadOutput
	CreateMultipartUploadError    error
	UploadPartOutput              *s3.UploadPartOutput
	UploadPartError               error
	CompleteMultipartUploadOutput *s3.CompleteMultipartUploadOutput
	CompleteMultipartUploadError  error
	AbortMultipartUploadError     error
	ListObjectsV2PagesFunc        func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error
}

func (m *mockS3Client) ListBuckets(*s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return &m.ListBucketsOutput, m.ListBucketsError
}

func (m *mockS3Client) DeleteObject(*s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, m.DeleteObjectError
}

func (m *mockS3Client) DeleteObjects(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{}, m.DeleteObjectsError
}

func (m *mockS3Client) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return m.HeadObjectOutput, m.HeadObjectError
}

func (m *mockS3Client) CopyObject(*s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	return m.CopyObjectOutput, m.CopyObjectError
}

func (m *mockS3Client) GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return m.GetObjectOutput, m.GetObjectError
}

func (m *mockS3Client) CreateMultipartUpload(input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	return m.CreateMultipartUploadOutput, m.CreateMultipartUploadError
}

func (m *mockS3Client) UploadPart(input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	return m.UploadPartOutput, m.UploadPartError
}

func (m *mockS3Client) CompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	return m.CompleteMultipartUploadOutput, m.CompleteMultipartUploadError
}

func (m *mockS3Client) AbortMultipartUpload(input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, m.AbortMultipartUploadError
}

func (m *mockS3Client) ListObjectsV2Pages(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
	if m.ListObjectsV2PagesFunc != nil {
		return m.ListObjectsV2PagesFunc(input, fn)
	}
	return nil
}

func (m *mockS3Client) GetObjectRequest(input *s3.GetObjectInput) (*request.Request, *s3.GetObjectOutput) {
	req := &request.Request{
		HTTPRequest: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "s3.amazonaws.com",
				Path:   "/" + *input.Bucket + "/" + *input.Key,
			},
		},
		Operation: &request.Operation{
			Name:       "GetObject",
			HTTPMethod: "GET",
		},
	}
	return req, m.GetObjectOutput
}

func (m *mockS3Client) PutObjectRequest(input *s3.PutObjectInput) (*request.Request, *s3.PutObjectOutput) {
	req := &request.Request{
		HTTPRequest: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "s3.amazonaws.com",
				Path:   "/" + *input.Bucket + "/" + *input.Key,
			},
		},
		Operation: &request.Operation{
			Name:       "PutObject",
			HTTPMethod: "PUT",
		},
	}
	return req, &s3.PutObjectOutput{}
}

func (m *mockS3Client) UploadPartRequest(input *s3.UploadPartInput) (*request.Request, *s3.UploadPartOutput) {
	req := &request.Request{
		HTTPRequest: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "s3.amazonaws.com",
				Path:   "/" + *input.Bucket + "/" + *input.Key + "?partNumber=" + strconv.FormatInt(*input.PartNumber, 10) + "&uploadId=" + *input.UploadId,
			},
		},
		Operation: &request.Operation{
			Name:       "UploadPart",
			HTTPMethod: "PUT",
		},
	}
	return req, m.UploadPartOutput
}

func TestListBuckets(t *testing.T) {
	mockSvc := &mockS3Client{
		ListBucketsOutput: s3.ListBucketsOutput{
			Buckets: []*s3.Bucket{
				{Name: aws.String("test-bucket-1")},
				{Name: aws.String("test-bucket-2")},
			},
		},
		ListBucketsError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	result, err := s3Ctrl.ListBuckets()

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Buckets, 2)
	require.Equal(t, "test-bucket-1", *result.Buckets[0].Name)
	require.Equal(t, "test-bucket-2", *result.Buckets[1].Name)
}

func TestListBucketsError(t *testing.T) {
	mockSvc := &mockS3Client{
		ListBucketsOutput: s3.ListBucketsOutput{},
		ListBucketsError:  awserr.New("ListBucketsError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	result, err := s3Ctrl.ListBuckets()

	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, "ListBucketsError: Mocked error", err.Error())
}

func TestDeleteObject(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError:   nil,
		DeleteObjectError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.DeleteObject("test-bucket", "test-key")

	require.NoError(t, err)
}

func TestDeleteObjectError(t *testing.T) {
	mockSvc := &mockS3Client{
		HeadObjectError:   awserr.New("HeadObjectError", "Mocked error", nil),
		DeleteObjectError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.DeleteObject("test-bucket", "test-key")

	require.Error(t, err)
	require.Equal(t, "error checking object's existence while attempting to delete, HeadObjectError: Mocked error", err.Error())
}

func TestDeleteList(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		DeleteObjectsError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	page := &s3.ListObjectsV2Output{
		Contents: []*s3.Object{
			{Key: aws.String("test-key-1")},
			{Key: aws.String("test-key-2")},
		},
	}

	err := s3Ctrl.DeleteList(page, "test-bucket")

	require.NoError(t, err)
}

func TestDeleteListError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		DeleteObjectsError: awserr.New("DeleteObjectsError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	page := &s3.ListObjectsV2Output{
		Contents: []*s3.Object{
			{Key: aws.String("test-key-1")},
			{Key: aws.String("test-key-2")},
		},
	}

	err := s3Ctrl.DeleteList(page, "test-bucket")

	require.Error(t, err)
	require.Equal(t, "DeleteObjectsError: Mocked error", err.Error())
}

func TestDeleteKeys(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		DeleteObjectsError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	keys := []string{"test-key-1", "test-key-2"}

	err := s3Ctrl.DeleteKeys("test-bucket", keys)

	require.NoError(t, err)
}

func TestDeleteKeysError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		DeleteObjectsError: awserr.New("DeleteObjectsError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	keys := []string{"test-key-1", "test-key-2"}

	err := s3Ctrl.DeleteKeys("test-bucket", keys)

	require.Error(t, err)
	require.Equal(t, "DeleteObjectsError: Mocked error", err.Error())
}

func TestDeleteKeysNonExistent(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: awserr.New("NoSuchKey", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	keys := []string{"nonexistent-key-1", "nonexistent-key-2"}

	err := s3Ctrl.DeleteKeys("test-bucket", keys)

	require.Error(t, err)
	require.Contains(t, err.Error(), "NoSuchKey: Mocked error")
}

func TestGetList(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		ListObjectsV2PagesFunc: func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
			page := &s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("test-prefix/file1")},
					{Key: aws.String("test-prefix/file2")},
				},
				IsTruncated: aws.Bool(false),
			}
			fn(page, true)
			return nil
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	result, err := s3Ctrl.GetList("test-bucket", "test-prefix/", false)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Contents, 2)
	require.Equal(t, "test-prefix/file1", *result.Contents[0].Key)
	require.Equal(t, "test-prefix/file2", *result.Contents[1].Key)
}

func TestGetListError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		ListObjectsV2PagesFunc: func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
			return awserr.New("ListObjectsV2PagesError", "Mocked error", nil)
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	result, err := s3Ctrl.GetList("test-bucket", "test-prefix/", false)

	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, "ListObjectsV2PagesError: Mocked error", err.Error())
}

func TestGetListWithCallBack(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		ListObjectsV2PagesFunc: func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
			page := &s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("test-prefix/file1")},
					{Key: aws.String("test-prefix/file2")},
				},
				IsTruncated: aws.Bool(false),
			}
			fn(page, true)
			return nil
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.GetListWithCallBack("test-bucket", "test-prefix/", false, func(page *s3.ListObjectsV2Output) error {
		require.Len(t, page.Contents, 2)
		require.Equal(t, "test-prefix/file1", *page.Contents[0].Key)
		require.Equal(t, "test-prefix/file2", *page.Contents[1].Key)
		return nil
	})

	require.NoError(t, err)
}

func TestGetListWithCallBackError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		ListObjectsV2PagesFunc: func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
			return awserr.New("ListObjectsV2PagesError", "Mocked error", nil)
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.GetListWithCallBack("test-bucket", "test-prefix/", false, func(page *s3.ListObjectsV2Output) error {
		return nil
	})

	require.Error(t, err)
	require.Equal(t, "ListObjectsV2PagesError: Mocked error", err.Error())
}

func TestGetListWithCallBackProcessError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		ListObjectsV2PagesFunc: func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
			page := &s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("test-prefix/file1")},
					{Key: aws.String("test-prefix/file2")},
				},
				IsTruncated: aws.Bool(false),
			}
			fn(page, true)
			return nil
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	processError := errors.New("ProcessError")
	err := s3Ctrl.GetListWithCallBack("test-bucket", "test-prefix/", false, func(page *s3.ListObjectsV2Output) error {
		return processError
	})

	require.Error(t, err)
	require.Equal(t, processError, err)
}

func TestGetMetaData(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectOutput: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(1234),
			ContentType:   aws.String("text/plain"),
		},
		HeadObjectError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	result, err := s3Ctrl.GetMetaData("test-bucket", "test-key")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(1234), *result.ContentLength)
	require.Equal(t, "text/plain", *result.ContentType)
}

func TestGetMetaDataError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: awserr.New("HeadObjectError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	result, err := s3Ctrl.GetMetaData("test-bucket", "test-key")

	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, "HeadObjectError: Mocked error", err.Error())
}

func TestKeyExists(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	exists, err := s3Ctrl.KeyExists("test-bucket", "test-key")

	require.NoError(t, err)
	require.True(t, exists)
}

func TestKeyExistsNotFound(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: awserr.New("NotFound", "Mocked not found error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	exists, err := s3Ctrl.KeyExists("test-bucket", "test-key")

	require.NoError(t, err)
	require.False(t, exists)
}

func TestKeyExistsError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: awserr.New("HeadObjectError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	exists, err := s3Ctrl.KeyExists("test-bucket", "test-key")

	require.Error(t, err)
	require.False(t, exists)
	require.Equal(t, "HeadObjectError: Mocked error", err.Error())
}

func TestMovePrefix(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		CopyObjectError:    nil,
		DeleteObjectsError: nil,
		ListObjectsV2PagesFunc: func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
			page := &s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("src-prefix/file1")},
					{Key: aws.String("src-prefix/file2")},
				},
				IsTruncated: aws.Bool(false),
			}
			fn(page, true)
			return nil
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.MovePrefix("test-bucket", "src-prefix/", "dest-prefix/")

	require.NoError(t, err)
}

func TestMovePrefixError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		CopyObjectError: awserr.New("CopyObjectError", "Mocked error", nil),
		ListObjectsV2PagesFunc: func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
			page := &s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("src-prefix/file1")},
					{Key: aws.String("src-prefix/file2")},
				},
				IsTruncated: aws.Bool(false),
			}
			fn(page, true)
			return nil
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.MovePrefix("test-bucket", "src-prefix/", "dest-prefix/")

	require.Error(t, err)
	require.Equal(t, "CopyObjectError: Mocked error", err.Error())
}

func TestCopyObject(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		CopyObjectError:   nil,
		DeleteObjectError: nil,
		HeadObjectError:   awserr.New("NotFound", "Mocked not found error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.CopyObject("test-bucket", "src-key", "dest-key")

	require.NoError(t, err)
}

func TestCopyObjectError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		CopyObjectError: awserr.New("CopyObjectError", "Mocked error", nil),
		HeadObjectError: awserr.New("NotFound", "Mocked not found error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.CopyObject("test-bucket", "src-key", "dest-key")

	require.Error(t, err)
	require.Equal(t, "CopyObjectError: Mocked error", err.Error())
}

func TestCopyObjectIdenticalKeysError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.CopyObject("test-bucket", "same-key", "same-key")

	require.Error(t, err)
	require.Equal(t, "InvalidParameter: Source and Destination Keys are Identical\ncaused by: `src_key` same-key and `dest_key` same-key cannot be the same for move operation", err.Error())
}

func TestCopyObjectDestinationExistsError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: nil, // Simulates that the destination key already exists
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.CopyObject("test-bucket", "src-key", "dest-key")

	require.Error(t, err)
	require.Equal(t, "AlreadyExists: Destination Key Already Exists\ncaused by: dest-key already exists in the bucket; consider renaming `dest_key`", err.Error())
}

func TestFetchObjectContent(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	content := "mock file content"
	mockSvc := &mockS3Client{
		GetObjectOutput: &s3.GetObjectOutput{
			Body: io.NopCloser(strings.NewReader(content)),
		},
		GetObjectError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	body, err := s3Ctrl.FetchObjectContent("test-bucket", "test-key")

	require.NoError(t, err)
	require.NotNil(t, body)

	defer body.Close()
	buf := new(strings.Builder)
	_, err = io.Copy(buf, body)
	require.NoError(t, err)
	require.Equal(t, content, buf.String())
}

func TestFetchObjectContentError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		GetObjectError: awserr.New("GetObjectError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	body, err := s3Ctrl.FetchObjectContent("test-bucket", "test-key")

	require.Error(t, err)
	require.Nil(t, body)
	require.Equal(t, "GetObjectError: Mocked error", err.Error())
}

func TestGetDownloadPresignedURL(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: nil,
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	url, err := s3Ctrl.GetDownloadPresignedURL("test-bucket", "test-key", 7)

	require.NoError(t, err)
	require.NotEmpty(t, url)
	require.Contains(t, url, "https://s3.amazonaws.com/test-bucket/test-key")
}

func TestGetDownloadPresignedURLError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		HeadObjectError: awserr.New("HeadObjectError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	url, err := s3Ctrl.GetDownloadPresignedURL("test-bucket", "test-key", 7)

	require.Error(t, err)
	require.Empty(t, url)
	require.Contains(t, err.Error(), "error checking if object with `key` test-key exists")
}

func TestUploadS3Obj(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	content := "mock file content"
	body := io.NopCloser(strings.NewReader(content))
	mockSvc := &mockS3Client{
		CreateMultipartUploadOutput: &s3.CreateMultipartUploadOutput{
			UploadId: aws.String("mockUploadID"),
		},
		UploadPartOutput: &s3.UploadPartOutput{
			ETag: aws.String("mockETag"),
		},
		CompleteMultipartUploadOutput: &s3.CompleteMultipartUploadOutput{},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.UploadS3Obj("test-bucket", "test-key", body)

	require.NoError(t, err)
}

func TestUploadS3ObjError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	content := "mock file content"
	body := io.NopCloser(strings.NewReader(content))
	mockSvc := &mockS3Client{
		CreateMultipartUploadError: awserr.New("CreateMultipartUploadError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.UploadS3Obj("test-bucket", "test-key", body)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error creating multipart upload for object with `key` test-key")
}

func TestGetUploadPresignedURL(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	url, err := s3Ctrl.GetUploadPresignedURL("test-bucket", "test-key", 60)

	require.NoError(t, err)
	require.NotEmpty(t, url)
	require.Contains(t, url, "https://s3.amazonaws.com/test-bucket/test-key")
}

func TestGetUploadPartPresignedURL(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	urlStr, err := s3Ctrl.GetUploadPartPresignedURL("test-bucket", "test-key", "mockUploadID", 1, 60)

	require.NoError(t, err)
	require.NotEmpty(t, urlStr)

	// Decode the URL before checking
	decodedURL, err := url.QueryUnescape(urlStr)
	require.NoError(t, err)
	require.Contains(t, decodedURL, "https://s3.amazonaws.com/test-bucket/test-key?partNumber=1&uploadId=mockUploadID")
}

func TestGetMultiPartUploadID(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		CreateMultipartUploadOutput: &s3.CreateMultipartUploadOutput{
			UploadId: aws.String("mockUploadID"),
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	uploadID, err := s3Ctrl.GetMultiPartUploadID("test-bucket", "test-key")

	require.NoError(t, err)
	require.Equal(t, "mockUploadID", uploadID)
}

func TestCompleteMultipartUpload(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		CompleteMultipartUploadOutput: &s3.CompleteMultipartUploadOutput{
			Location: aws.String("https://s3.amazonaws.com/test-bucket/test-key"),
		},
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	parts := []*s3.CompletedPart{
		{ETag: aws.String("etag1"), PartNumber: aws.Int64(1)},
	}

	result, err := s3Ctrl.CompleteMultipartUpload("test-bucket", "test-key", "mockUploadID", parts)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "https://s3.amazonaws.com/test-bucket/test-key", *result.Location)
}

func TestAbortMultipartUpload(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.AbortMultipartUpload("test-bucket", "test-key", "mockUploadID")

	require.NoError(t, err)
}

func TestAbortMultipartUploadError(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

	mockSvc := &mockS3Client{
		AbortMultipartUploadError: awserr.New("AbortMultipartUploadError", "Mocked error", nil),
	}

	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	err := s3Ctrl.AbortMultipartUpload("test-bucket", "test-key", "mockUploadID")

	require.Error(t, err)
	require.Contains(t, err.Error(), "AbortMultipartUploadError: Mocked error")
}
