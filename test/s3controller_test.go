//go:build test
// +build test

package test

import (
	"errors"
	"testing"

	"github.com/Dewberry/s3api/blobstore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
)

type mockS3Client struct {
	s3iface.S3API
	ListBucketsOutput      s3.ListBucketsOutput
	ListBucketsError       error
	DeleteObjectError      error
	DeleteObjectsError     error
	HeadObjectError        error
	ListObjectsV2PagesFunc func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error
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

func (m *mockS3Client) HeadObject(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, m.HeadObjectError
}

func (m *mockS3Client) ListObjectsV2Pages(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error {
	if m.ListObjectsV2PagesFunc != nil {
		return m.ListObjectsV2PagesFunc(input, fn)
	}
	return nil
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
