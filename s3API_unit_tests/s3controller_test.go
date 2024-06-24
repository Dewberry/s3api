//go:build test
// +build test

package test

import (
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
	ListBucketsOutput s3.ListBucketsOutput
	ListBucketsError  error
}

func (m *mockS3Client) ListBuckets(*s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return &m.ListBucketsOutput, m.ListBucketsError
}

func TestListBuckets(t *testing.T) {
	t.Setenv("INIT_AUTH", "0")

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
	t.Setenv("INIT_AUTH", "0")

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
