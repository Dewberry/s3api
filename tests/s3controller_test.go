package s3controller_test

import (
	"testing"

	"github.com/Dewberry/s3api/blobstore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// Define a mock struct that implements the s3iface.S3API interface
type mockS3Client struct {
	s3iface.S3API
	ListBucketsOutput s3.ListBucketsOutput
	ListBucketsError  error
}

// Mock implementation of the ListBuckets method
func (m *mockS3Client) ListBuckets(*s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return &m.ListBucketsOutput, m.ListBucketsError
}

func TestListBuckets(t *testing.T) {
	// Create a mock S3 client with predefined output and error
	t.Setenv("INIT_AUTH", "0")
	mockSvc := &mockS3Client{
		ListBucketsOutput: s3.ListBucketsOutput{
			Buckets: []*s3.Bucket{
				{
					Name: aws.String("test-bucket-1"),
				},
				{
					Name: aws.String("test-bucket-2"),
				},
			},
		},
		ListBucketsError: nil,
	}

	// Initialize the S3Controller with the mock S3 client
	s3Ctrl := blobstore.S3Controller{
		Sess:  &session.Session{},
		S3Svc: mockSvc,
	}

	// Call the ListBuckets method
	result, err := s3Ctrl.ListBuckets()

	// Validate the results
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Buckets, 2)
	assert.Equal(t, "test-bucket-1", *result.Buckets[0].Name)
	assert.Equal(t, "test-bucket-2", *result.Buckets[1].Name)
}
