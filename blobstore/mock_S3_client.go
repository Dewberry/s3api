package blobstore

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client/metadata"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type mockS3Client struct {
	s3iface.S3API
	ListBucketsOutput             s3.ListBucketsOutput
	ListBucketsError              error
	HeadBucketError               error
	CreateBucketError             error
	PutBucketPolicyError          error
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
	GetBucketLocationInput        *s3.GetBucketLocationInput
	GetBucketLocationOutput       *s3.GetBucketLocationOutput
	GetBucketLocationError        error
	ListObjectsV2PagesFunc        func(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error
}

func (m *mockS3Client) HeadBucket(input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, m.HeadBucketError
}

func (m *mockS3Client) CreateBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	return &s3.CreateBucketOutput{}, m.CreateBucketError
}

func (m *mockS3Client) PutBucketPolicy(input *s3.PutBucketPolicyInput) (*s3.PutBucketPolicyOutput, error) {
	return &s3.PutBucketPolicyOutput{}, m.PutBucketPolicyError
}

func (m *mockS3Client) GetBucketLocationRequest(input *s3.GetBucketLocationInput) (*request.Request, *s3.GetBucketLocationOutput) {
	req := request.New(
		aws.Config{},
		metadata.ClientInfo{},
		request.Handlers{},
		nil,
		&request.Operation{
			Name:       "GetBucketLocation",
			HTTPMethod: "GET",
			HTTPPath:   "/{Bucket}?location",
		},
		input,
		m.GetBucketLocationOutput,
	)
	req.Handlers.Send.Clear()
	req.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = m.GetBucketLocationError
		r.Data = m.GetBucketLocationOutput
	})
	return req, m.GetBucketLocationOutput
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
