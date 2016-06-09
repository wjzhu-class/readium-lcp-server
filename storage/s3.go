package storage

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3store struct {
	config S3Config
	client *s3.S3
}

type s3item struct {
	bucket string
	key    string
	store  *s3store
}

func (i s3item) Key() string {
	return i.key
}

func (i s3item) PublicUrl() string {
	return fmt.Sprintf("http://%s/%s", i.store.config.PublicURL, i.bucket, i.key)
}

func (i s3item) Contents() (io.ReadCloser, error) {
	resp, err := i.store.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(i.store.config.Bucket),
		Key:    aws.String(i.key),
	})

	return resp.Body, err
}

func (s *s3store) Add(key string, r io.ReadSeeker) (Item, error) {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
		Body:   r,
	})

	item := s3item{bucket: s.config.Bucket, key: key, store: s}

	return item, err
}

func (s *s3store) Get(key string) (Item, error) {
	_, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	return s3item{bucket: s.config.Bucket, key: key, store: s}, err
}

func (s *s3store) Remove(key string) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})

	return err
}

func (s *s3store) List() ([]Item, error) {
	objects, err := s.client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(s.config.Bucket),
	})

	if err != nil {
		return nil, err
	}

	var items []Item

	for _, o := range objects.Contents {
		items = append(items, s3item{bucket: s.config.Bucket, key: *o.Key, store: s})
	}

	return items, nil
}

type S3Config struct {
	PublicURL string

	Bucket   string
	Endpoint string
	Region   string

	Id     string
	Secret string
	Token  string

	DisableSSL     bool
	ForcePathStyle bool
}

func S3(config S3Config) (Store, error) {
	c := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.Id, config.Secret, config.Token),
		DisableSSL:       aws.Bool(config.DisableSSL),
		S3ForcePathStyle: aws.Bool(config.ForcePathStyle),
		Region:           aws.String(config.Region),
		Endpoint:         aws.String(config.Endpoint)}
	client := s3.New(session.New(c.WithLogLevel(aws.LogDebug)))
	return &s3store{client: client, config: config}, nil
}
