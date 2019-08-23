package writer

import (
	"bufio"
	"compress/gzip"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/namsral/flag"
	"github.com/prometheus/common/log"
)

type S3 struct {
	s3Region string
	s3Bucket string
	session  *session.Session
}

var (
	s3ID       string
	s3Secret   string
	s3Endpoint string
)

func init() {
	flag.StringVar(&s3ID, "AWS_ACCESS_KEY_ID", "-", "Specifies an AWS access key associated with an IAM user or role.")
	flag.StringVar(&s3Secret, "AWS_SECRET_ACCESS_KEY", "-", "Specifies the secret key associated with the access key. This is essentially the 'password' for the access key.")
	flag.StringVar(&s3Endpoint, "AWS_ENDPOINT", "-", "Specifies the url endpoint.")
}

func NewS3(s3Region, s3Bucket string) (f *S3, err error) {
	s, err := session.NewSession(&aws.Config{
		Endpoint:    aws.String(s3Endpoint),
		Region:      aws.String(s3Region),
		Credentials: credentials.NewStaticCredentials(s3ID, s3Secret, ""),
	})
	if err != nil {
		log.Fatal(err)
	}

	return &S3{
		s3Region: s3Region,
		s3Bucket: s3Bucket,
		session:  s,
	}, err
}

func (s *S3) Write(f string, r *bufio.Reader) (err error) {
	pr, pw := io.Pipe()
	go func() {
		gw := gzip.NewWriter(pw)
		_, err := r.WriteTo(gw)
		gw.Close()
		pw.Close()
		if err != nil {
			log.Fatalln("Failed to upload", err)
		}
	}()

	uploader := s3manager.NewUploader(s.session)
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   pr,
		Bucket: aws.String(s.s3Bucket),
		Key:    aws.String("pgbackup/" + f),
	})
	if err != nil {
		log.Fatalln("Failed to upload", err)
	}

	log.Infoln("Successfully uploaded to", result.Location)

	return nil
}

func (s *S3) CleanupOldBackups() (err error) {
	now := time.Now()
	svc := s3.New(s.session)
	listRes, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(s.s3Bucket)})
	if err != nil {
		log.Error(err.Error())
	}

	for _, listObj := range listRes.Contents {
		if err != nil {
			continue
		}
		age := now.Sub(*listObj.LastModified).Hours()
		if age > 8 {
			log.Infoln("DELETING BACKUP", listObj.LastModified)
			svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(s.s3Bucket), Key: listObj.Key})
		}
	}
	return
}
