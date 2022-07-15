package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

func main() {
	sess := session.Must(session.NewSession())
	creds := stscreds.NewCredentials(sess, "arn:aws:iam::337081975962:role/AucfanDevMasterRole")

	sess = (session.New(&aws.Config{
		Region:      aws.String("ap-northeast-1"),
		Credentials: creds,
	}))

	const bucket = "isoda-test-firehose"
	const prefix = "date/2021/12/04/"
	const dst_key = "date/2021/12/04/a.gz"

	svc := s3.New(sess)

	result, _ := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for _, r := range result.Contents {
		fmt.Println(*r.Key)

		obj, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String("isoda-test-firehose"),
			Key:    aws.String(*r.Key),
		})
		if err != nil {
			fmt.Println(err)
		}

		b, _ := ioutil.ReadAll(obj.Body)

		var gzip_buf bytes.Buffer
		writer := gzip.NewWriter(&gzip_buf)

		if _, err := writer.Write(b); err != nil {
			fmt.Println(err)
		}
		writer.Close()

		_, err = s3.New(sess).PutObject(&s3.PutObjectInput{
			Bucket:          aws.String("isoda-test-firehose"),
			Key:             aws.String(dst_key),
			Body:            bytes.NewReader(gzip_buf.Bytes()),
			ContentType:     aws.String("application/octet-stream"),
			ContentEncoding: aws.String("gzip"),
		})

	}

}
