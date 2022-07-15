package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aucfan-yotsuya/gomod/db"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strconv"
	"time"
)

type Data struct {
	Id         int    `json:"id"`
	Ymd        int    `json:"ymd"`
	UserId     int    `json:"user_id"`
	Disfa      string `json:"disfa"`
	Fqdn       string `json:"fqdn"`
	Path       string `json:"path"`
	QueryParam string `json:"queryparam"`
	CreatedAt  string `json:"created_at"`
}

func main() {

	const day = "20211203"
	const dst_key = "date/2021/12/03/a.gz"

	var (
		d       *db.DB
		err     error
		records []map[string][]byte
		jb      []byte
		buf     bytes.Buffer
	)
	d = db.New()
	d.NewTarget().NewConn(&db.DbConnOpt{
		Driver:  "mysql",
		Dsn:     "root:@tcp(test-isoda-user-track.cluster-ro-c5raum2iblmj.ap-northeast-1.rds.amazonaws.com:3306)/pro?timeout=100s&charset=utf8mb4&interpolateParams=true&parseTime=true&loc=Asia%2fTokyo",
		Timeout: 100 * time.Second,
	})
	if err = d.GetTarget(0).Conn.Ping(); err != nil {
		fmt.Println(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*100)
	defer cancel()
	records, err = d.GetTarget(0).Select(ctx, fmt.Sprintf("select * from user_track where ymd = %s;", day))

	if err != nil {
		fmt.Println(err)
	}

	for _, record := range records {
		id, _ := strconv.Atoi(string(record["id"]))
		user_id, _ := strconv.Atoi(string(record["user_id"]))
		ymd, _ := strconv.Atoi(string(record["ymd"]))
		t, _ := time.Parse(time.RFC3339, string(record["created_at"]))
		var layout = "2006-01-02 15:04:05"
		created_at := t.Format(layout)
		data := Data{
			Id:         id,
			Ymd:        ymd,
			UserId:     user_id,
			Disfa:      string(record["disfa"]),
			Fqdn:       string(record["fqdn"]),
			Path:       string(record["path"]),
			QueryParam: string(record["query_param"]),
			CreatedAt:  created_at,
		}

		jb, err = json.Marshal(data)
		if err != nil {
			fmt.Println(err)
		}

		if _, err := buf.Write(jb); err != nil {
			fmt.Println(err)
		}
		if _, err := buf.Write([]byte("\n")); err != nil {
			fmt.Println(err)
		}
	}

	d.Close()

	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)

	if _, err := writer.Write(buf.Bytes()); err != nil {
		fmt.Println(err)
	}
	writer.Close()

	sess := session.Must(session.NewSession())

	creds := stscreds.NewCredentials(sess, "arn:aws:iam::337081975962:role/AucfanDevMasterRole")

	sess = (session.New(&aws.Config{
		Region:      aws.String("ap-northeast-1"),
		Credentials: creds,
	}))

	_, err = s3.New(sess).PutObject(&s3.PutObjectInput{
		Bucket:          aws.String("isoda-test-firehose"),
		Key:             aws.String(dst_key),
		Body:            bytes.NewReader(buffer.Bytes()),
		ContentType:     aws.String("application/octet-stream"),
		ContentEncoding: aws.String("gzip"),
	})
}
