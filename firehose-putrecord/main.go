package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"bytes"
	"github.com/aucfan-yotsuya/gomod/common"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)

type Data struct {
	Ymd        string `json:"ymd"`
	UserId     int    `json:"user_id"`
	Disfa      string `json:"disfa"`
	Fqdn       string `json:"fqdn"`
	Path       string `json:"path"`
	QueryParam string `json:"queryparam"`
	CreatedAt  string `json:"created_at"`
}

const maxUint = 100000000

func main() {
	streamName := "test-isoda-dynamic"
	// streamName := "KDS-S3-ib5f9"

	var layout = "2006-01-02 15:04:05"
	var layout2 = "20060102"
	fmt.Println(common.NowJST().Format(layout))

	// Assume Roleを使用する
	sess := session.Must(session.NewSession())
	creds := stscreds.NewCredentials(sess, "arn:aws:iam::337081975962:role/AucfanDevMasterRole")
	firehoseService := firehose.New(sess, aws.NewConfig().WithRegion("ap-northeast-1").WithCredentials(creds))

	// Put Recoad
	for i := 0; i < 100; i++ {
		data := Data{
			Ymd:        common.NowJST().Format(layout2),
			UserId:     rand.Intn(maxUint),
			Disfa:      "97d0a27afdafcacc1e346c69fbca65aa",
			Fqdn:       "pro.aucfan.com",
			Path:       "/home",
			QueryParam: "",
			CreatedAt:  common.NowJST().Format(layout),
		}

		b := new(bytes.Buffer)
		json.NewEncoder(b).Encode(data)
		record := &firehose.PutRecordInput{DeliveryStreamName: &streamName, Record: &firehose.Record{Data: b.Bytes()}}
		resp, err := firehoseService.PutRecord(record)
		if err != nil {
			fmt.Printf("PutRecord err: %v\n", err)
		} else {
			fmt.Printf("PutRecord: %v\n", resp)
		}
	}

	// Put Record Batch
	recordsBatchInput := &firehose.PutRecordBatchInput{}
	recordsBatchInput = recordsBatchInput.SetDeliveryStreamName(streamName)
	records := []*firehose.Record{}

	for i := 0; i < 100; i++ {
		data := Data{
			Ymd:        common.NowJST().Format(layout2),
			UserId:     rand.Intn(maxUint),
			Disfa:      "97d0a27afdafcacc1e346c69fbca65aa",
			Fqdn:       "pro.aucfan.com",
			Path:       "/home",
			QueryParam: "",
			CreatedAt:  common.NowJST().Format(layout),
		}

		// b, err := json.Marshal(data)
		b := new(bytes.Buffer)
		json.NewEncoder(b).Encode(data)

		record := &firehose.Record{Data: b.Bytes()}
		records = append(records, record)

	}
	recordsBatchInput = recordsBatchInput.SetRecords(records)
	resp, err := firehoseService.PutRecordBatch(recordsBatchInput)
	if err != nil {
		fmt.Printf("PutRecordBatch err: %v\n", err)
	} else {
		fmt.Printf("PutRecordBatch: %v\n", resp)
	}
}
