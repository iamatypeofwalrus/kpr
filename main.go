package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "kpr"
	app.Usage = "Kinesis Put Records\n\n   A simple CLI that takes input from STDIN and sends it to an AWS Kinesis stream"
	app.UsageText = "cat your_records.json | kpr --stream YOUR_STREAM_NAME --region us-west-2"
	app.Version = "0.1.0"
	app.HideHelp = true
	app.HideVersion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "stream, s",
			Usage: "Kinesis stream `NAME`",
		},
		cli.StringFlag{
			Name:  "region, r",
			Usage: "Amazon Web Service `REGION`",
			Value: "us-east-1",
		},
		cli.StringFlag{
			Name:  "delimiter, d",
			Usage: "Kinesis Firehose `DELIMITER`",
			Value: "\n",
		},
		cli.BoolFlag{
			Name:  "help, h",
			Usage: "show this help message",
		},
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "log verbosely",
		},
	}

	app.Action = do

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func do(c *cli.Context) error {
	if c.Bool("help") {
		cli.ShowAppHelp(c)
		return nil
	}

	if !c.Bool("verbose") {
		log.SetOutput(ioutil.Discard)
	}

	delimiter := c.String("delimiter")

	streamName := c.String("stream")
	if streamName == "" {
		return fmt.Errorf("--stream, -s flag is required")
	}

	region := c.String("region")

	sess := session.Must(
		session.NewSession(
			&aws.Config{Region: aws.String(region)},
		),
	)

	kinesisClient := kinesis.New(sess)
	firehoseClient := firehose.New(sess)

	streamExists, isFirehose := checkStream(streamName, kinesisClient, firehoseClient)

	if !streamExists {
		return fmt.Errorf("stream %v doesn't exists in kinesis streams or firehose", streamName)
	}

	if isFirehose {
		return streamToFirehose(streamName, os.Stdin, delimiter, firehoseClient)
	}

	return streamToKinesis(streamName, os.Stdin, kinesisClient)
}

func checkStream(streamName string, kinesisClient *kinesis.Kinesis, firehoseClient *firehose.Firehose) (exists, useFirehose bool) {
	log.Println("checking if stream", streamName, "exists")
	exists = false
	useFirehose = false

	_, err := kinesisClient.DescribeStream(
		&kinesis.DescribeStreamInput{StreamName: aws.String(streamName)},
	)

	if err == nil {
		log.Println("it exists! stream is a Kinesis stream")
		exists = true
		return
	}

	_, err = firehoseClient.DescribeDeliveryStream(
		&firehose.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String(streamName)},
	)

	if err == nil {
		log.Println("it exists! stream is a Firehose")
		exists, useFirehose = true, true
		return
	}

	log.Println("could not find stream in kinesis or firehose")
	return
}

func streamToFirehose(streamName string, input io.Reader, delimiter string, svc *firehose.Firehose) error {
	log.Print("streaming to firehose")
	scanner := bufio.NewScanner(input)

	var count uint
	for scanner.Scan() {
		req := &firehose.PutRecordInput{
			DeliveryStreamName: aws.String(streamName),
			Record: &firehose.Record{
				Data: []byte(scanner.Text() + delimiter),
			},
		}

		_, err := svc.PutRecord(req)

		if err != nil {
			return err
		}

		count++
	}

	log.Println("streamed", count, "records")

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func streamToKinesis(streamName string, input io.Reader, svc *kinesis.Kinesis) error {
	// Hey! You're probably wondering what the hell I'm doing here with this variable.
	// I want to approximate a round-robin strategy when sending data to Kinesis to make sure
	// all shards get an equal amount of data. Unfortunately, doing a real round robin strategy
	// is a pain in the ass to implement with the Kinesis API. We're going to do a poor man's
	// round robin by letting the hash function that is applied to the partition key do it's job
	// and just send it an monotonically increasing integer. We _should_ get good coverage across
	// all shards this way.
	log.Print("streaming to kinesis")
	var count uint
	ctx := context.Background()
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		req := &kinesis.PutRecordInput{
			StreamName:   aws.String(streamName),
			PartitionKey: aws.String(fmt.Sprint(count)),
			Data:         []byte(scanner.Text()),
		}

		_, err := svc.PutRecordWithContext(ctx, req)
		if err != nil {
			return err
		}

		count++
	}

	log.Print("streamed", count, "records")

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
