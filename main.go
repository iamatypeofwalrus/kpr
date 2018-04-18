package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "kpr"
	app.Usage = "Kinesis Put Records\n\n   A simple CLI that takes input from STDIN and sends it to an AWS Kinesis stream"
	app.UsageText = "cat your_records.json | kpr --stream YOUR_STREAM --region us-west-2"
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
			Usage: "AWS `REGION`",
			Value: "us-east-1",
		},
		cli.BoolFlag{
			Name:  "help, h",
			Usage: "show this help message",
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
	svc := kinesis.New(sess)
	return stream(streamName, svc)
}

func stream(streamName string, svc *kinesis.Kinesis) error {
	// Hey! You're probably wondering what the hell I'm doing here with this variable.
	// I want to approximate a round-robin strategy when sending data to Kinesis to make sure
	// all shards get an equal amount of data. Unfortunately, doing a real round robin strategy
	// is a pain in the ass to implement with the Kinesis API. We're going to do a poor man's
	// round robin by letting the hash function that is applied to the partition key do it's job
	// and just send it an monotonically increasing integer. We _should_ get good coverage across
	// all shards this way.
	var count uint
	ctx := context.Background()
	scanner := bufio.NewScanner(os.Stdin)
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

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
