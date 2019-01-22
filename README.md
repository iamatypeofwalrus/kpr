# kpr -> Kinesis Put Records
This tool uses [Go Modules](https://github.com/golang/go/wiki/Modules) for its dependencies :-)

## Install
```
go get -u github.com/iamatypeofwalrus/kpr
```
## Usage
```
$ kpr -h
NAME:
   kpr - Kinesis Put Records

   A simple CLI that takes input from STDIN and sends it to an AWS Kinesis or Firehose stream

USAGE:
   cat your_records.json | kpr --stream YOUR_STREAM_NAME --region us-west-2

GLOBAL OPTIONS:
   --stream NAME, -s NAME               Kinesis stream NAME
   --region REGION, -r REGION           Amazon Web Service REGION (default: "us-east-1")
   --delimiter DELIMITER, -d DELIMITER  Kinesis Firehose DELIMITER (default: "\n")
   --help, -h                           show this help message
   --verbose                            log verbosely
```
