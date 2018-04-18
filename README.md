# kpr -> Kinesis Put Records
## Install
```
go get -u github.com/iamatypeofwalrus/kpr
```
## Usage
```
$ kpr -h
NAME:
   kpr - Kinesis Put Records

   A simple CLI that takes input from STDIN and sends it to an AWS Kinesis stream

USAGE:
   cat your_records.json | kpr --stream YOUR_STREAM --region us-west-2

GLOBAL OPTIONS:
   --stream NAME, -s NAME      Kinesis stream NAME
   --region REGION, -r REGION  AWS REGION (default: "us-east-1")
   --help, -h                  show this help message
```
