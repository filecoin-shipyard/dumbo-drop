#!/bin/sh
aws lambda invoke \
  --cli-binary-format raw-in-base64-out \
  --function-name InitStaging-GetParseFileV2-110HY43XW9LJ \
  --payload '{"query": {"urls":["https://chafey-dumbo-drop-test2.s3.us-west-2.amazonaws.com/wg04-compsamples/.DS_Store"],"blockBucket": "chafey-dump-drop-test-block2"}}' \
  outfile.json