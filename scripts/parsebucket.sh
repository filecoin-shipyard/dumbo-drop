#!/bin/sh
rm -f .state*
export AWS_SDK_LOAD_CONFIG=1
export DUMBO_PARSE_FILE_LAMBDA=InitStaging-GetParseFileV2-110HY43XW9LJ
export DUMBO_BLOCK_STORE=chafey-dumbo-drop-test-block
./cli.js pull-bucket-v2 chafey-dumbo-drop-test --concurrency 1 --checkHead