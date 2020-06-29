#!/bin/sh
rm -f .state*
export AWS_SDK_LOAD_CONFIG=1
export DUMBO_BLOCK_BUCKET=chafey-dumbo-drop-test-block
export DUMBO_CREATE_PART_LAMBDA=InitStaging-GetCreatePartV2-7C3Q5VJDLIUG
./cli.js create-parts-v2 chafey-dumbo-drop-test --concurrency 1 