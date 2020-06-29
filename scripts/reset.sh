#!/bin/sh
# clear dynamodb
aws dynamodb delete-item \
    --table-name dumbo-v2-chafey-dumbo-drop-test \
    --key '{"url" : {"S": "https://chafey-dumbo-drop-test.s3.us-west-2.amazonaws.com/CT1_J2KR"}}'
aws dynamodb delete-item \
    --table-name dumbo-v2-chafey-dumbo-drop-test \
    --key '{"url" : {"S": "https://chafey-dumbo-drop-test.s3.us-west-2.amazonaws.com/CT2_J2KR"}}'
aws dynamodb delete-item \
    --table-name dumbo-v2-chafey-dumbo-drop-test \
    --key '{"url" : {"S": "https://chafey-dumbo-drop-test.s3.us-west-2.amazonaws.com/MG1_J2KR"}}'

#clear block bucket
aws s3 rm \
    s3://chafey-dumbo-drop-test-block \
    --recursive
# clear car bucket
aws s3 rm \
    s3://dumbo-v2-cars-chafey-dumbo-drop-test \
    --recursive
