#!/bin/bash
aws dynamodb delete-table \
    --table-name dumbo-v2-chafey-dumbo-drop-test

aws dynamodb create-table \
    --attribute-definitions AttributeName=url,AttributeType=S \
    --table-name dumbo-v2-chafey-dumbo-drop-test \
    --key-schema AttributeName=url,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
