#!/bin/bash
scripts/reset.sh
scripts/parsebucket.sh
scripts/createparts.sh

if curl --output /dev/null --silent --head --fail https://dumbo-v2-cars-chafey-dumbo-drop-test.s3-us-west-2.amazonaws.com/bafyreibspq2gevryu62m73u5cuxbxtpomysipap5v7byxoma5kmqqckh7y/bafyreibspq2gevryu62m73u5cuxbxtpomysipap5v7byxoma5kmqqckh7y.car; then
  echo "SUCCESS! CAR FILE FOUND"
else
    if curl --output /dev/null --silent --head --fail https://dumbo-v2-cars-chafey-dumbo-drop-test.s3-us-west-2.amazonaws.com/bafyreihzmxur34jpn6y7i3cois3qiyu4qa7wsygcjxu2gdgi2pnpzc54o4/bafyreihzmxur34jpn6y7i3cois3qiyu4qa7wsygcjxu2gdgi2pnpzc54o4.car; then
        echo "SUCCESS! CAR FILE FOUND"
    else
        echo "FAILURE! CAR FILE NOT FOUND"
    fi
fi

