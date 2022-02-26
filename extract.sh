#!/bin/bash

set -o xtrace

cd "$(dirname "$0")"

docker run --rm \
    -e PG_DB_HOST=$PG_DB_HOST \
    -e PG_DB_PORT=$PG_DB_PORT \
    -e PG_DB_NAME=$PG_DB_NAME \
    -e PG_DB_USER=$PG_DB_USER \
    -e PG_DB_PASSWORD=$PG_DB_PASSWORD \
    -e PG_REDDIT_GRANT_TYPE=$PG_REDDIT_GRANT_TYPE \
    -e PG_REDDIT_USERNAME=$PG_REDDIT_USERNAME \
    -e PG_REDDIT_PASSWORD=$PG_REDDIT_PASSWORD \
    -e PG_REDDIT_CLIENT_ID=$PG_REDDIT_CLIENT_ID \
    -e PG_REDDIT_SECRET_TOKEN=$PG_REDDIT_SECRET_TOKEN \
    -e PG_REDDIT_USER_AGENT=$PG_REDDIT_USER_AGENT \
    --name extract-test \
    -v $PWD:/home/ \
    -p 443:443 \
    -p 5432:5432 \
    benkl/playground \
    /bin/bash -c "python ./extract/get_reddit_data.py && python ./extract/scrape_news_sites.py"
