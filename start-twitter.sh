#!/bin/bash

flink run -p 1 -c io.eventador.flinktwitter.FlinkTwitter ./target/flink-twitter-1.0-SNAPSHOT.jar \
    --consumer_key "CONSUMER_KEY" \
    --consumer_secret "CONSUMER_SECRET" \
    --token "TOKEN" \
    --token_secret "TOKEN_SECRET" \
    --topic "hashtags" \
    --bootstrap.servers "localhost:9092"
