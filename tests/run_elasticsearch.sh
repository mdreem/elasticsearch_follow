#!/bin/bash

ES_VERSION=${ES_VERSION:-"7.0.0"}
SERVER_HOSTNAME=${SERVER_HOSTNAME:-"http://localhost:9200"}

exec docker run -d \
    -e ES_HOST=$SERVER_HOSTNAME \
    -e node.name=test \
    -e cluster.initial_master_nodes=test \
    -p "9200:9200" \
    docker.elastic.co/elasticsearch/elasticsearch-oss:$ES_VERSION
