#!/bin/bash
until curl http://localhost:9200/_cluster/health
do
  echo "Waiting"
  sleep 0.1
done
