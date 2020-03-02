# Follow Elasticsearch continuously
[![CircleCI](https://circleci.com/gh/mdreem/elasticsearch_follow.svg?style=svg&circle-token=a53243ea7942ee439f51be3ea4fce2628ed4d58f)](https://circleci.com/gh/mdreem/elasticsearch_follow)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d192317c5ff74fd7a17dc5c0c2f13317)](https://www.codacy.com/manual/mdreem/elasticsearch_follow?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=mdreem/elasticsearch_follow&amp;utm_campaign=Badge_Grade)
[![Coverage Status](https://coveralls.io/repos/github/mdreem/elasticsearch_follow/badge.svg?branch=master)](https://coveralls.io/github/mdreem/elasticsearch_follow?branch=master)

## Overview

elasticsearch_follow is library helping to query Elasticsearch continuously.

It needs <https://github.com/elastic/elasticsearch-py> as a dependency.

elasticsearch_follow acts as a wrapper for elasticsearch-py and handles various
use-cases, like following logs by polling elasticsearch continuously and fetching
loglines via a generator. It is possible to easily fetch lines surrounding a
given logline.

### How to poll Elasticsearch continuously

The polling logic is implemented in the class ElasticsearchFollow, which needs
an Elasircsearch object from elasticsearch-py. The class Follower takes an
ElasticsearchFollow-object and has a method to create a generator which yields
loglines until all elements of a query have been returned. After this a new
generator has to be created and used.

### How to fetch log-lines from Elasticsearch

To just fetch loglines, one can use ElasticsearchFetch which has
a search_surrounding. This returns a list of lists, where each list contains
the queried loglines and the lines before and after as requested by the parameters 
num_before and num_after.

## Installation

You can install the ``elasticsearch`` package with pip:

```bash
pip install elasticsearch_follow
```

See also: <https://pypi.org/project/elasticsearch-follow/>

## Exampluse use

This package introduces the command line tool ``es_tail`` which can be used for
following logs written to Elasticsearch and directly fetching log lines by a query.
It is possible to configure the output via an format string.

```bash
# Follow the logs written to the indexes starting with logstash.
# Print the fieds @timestamp and message
es_tail -c "http://localhost:9200" tail --index "logstash*" -f "{@timestamp} {message}" 


# Fetch all logs in the last hour with the field loglevel contains ERROR and fetch the two lines before and after.
# Print the fieds @timestamp and message
es_tail -c "http://localhost:9200" fetch --index "logstash" -f "{@timestamp} {message}"  --query loglevel:ERROR -A 2 -B 2 -F "now-1h" 


# It is also possible to print nested fields
es_tail -c "http://localhost:9200" fetch --index "logstash" -f "{@timestamp} {message} {kv[field]} {kv[nested][field]}" -F "now-1h" 
```

## Example use of the library

```python
from elasticsearch import Elasticsearch
from elasticsearch_follow import ElasticsearchFollow, Follower

es = Elasticsearch()
es_follow = ElasticsearchFollow(elasticsearch=es)

# The Follower is used to get a generator which yields new 
# elements until it runs out. time_delta give the number of
# seconds to look into the past.
follower = Follower(elasticsearch_follow=es_follow, index='some-index', time_delta=60)

while True:
    entries = follower.generator()
    for entry in entries:
        print(entry)
    time.sleep(0.1)
```