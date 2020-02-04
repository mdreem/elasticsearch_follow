# Follow Elasticsearch continuously
[![CircleCI](https://circleci.com/gh/mdreem/elasticsearch_follow.svg?style=svg&circle-token=a53243ea7942ee439f51be3ea4fce2628ed4d58f)](https://circleci.com/gh/mdreem/elasticsearch_follow)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d192317c5ff74fd7a17dc5c0c2f13317)](https://www.codacy.com/manual/mdreem/elasticsearch_follow?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=mdreem/elasticsearch_follow&amp;utm_campaign=Badge_Grade)
[![Coverage Status](https://coveralls.io/repos/github/mdreem/elasticsearch_follow/badge.svg?branch=master)](https://coveralls.io/github/mdreem/elasticsearch_follow?branch=master)

elasticsearch_follow is library helping to query Elasticsearch continuously.

It needs https://github.com/elastic/elasticsearch-py as a dependency.

# Installation

You can install the ``elasticsearch`` package with pip:

```
pip install elasticsearch_follow
```

See also: https://pypi.org/project/elasticsearch-follow/

# Example use

```
from elasticsearch import Elasticsearch
from elasticsearch_follow import ElasticsearchFollow, Follower

es = Elasticsearch()
es_follow = elasticsearch_follow.ElasticsearchFollow(elasticsearch=es)

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