#!/usr/bin/env python3

import argparse
import time

import elasticsearch

import elasticsearch_follow


class ExampleProcessor:
    def process_line(self, line):
        entries = [str(line[key]) for key in sorted(line.keys())]
        return ' '.join(entries)


def run(host, index):
    es = elasticsearch.Elasticsearch([host])
    es_follow = elasticsearch_follow.ElasticsearchFollow(es)

    processor = ExampleProcessor()
    follower = elasticsearch_follow.Follower(elasticsearch_follow=es_follow, index=index, time_delta=60, processor=processor)

    print('Started...')
    while True:
        entries = follower.generator()
        for entry in entries:
            print(entry)
        time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='http://localhost:9200', type=str)
    parser.add_argument('--index', default='test_index', type=str)

    args = parser.parse_args()
    run(args.host)
