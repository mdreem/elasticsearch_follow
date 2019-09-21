import datetime
import dateutil.parser
import json
import time
from collections import deque


class ElasticsearchFollow:
    BASE_QUERY = {'query': {'bool': {'must': []}}}

    def __init__(self, elasticsearch):
        self.es = elasticsearch

        self.added_entries = set()

    def get_entries_since(self, index, timestamp):
        query_since = dict(self.BASE_QUERY)
        query_since['query']['bool']['must'].append({
            'range': {
                "@timestamp": {
                    "gt": timestamp
                }
            }
        })
        res = self.es.search(index=index,
                             scroll='2m',
                             doc_type="doc",
                             body=query_since)
        sid = res['_scroll_id']
        hits = res['hits']['hits']

        while res['hits']['hits']:
            res = self.es.scroll(scroll_id=sid, scroll='2m')
            sid = res['_scroll_id']
            current_hits = res['hits']['hits']
            if current_hits:
                hits = hits + current_hits

        return hits

    def get_new_lines(self, index, timestamp):
        entries = self.get_entries_since(index, timestamp)
        lines = []

        for entry in entries:
            id = entry['_id']
            if id not in self.added_entries:
                lines.append(entry['_source'])
                self.added_entries.add(id)

        return lines
