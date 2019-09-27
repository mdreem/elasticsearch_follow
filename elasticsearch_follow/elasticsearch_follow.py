import heapq
from functools import total_ordering

from dateutil.parser import parse


@total_ordering
class Entry:
    def __init__(self, timestamp, entry_id):
        self.timestamp = timestamp
        self.entry_id = entry_id

    def __eq__(self, other):
        return (self.timestamp, self.entry_id) == (other.timestamp, other.entry_id)

    def __lt__(self, other):
        return self.timestamp < other.timestamp


class ElasticsearchFollow:
    BASE_QUERY = {'query': {'bool': {'must': []}}}

    def __init__(self, elasticsearch, timestamp_field='@timestamp'):
        self.es = elasticsearch
        self.timestamp_field = timestamp_field

        self.added_entries = set()
        self.entries_by_timestamp = []

    def get_entries_since(self, index, timestamp):
        query_since = dict(self.BASE_QUERY)
        query_since['query']['bool']['must'].append({
            'range': {
                self.timestamp_field: {
                    "gt": timestamp
                }
            }
        })

        res = self.es.search(index=index,
                             scroll='2m',
                             doc_type='doc',
                             body=query_since)
        scroll_id = res['_scroll_id']
        hits = res['hits']['hits']

        for hit in hits:
            yield hit

        while res['hits']['hits']:
            res = self.es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = res['_scroll_id']
            current_hits = res['hits']['hits']
            for hit in current_hits:
                yield hit

    def get_new_lines(self, index, timestamp):
        entries = self.get_entries_since(index, timestamp)

        for entry in entries:
            entry_id = entry['_id']
            if entry_id not in self.added_entries:
                new_line = entry['_source']
                entry_timestamp = parse(new_line[self.timestamp_field])
                heapq.heappush(self.entries_by_timestamp, Entry(timestamp=entry_timestamp, entry_id=entry_id))

                self.added_entries.add(entry_id)
                yield new_line

    def prune_before(self, timestamp):
        while len(self.entries_by_timestamp) > 0:
            oldest_entry = heapq.heappop(self.entries_by_timestamp)
            if oldest_entry.timestamp <= timestamp:
                self.added_entries.remove(oldest_entry.entry_id)
            else:
                heapq.heappush(self.entries_by_timestamp, oldest_entry)
                return
