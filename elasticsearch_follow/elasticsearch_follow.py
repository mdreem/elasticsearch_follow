from sortedcontainers import SortedList
from functools import total_ordering


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
        self.entries_by_timestamp = SortedList()

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

        while res['hits']['hits']:
            res = self.es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = res['_scroll_id']
            current_hits = res['hits']['hits']
            if current_hits:
                hits = hits + current_hits

        return hits

    def get_new_lines(self, index, timestamp):
        entries = self.get_entries_since(index, timestamp)
        lines = []

        for entry in entries:
            entry_id = entry['_id']
            if entry_id not in self.added_entries:
                new_line = entry['_source']
                lines.append(new_line)
                self.entries_by_timestamp.add(Entry(entry_id, new_line[self.timestamp_field]))

                self.added_entries.add(entry_id)

        return lines
