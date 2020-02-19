import heapq
from copy import deepcopy
from functools import total_ordering

import pytz
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
    def __init__(self, elasticsearch, timestamp_field='@timestamp', query_string=None):
        """
        :param elasticsearch: elasticsearch instance from the ``elasticsearch``-library.
        :param timestamp_field: Denotes which field in the elasticsearch-index is used
            as the timestamp.
        :param query_string: The query used to fetch data from elasticsearch.
        """
        self.es = elasticsearch
        self.timestamp_field = timestamp_field

        self.added_entries = set()
        self.entries_by_timestamp = []

        self.base_query = {
            "sort": [
                {self.timestamp_field: "asc"}
            ],
            'query': {'bool': {'must': []}}
        }

        if query_string:
            self.base_query['query']['bool']['must'].append({
                'query_string': {
                    'query': query_string
                }
            })

    def get_entries_since(self, index, timestamp):
        """
        Yield all entries since ``timestamp`` until now from ``index``.

        :param index: Elasticsearch index from which new entries should
            be retrieved. Wildcards are supported.
        :param timestamp: Timestamp from whoch to start fetching lines.
        :return: Yields entries until no entries are left.
        """
        query_since = deepcopy(self.base_query)
        query_since['query']['bool']['must'].append({
            'range': {
                self.timestamp_field: {
                    "gt": timestamp
                }
            }
        })

        res = self.es.search(index=index,
                             scroll='2m',
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

        self.es.clear_scroll(scroll_id=scroll_id)

    def get_new_lines(self, index, timestamp):
        """
        Retrieves new lines starting with timestamp until now.
        Only yield entries which have not yet been fetched from elasticsearch.

        :param index: Elasticsearch index from which new entries should
            be retrieved. Wildcards are supported.
        :param timestamp: Timestamp from whoch to start fetching lines.
        :return: Yields the new lines until the list is empty.
        """
        entries = self.get_entries_since(index, timestamp)

        for entry in entries:
            entry_id = entry['_id']
            if entry_id not in self.added_entries:
                new_line = entry['_source']
                self._update_entries_by_timestamp(entry_id, new_line)

                self.added_entries.add(entry_id)
                yield new_line

    def _update_entries_by_timestamp(self, entry_id, new_line):
        entry_timestamp = parse(new_line[self.timestamp_field])
        if not entry_timestamp.tzinfo:
            entry_timestamp = pytz.utc.localize(entry_timestamp)
        heapq.heappush(self.entries_by_timestamp, Entry(timestamp=entry_timestamp, entry_id=entry_id))

    def prune_before(self, timestamp):
        """
        Removes All entries before  ``timestamp`` from the internal buffer.
        :param timestamp: All entries in the internal before this timestamp will be removed.
        """
        while len(self.entries_by_timestamp) > 0:
            oldest_entry = heapq.heappop(self.entries_by_timestamp)
            if oldest_entry.timestamp <= timestamp:
                self.added_entries.remove(oldest_entry.entry_id)
            else:
                heapq.heappush(self.entries_by_timestamp, oldest_entry)
                return
