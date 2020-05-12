from copy import deepcopy

from dateutil.parser import parse

from .elasticsearch_fetch import ElasticsearchFetch
from .entry_tracker import EntryTracker


class ElasticsearchFollow:
    def __init__(self, elasticsearch, timestamp_field="@timestamp", query_string=None):
        """
        :param elasticsearch: elasticsearch instance from the ``elasticsearch``-library.
        :param timestamp_field: Denotes which field in the elasticsearch-index is used
            as the timestamp.
        :param query_string: The query used to fetch data from Elasticsearch.
        """
        self.es = elasticsearch
        self.es_fetch = ElasticsearchFetch(
            elasticsearch=elasticsearch, timestamp_field=timestamp_field
        )
        self.timestamp_field = timestamp_field

        self.entry_tracker = EntryTracker()

        self.base_query = {
            "sort": [{self.timestamp_field: "asc"}],
            "query": {"bool": {"must": []}},
        }

        if query_string:
            self.base_query["query"]["bool"]["must"].append(
                {"query_string": {"query": query_string}}
            )

    def get_entries_since(self, index, timestamp):
        """
        Yield all entries since ``timestamp`` until now from ``index``.

        :param index: Elasticsearch index from which new entries should
            be retrieved. Wildcards are supported.
        :param timestamp: Timestamp from whoch to start fetching lines.
        :return: Yields entries until no entries are left.
        """
        query_since = deepcopy(self.base_query)
        query_since["query"]["bool"]["must"].append(
            {"range": {self.timestamp_field: {"gt": timestamp}}}
        )

        res = self.es.search(index=index, scroll="2m", body=query_since)
        yield from self.es_fetch.get_hits(res)

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
            entry_id = entry["_id"]
            if entry_id not in self.entry_tracker:
                new_line = entry["_source"]

                entry_timestamp = parse(new_line[self.timestamp_field])

                self.entry_tracker.add(entry_id, entry_timestamp)
                yield new_line

    def prune_before(self, timestamp):
        """
        Removes All entries before  ``timestamp`` from the internal buffer.
        :param timestamp: All entries in the internal before this timestamp will be removed.
        """
        self.entry_tracker.prune_before(timestamp)
