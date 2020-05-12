class ElasticsearchFetch:
    def __init__(self, elasticsearch, timestamp_field="@timestamp"):
        """
        :param elasticsearch: elasticsearch instance from the ``elasticsearch``-library.
        :param timestamp_field: Denotes which field in the elasticsearch-index is used
            as the timestamp.
        """
        self.es = elasticsearch
        self.timestamp_field = timestamp_field

    def search(self, index, query_string=None, from_time=None, to_time=None):
        """
        :param index: The index to search in. May contain wildcards.
        :param query_string: The query string.
        :param to_time: Upper bound of time to query.
        :param from_time: Lower bound of time to query.
        :return: A list with results of the query, including the sort-parameter which
        can be used with search_nearby.
        """
        query = {
            "sort": [{self.timestamp_field: "asc"}, {"_doc": "desc"}],
            "query": {"bool": {"must": []}},
        }

        if query_string:
            query["query"]["bool"]["must"].append(
                {"query_string": {"query": query_string}}
            )

        if from_time or to_time:
            query_range = {self.timestamp_field: {}}

            if from_time:
                query_range[self.timestamp_field]["gte"] = from_time
            if to_time:
                query_range[self.timestamp_field]["lte"] = to_time

            query["query"]["bool"]["must"].append({"range": query_range})

        return self.es.search(index=index, scroll="2m", body=query)

    def search_nearby(self, index, timestamp, doc_id, after=True, number=0):
        """
        Fetching
        :param index: The index to search in. May contain wildcards.
        :param timestamp: The number returned as the first parameter in 'sort' return from search.
        :param doc_id: The number returned as the second parameter in 'sort' return from search.
        :param after: If after=True, search after the baseline. If after=False, search after baseline.
        :param number: Number of lines after the entry defined by timestamp and doc_id
        :return: Returns the result of the Elasticsearch query.
        """

        if number <= 0:
            return []

        if after:
            query = {
                "search_after": [timestamp, doc_id],
                "size": number,
                "sort": [{self.timestamp_field: "asc"}, {"_doc": "desc"}],
            }
        else:
            query = {
                "search_after": [timestamp, doc_id],
                "size": number,
                "sort": [{self.timestamp_field: "desc"}, {"_doc": "asc"}],
            }

        return self.es.search(index=index, body=query)

    @staticmethod
    def _extract_source(line):
        if not line:
            return []
        if line["hits"]["hits"]:
            return list(map(lambda x: x["_source"], line["hits"]["hits"]))
        else:
            return []

    def search_surrounding(
        self,
        index,
        query_string=None,
        from_time=None,
        to_time=None,
        num_before=0,
        num_after=0,
    ):
        """
        Fetches the line found by query_string as well as lines before and after as given
        by num_before and num_after.

        :param index: The index to search in. May contain wildcards.
        :param query_string: The query used to fetch data from Elasticsearch.
        :param to_time: Upper bound of time to query.
        :param from_time: Lower bound of time to query.
        :param num_before: Number of lines to fetch before a hit.
        :param num_after: Number of lines to fetch after a hit.
        :return: Returns a list of lists, where the sublists contain the found and
        the surrounding documents.
        """
        search_result = self.search(
            index, query_string, from_time=from_time, to_time=to_time
        )

        for hit in self.get_hits(search_result):
            line = hit["_source"]
            line_timestamp, line_doc_id = hit["sort"]

            lines_before = self.search_nearby(
                index=index,
                timestamp=line_timestamp,
                doc_id=line_doc_id,
                after=False,
                number=num_before,
            )
            lines_after = self.search_nearby(
                index=index,
                timestamp=line_timestamp,
                doc_id=line_doc_id,
                after=True,
                number=num_after,
            )

            yield list(reversed(self._extract_source(lines_before))) + [
                line
            ] + self._extract_source(lines_after)

    def get_hits(self, search_result):
        """
        Fetches all hits from a search_result. Iterates through all pages via the scroll_id.

        :param search_result: The result of an ElasticSearch.search-request.
        :return: Yields the resulting documents one by one.
        """
        res = search_result
        scroll_id = res["_scroll_id"]
        hits = res["hits"]["hits"]

        for hit in hits:
            yield hit

        while res["hits"]["hits"]:
            res = self.es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = res["_scroll_id"]
            current_hits = res["hits"]["hits"]
            for hit in current_hits:
                yield hit

        self.es.clear_scroll(scroll_id=scroll_id)
