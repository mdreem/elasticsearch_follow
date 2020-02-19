class ElasticsearchFetch:
    def __init__(self, elasticsearch, timestamp_field='@timestamp'):
        """
        :param elasticsearch: elasticsearch instance from the ``elasticsearch``-library.
        :param timestamp_field: Denotes which field in the elasticsearch-index is used
            as the timestamp.
        :param query_string: The query used to fetch data from elasticsearch.
        """
        self.es = elasticsearch
        self.timestamp_field = timestamp_field

    def search(self, index, query_string=None):
        """
        :param index: The index to search in. May contain wildcards.
        :param query_string: The query string.
        :return: A list with results of the query, including the sort-parameter which
        can be used with search_nearby.
        """
        query = {
            "sort": [
                {self.timestamp_field: 'asc'},
                {'_doc': 'desc'}
            ],
            'query': {'bool': {'must': []}}
        }

        if query_string:
            query['query']['bool']['must'].append({
                'query_string': {
                    'query': query_string
                }
            })

        res = self.es.search(index=index,
                             scroll='2m',
                             body=query)
        return res

    def search_nearby(self, index, timestamp, doc_id, after=True, number=0):
        """
        Fetching
        :param index: The index to search in. May contain wildcards.
        :param timestamp: The number returned as the first parameter in 'sort' return from search.
        :param doc_id: The number returned as the second parameter in 'sort' return from search.
        :param after: If after=True, search after the baseline. If after=False, search after baseline.
        :param number: Number of lines after the entry defined by timestamp and doc_id
        :return:
        """

        if after:
            query = {
                'search_after': [timestamp, doc_id],
                'size': number,
                'sort': [
                    {self.timestamp_field: 'asc'},
                    {'_doc': 'desc'}
                ]
            }
        else:
            query = {
                'search_after': [timestamp, doc_id],
                'size': number,
                'sort': [
                    {self.timestamp_field: 'desc'},
                    {'_doc': 'asc'}
                ]
            }

        return self.es.search(index=index, body=query)

    def _extract_source(self, line):
        if line['hits']['hits']:
            return list(map(lambda x: x['_source'], line['hits']['hits']))
        else:
            return []

    def search_surrounding(self, index, query_string=None, num_before=0, num_after=0):
        search_result = self.search(index, query_string)

        for hit in self.get_hits(search_result):
            line = hit['_source']
            line_timestamp, line_doc_id = hit['sort']

            lines_before = self.search_nearby(index=index, timestamp=line_timestamp, doc_id=line_doc_id, after=False, number=num_before)
            lines_after = self.search_nearby(index=index, timestamp=line_timestamp, doc_id=line_doc_id, after=True, number=num_after)

            yield list(reversed(self._extract_source(lines_before))) + [line] + self._extract_source(lines_after)

    def get_hits(self, search_result):
        res = search_result
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
