from datetime import datetime

from dateutil import tz
from elasticsearch import Elasticsearch

import elasticsearch_follow

TIMESTAMP_ONE = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)
TIMESTAMP_TWO = datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=tz.UTC)
TIMESTAMP_THREE = datetime(year=2019, month=1, day=1, hour=10, minute=3, tzinfo=tz.UTC)
TIMESTAMP_FOUR = datetime(year=2019, month=1, day=1, hour=10, minute=4, tzinfo=tz.UTC)
TIMESTAMP_FIVE = datetime(year=2019, month=1, day=1, hour=10, minute=5, tzinfo=tz.UTC)


class TestElasticsearchFetch:
    def find_hit(self, hits, message):
        return next((hit for hit in hits if hit['_source']['message'] == message), None)

    def insert_line(self, message, timestamp):
        es = Elasticsearch(["http://localhost:9200"])
        es.info()

        doc = {
            'message': message,
            '@timestamp': timestamp,
        }

        res = es.index(index='test_index', body=doc)
        print('Inserting line: {}'.format(res))

        query = {
            'size': 10000,
            'query': {
                'match_all': {}
            }
        }
        while True:
            res = es.search(index='test_index', body=query)
            hits = res['hits']['hits']
            if hits and self.find_hit(hits, message):
                print('Found ({}): {}'.format(message, hits))
                break
        print('Check: {}'.format(res['hits']['hits']))

    def delete_index(self, name):
        es = Elasticsearch(["http://localhost:9200"])
        es.indices.delete(index=name, ignore=[400, 404])

    def assert_source_with_message_in_line(self, lines, line_number, message):
        assert '_source' in lines[line_number]
        assert 'message' in lines[line_number]['_source']
        assert lines[line_number]['_source']['message'] == message

    def assert_message_in_line(self, lines, line_number, message):
        assert 'message' in lines[line_number]
        assert lines[line_number]['message'] == message

    def assert_sort_field_expected(self, lines, line_number, timestamp, doc_id):
        assert 'sort' in lines[line_number]
        assert lines[line_number]['sort'] == [timestamp.timestamp() * 1000.0, doc_id]

    def test_search_lines_in_order(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage1', timestamp=TIMESTAMP_ONE)
        self.insert_line(message='testMessage2', timestamp=TIMESTAMP_TWO)
        self.insert_line(message='testMessage3', timestamp=TIMESTAMP_THREE)

        es = Elasticsearch(["http://localhost:9200"])
        es_fetch = elasticsearch_follow.ElasticsearchFetch(es)

        new_lines = es_fetch.search('test_index')
        new_lines = new_lines['hits']['hits']
        print('Received: {}'.format(new_lines))

        assert len(new_lines) == 3
        self.assert_source_with_message_in_line(new_lines, 0, 'testMessage1')
        self.assert_source_with_message_in_line(new_lines, 1, 'testMessage2')
        self.assert_source_with_message_in_line(new_lines, 2, 'testMessage3')

        self.assert_sort_field_expected(new_lines, 0, TIMESTAMP_ONE, 0)
        self.assert_sort_field_expected(new_lines, 1, TIMESTAMP_TWO, 1)
        self.assert_sort_field_expected(new_lines, 2, TIMESTAMP_THREE, 2)

    def test_search_lines_out_of_order(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage1', timestamp=TIMESTAMP_ONE)
        self.insert_line(message='testMessage2', timestamp=TIMESTAMP_THREE)
        self.insert_line(message='testMessage3', timestamp=TIMESTAMP_TWO)

        es = Elasticsearch(["http://localhost:9200"])
        es_fetch = elasticsearch_follow.ElasticsearchFetch(es)

        new_lines = es_fetch.search('test_index')
        new_lines = new_lines['hits']['hits']
        print('Received: {}'.format(new_lines))

        assert len(new_lines) == 3
        self.assert_source_with_message_in_line(new_lines, 0, 'testMessage1')
        self.assert_source_with_message_in_line(new_lines, 1, 'testMessage3')
        self.assert_source_with_message_in_line(new_lines, 2, 'testMessage2')

        self.assert_sort_field_expected(new_lines, 0, TIMESTAMP_ONE, 0)
        self.assert_sort_field_expected(new_lines, 1, TIMESTAMP_TWO, 2)
        self.assert_sort_field_expected(new_lines, 2, TIMESTAMP_THREE, 1)

    def test_search_lines_with_query_string(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage1', timestamp=TIMESTAMP_ONE)
        self.insert_line(message='testMessage2', timestamp=TIMESTAMP_TWO)
        self.insert_line(message='testMessage3', timestamp=TIMESTAMP_THREE)

        es = Elasticsearch(["http://localhost:9200"])
        es_fetch = elasticsearch_follow.ElasticsearchFetch(es)

        new_lines = es_fetch.search('test_index', query_string='message:testMessage2')
        new_lines = new_lines['hits']['hits']
        print('Received: {}'.format(new_lines))

        assert len(new_lines) == 1
        self.assert_source_with_message_in_line(new_lines, 0, 'testMessage2')
        self.assert_sort_field_expected(new_lines, 0, TIMESTAMP_TWO, 1)

    def test_search_nearby_lines(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage1', timestamp=TIMESTAMP_ONE)
        self.insert_line(message='testMessage2', timestamp=TIMESTAMP_TWO)
        self.insert_line(message='testMessage3', timestamp=TIMESTAMP_THREE)
        self.insert_line(message='testMessage4', timestamp=TIMESTAMP_FOUR)
        self.insert_line(message='testMessage5', timestamp=TIMESTAMP_FIVE)

        es = Elasticsearch(["http://localhost:9200"])
        es_fetch = elasticsearch_follow.ElasticsearchFetch(es)

        middle_timestamp = TIMESTAMP_THREE.timestamp() * 1000
        middle_docid = 2

        print(middle_timestamp)
        print(middle_docid)

        results_before = es_fetch.search_nearby('test_index', timestamp=middle_timestamp, doc_id=middle_docid, after=False, number=2)
        results_before = results_before['hits']['hits']
        results_after = es_fetch.search_nearby('test_index', timestamp=middle_timestamp, doc_id=middle_docid, after=True, number=2)
        results_after = results_after['hits']['hits']

        print('results_before: {}'.format(results_before))
        print('results_after: {}'.format(results_after))

        assert len(results_before) == 2
        self.assert_source_with_message_in_line(results_before, 0, 'testMessage2')
        self.assert_source_with_message_in_line(results_before, 1, 'testMessage1')

        assert len(results_after) == 2
        self.assert_source_with_message_in_line(results_after, 0, 'testMessage4')
        self.assert_source_with_message_in_line(results_after, 1, 'testMessage5')

    def test_search_surrounding_lines(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage1', timestamp=TIMESTAMP_ONE)
        self.insert_line(message='testMessage2', timestamp=TIMESTAMP_TWO)
        self.insert_line(message='testMessage3', timestamp=TIMESTAMP_THREE)
        self.insert_line(message='testMessage4', timestamp=TIMESTAMP_FOUR)
        self.insert_line(message='testMessage5', timestamp=TIMESTAMP_FIVE)

        es = Elasticsearch(["http://localhost:9200"])
        es_fetch = elasticsearch_follow.ElasticsearchFetch(es)

        new_lines = es_fetch.search('test_index')
        new_lines = new_lines['hits']['hits']
        print('Received: {}'.format(new_lines))

        middle_timestamp = TIMESTAMP_THREE.timestamp() * 1000
        middle_docid = 2

        print(middle_timestamp)
        print(middle_docid)

        results = list(es_fetch.search_surrounding(index='test_index', num_before=2, num_after=2))

        print('results: {}'.format(results))

        assert len(results) == 5

        assert len(results[0]) == 3
        self.assert_message_in_line(results[0], 0, 'testMessage1')
        self.assert_message_in_line(results[0], 1, 'testMessage2')
        self.assert_message_in_line(results[0], 2, 'testMessage3')

        assert len(results[1]) == 4
        self.assert_message_in_line(results[1], 0, 'testMessage1')
        self.assert_message_in_line(results[1], 1, 'testMessage2')
        self.assert_message_in_line(results[1], 2, 'testMessage3')
        self.assert_message_in_line(results[1], 3, 'testMessage4')

        assert len(results[2]) == 5
        self.assert_message_in_line(results[2], 0, 'testMessage1')
        self.assert_message_in_line(results[2], 1, 'testMessage2')
        self.assert_message_in_line(results[2], 2, 'testMessage3')
        self.assert_message_in_line(results[2], 3, 'testMessage4')
        self.assert_message_in_line(results[2], 4, 'testMessage5')

        assert len(results[3]) == 4
        self.assert_message_in_line(results[3], 0, 'testMessage2')
        self.assert_message_in_line(results[3], 1, 'testMessage3')
        self.assert_message_in_line(results[3], 2, 'testMessage4')
        self.assert_message_in_line(results[3], 3, 'testMessage5')

        assert len(results[4]) == 3
        self.assert_message_in_line(results[4], 0, 'testMessage3')
        self.assert_message_in_line(results[4], 1, 'testMessage4')
        self.assert_message_in_line(results[4], 2, 'testMessage5')
