from datetime import datetime

from elasticsearch import Elasticsearch

import elasticsearch_follow


class TestElasticsearch:
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

    def test_query_line(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=1))

        es = Elasticsearch(["http://localhost:9200"])
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        new_lines = list(es_follow.get_new_lines('test_index', datetime(year=2019, month=1, day=1, hour=10, minute=0)))
        print('Received: {}'.format(new_lines))

        assert len(new_lines) == 1
        assert 'message' in new_lines[0]
        assert new_lines[0]['message'] == 'testMessage'

    def test_query_lines_returned_ordered_by_timestamp(self):
        self.delete_index('test_index')
        self.insert_line(message='firstMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=1))
        self.insert_line(message='thirdMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=3))
        self.insert_line(message='secondMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=2))

        es = Elasticsearch(["http://localhost:9200"])
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        new_lines = list(es_follow.get_new_lines('test_index', datetime(year=2019, month=1, day=1, hour=10, minute=0)))
        print('Received: {}'.format(new_lines))

        assert len(new_lines) == 3
        assert 'message' in new_lines[0]
        assert new_lines[0]['message'] == 'firstMessage'

        assert 'message' in new_lines[1]
        assert new_lines[1]['message'] == 'secondMessage'

        assert 'message' in new_lines[2]
        assert new_lines[2]['message'] == 'thirdMessage'

    def test_query_string_returns_correct_results(self):
        self.delete_index('test_index')
        self.insert_line(message='firstMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=1))
        self.insert_line(message='secondMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=2))

        query_string = 'message:firstMessage'
        es = Elasticsearch(["http://localhost:9200"])
        es_follow = elasticsearch_follow.ElasticsearchFollow(elasticsearch=es, query_string=query_string)

        new_lines = list(es_follow.get_new_lines('test_index', datetime(year=2019, month=1, day=1, hour=10, minute=0)))
        print('Received: {}'.format(new_lines))

        assert len(new_lines) == 1
        assert 'message' in new_lines[0]
        assert new_lines[0]['message'] == 'firstMessage'
