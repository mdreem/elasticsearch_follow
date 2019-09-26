from datetime import datetime

from elasticsearch import Elasticsearch

import elasticsearch_follow


class TestElasticsearch:

    def insert_line(self):
        es = Elasticsearch(["http://localhost:9200"])
        es.info()

        doc = {
            'message': 'testMessage',
            '@timestamp': datetime(year=2019, month=1, day=1, hour=10, minute=1),
        }

        res = es.index(index='test_index', doc_type='doc', id=1, body=doc)
        print('Insert line: {}'.format(res))

        query = {
            'size': 10000,
            'query': {
                'match_all': {}
            }
        }
        while True:
            res = es.search(index='test_index', doc_type='doc', body=query, scroll='1m')
            if res['hits']['hits']:
                break
        print('Check: {}'.format(res['hits']['hits']))

    def delete_index(self, name):
        es = Elasticsearch(["http://localhost:9200"])
        es.indices.delete(index=name, ignore=[400, 404])

    def test_query_line(self):
        self.delete_index('test_index')
        self.insert_line()

        es = Elasticsearch(["http://localhost:9200"])
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        new_lines = es_follow.get_new_lines('test_index', datetime(year=2019, month=1, day=1, hour=10, minute=0))
        print('Received: {}'.format(new_lines))

        assert len(new_lines) == 1
        assert 'message' in new_lines[0]
        assert new_lines[0]['message'] == 'testMessage'
