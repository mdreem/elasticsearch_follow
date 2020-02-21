from datetime import datetime

from dateutil import tz
from elasticsearch import Elasticsearch

import elasticsearch_follow
from .elasticsearch_integration_base import TestElasticsearchIntegrationBase


class TestElasticsearchFollowIntegration(TestElasticsearchIntegrationBase):
    def test_query_line(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC))

        es = Elasticsearch(["http://localhost:9200"])
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        new_lines = list(es_follow.get_new_lines('test_index', datetime(year=2019, month=1, day=1, hour=10, minute=0, tzinfo=tz.UTC)))
        print('Received: {}'.format(new_lines))

        self.assertEqual(len(new_lines), 1)
        self.assertIn('message', new_lines[0])
        self.assertEqual(new_lines[0]['message'], 'testMessage')

    def test_query_lines_returned_ordered_by_timestamp(self):
        self.delete_index('test_index')
        self.insert_line(message='firstMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC))
        self.insert_line(message='thirdMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=3, tzinfo=tz.UTC))
        self.insert_line(message='secondMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=tz.UTC))

        es = Elasticsearch(["http://localhost:9200"])
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        new_lines = list(es_follow.get_new_lines('test_index', datetime(year=2019, month=1, day=1, hour=10, minute=0, tzinfo=tz.UTC)))
        print('Received: {}'.format(new_lines))

        self.assertEqual(len(new_lines), 3)

        self.assertIn('message', new_lines[0])
        self.assertEqual(new_lines[0]['message'], 'firstMessage')

        self.assertIn('message', new_lines[1])
        self.assertEqual(new_lines[1]['message'], 'secondMessage')

        self.assertIn('message', new_lines[2])
        self.assertEqual(new_lines[2]['message'], 'thirdMessage')

    def test_query_string_returns_correct_results(self):
        self.delete_index('test_index')
        self.insert_line(message='firstMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC))
        self.insert_line(message='secondMessage', timestamp=datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=tz.UTC))

        query_string = 'message:firstMessage'
        es = Elasticsearch(["http://localhost:9200"])
        es_follow = elasticsearch_follow.ElasticsearchFollow(elasticsearch=es, query_string=query_string)

        new_lines = list(es_follow.get_new_lines('test_index', datetime(year=2019, month=1, day=1, hour=10, minute=0, tzinfo=tz.UTC)))
        print('Received: {}'.format(new_lines))

        self.assertEqual(len(new_lines), 1)
        self.assertIn('message', new_lines[0])
        self.assertEqual(new_lines[0]['message'], 'firstMessage')
