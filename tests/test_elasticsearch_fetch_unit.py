import unittest
from unittest.mock import Mock

import elasticsearch_follow

SEARCH_RESULT = ["some_value"]


class TestElasticsearchFetchUnit(unittest.TestCase):
    def test_fetch_one_surrounding_line_search_happens(self):
        es = Mock()
        es.search = Mock(return_value=SEARCH_RESULT)

        es_fetch = elasticsearch_follow.ElasticsearchFetch(es)

        result = es_fetch.search_nearby(
            index="test-index", timestamp=1, doc_id=2, after=True, number=1
        )

        self.assertTrue(es.search.called)
        self.assertEqual(result, SEARCH_RESULT)

    def test_fetch_no_surrounding_line_no_search_happens(self):
        es = Mock()
        es_fetch = elasticsearch_follow.ElasticsearchFetch(es)

        result = es_fetch.search_nearby(
            index="test-index", timestamp=1, doc_id=2, after=True, number=0
        )

        self.assertFalse(es.search.called)
        self.assertEqual(result, [])
