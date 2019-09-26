from datetime import datetime
from unittest.mock import Mock

import elasticsearch_follow
from .query_generator import generate_basic_query_response, generate_scroll


class TestMain:
    def test_main(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        es.search.return_value = generate_basic_query_response('id_1', 'line1', datetime(year=2019, month=1, day=1, hour=10, minute=1))
        es.scroll.return_value = generate_scroll([])

        new_lines = es_follow.get_new_lines('my_index', None)

        assert len(new_lines) == 1
        assert 'msg' in new_lines[0]
        assert new_lines[0]['msg'] == 'line1'

    def test_prune_only_element(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        es.search.return_value = generate_basic_query_response('id_1', 'line1', datetime(year=2019, month=1, day=1, hour=10, minute=0))
        es.scroll.return_value = generate_scroll([])

        new_lines = es_follow.get_new_lines('my_index', None)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=10)
        es_follow.prune_before(timestamp)

        assert len(es_follow.added_entries) == 0
        assert len(es_follow.entries_by_timestamp) == 0

    def test_prune_existing_remains(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        entry_timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=10)
        entry_id = 'id_1'
        es.search.return_value = generate_basic_query_response(entry_id, 'line1', entry_timestamp)
        es.scroll.return_value = generate_scroll([])

        es_follow.get_new_lines('my_index', None)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=0)
        es_follow.prune_before(timestamp)

        assert len(es_follow.added_entries) == 1
        assert entry_id in es_follow.added_entries

        assert len(es_follow.entries_by_timestamp) == 1
        remaining_entry = es_follow.entries_by_timestamp[0]
        assert remaining_entry.entry_id == entry_id
        assert remaining_entry.timestamp == entry_timestamp

    def test_prune_no_elements_existing(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=10)
        es_follow.prune_before(timestamp)

        assert len(es_follow.added_entries) == 0
        assert len(es_follow.entries_by_timestamp) == 0
