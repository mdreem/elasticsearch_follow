from datetime import datetime
from unittest.mock import Mock

from dateutil import tz

import elasticsearch_follow
from tests import generate_basic_query_response, generate_query_response, generate_hit_entry


class TestElasticsearchFollowUnit:
    def test_fetch_one_line(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        timestamp_first_entry = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)
        es.search.return_value = generate_basic_query_response('id_1', 'line1', timestamp_first_entry)
        es.scroll.return_value = generate_query_response([])

        new_lines = list(es_follow.get_new_lines('my_index', None))

        assert len(new_lines) == 1
        assert 'msg' in new_lines[0]
        assert new_lines[0]['msg'] == 'line1'

    def test_fetch_multiple_lines_with_scroll(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)
        es.search.return_value = generate_query_response([generate_hit_entry('id_1', 'line1', timestamp),
                                                          generate_hit_entry('id_2', 'line2', timestamp)])

        scroll_response = generate_query_response([generate_hit_entry('id_3', 'line3', timestamp),
                                                   generate_hit_entry('id_4', 'line4', timestamp)])
        empty_scroll_response = generate_query_response([])
        es.scroll.side_effect = [scroll_response, empty_scroll_response]

        new_lines = list(es_follow.get_new_lines('my_index', None))

        assert len(new_lines) == 4
        messages = [e['msg'] for e in new_lines]
        assert messages == ['line1', 'line2', 'line3', 'line4']

    def test_prune_only_element(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        timestamp_first_entry = datetime(year=2019, month=1, day=1, hour=10, minute=0, tzinfo=tz.UTC)
        es.search.return_value = generate_basic_query_response('id_1', 'line1', timestamp_first_entry)
        es.scroll.return_value = generate_query_response([])

        list(es_follow.get_new_lines('my_index', None))

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=10, tzinfo=tz.UTC)
        es_follow.prune_before(timestamp)

        assert len(es_follow.entry_tracker.added_entries) == 0
        assert len(es_follow.entry_tracker.entries_by_timestamp) == 0

    def test_prune_existing_remains(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        entry_timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=10, tzinfo=tz.UTC)
        entry_id = 'id_1'
        es.search.return_value = generate_basic_query_response(entry_id, 'line1', entry_timestamp)
        es.scroll.return_value = generate_query_response([])

        list(es_follow.get_new_lines('my_index', None))

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=0, tzinfo=tz.UTC)
        es_follow.prune_before(timestamp)

        assert len(es_follow.entry_tracker.added_entries) == 1
        assert entry_id in es_follow.entry_tracker.added_entries

        assert len(es_follow.entry_tracker.entries_by_timestamp) == 1
        remaining_entry = es_follow.entry_tracker.entries_by_timestamp[0]
        assert remaining_entry.entry_id == entry_id
        assert remaining_entry.timestamp == entry_timestamp

    def test_prune_no_elements_existing(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=10, tzinfo=tz.UTC)
        es_follow.prune_before(timestamp)

        assert len(es_follow.entry_tracker.added_entries) == 0
        assert len(es_follow.entry_tracker.entries_by_timestamp) == 0

    def test_set_missing_timestamp_to_entry(self):
        es = Mock()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        timestamp_first_entry = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)
        es.search.return_value = generate_basic_query_response('id_1', 'line1', timestamp_first_entry, False)
        es.scroll.return_value = generate_query_response([])

        new_lines = list(es_follow.get_new_lines('my_index', None))

        assert len(new_lines) == 1
        assert 'msg' in new_lines[0]
        assert new_lines[0]['msg'] == 'line1'
        print(new_lines)
