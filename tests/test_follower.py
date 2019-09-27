from datetime import datetime
from unittest.mock import Mock, patch

from dateutil import tz

import elasticsearch_follow
from tests import generate_query_response, generate_basic_query_response


class TestFollower:
    def test_follower_returns_added_entry(self):
        datetime_mock = Mock(wraps=datetime)

        now = datetime(year=2019, month=1, day=1, hour=10, minute=0)
        datetime_mock.utcnow = Mock(return_value=now)
        patch('elasticsearch_follow.follower.datetime.datetime', new=datetime_mock).start()

        es = Mock()
        es.scroll.return_value = generate_query_response([])

        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        follower = elasticsearch_follow.Follower(es_follow, 'some_index', 120)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=1)
        timestamp = timestamp.replace(tzinfo=tz.UTC)

        es.search.return_value = generate_basic_query_response('id_1', 'line1', timestamp)
        generator = follower.generator()
        assert next(generator)['msg'] == 'line1'
        assert next(generator) is None

        es.search.return_value = generate_basic_query_response('id_2', 'line2', timestamp)
        assert next(generator)['msg'] == 'line2'
        assert next(generator) is None
