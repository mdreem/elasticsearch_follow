from datetime import datetime
from unittest.mock import Mock, patch

import elasticsearch_follow
from .query_generator import generate_basic_query_response, generate_scroll


class TestFollower:
    def test_main(self):
        datetime_mock = Mock(wraps=datetime)
        datetime_mock.utcnow = Mock(return_value=datetime(year=2019, month=1, day=1, hour=10, minute=0))
        patch('elasticsearch_follow.follower.datetime.datetime', new=datetime_mock).start()

        es = Mock()
        es.scroll.return_value = generate_scroll([])

        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        follower = elasticsearch_follow.Follower(es_follow, 'some_index', 120)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=1)

        es.search.return_value = generate_basic_query_response('id_1', 'line1', timestamp)
        generator = follower.generator()
        assert next(generator)['msg'] == 'line1'
        assert next(generator) is None

        es.search.return_value = generate_basic_query_response('id_2', 'line2', timestamp)
        assert next(generator)['msg'] == 'line2'
        assert next(generator) is None
