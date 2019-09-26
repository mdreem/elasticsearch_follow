from datetime import datetime
from unittest.mock import Mock

import elasticsearch_follow
from .query_generator import generate_basic_query_response, generate_scroll


class TestFollower:
    def test_main(self):
        es = Mock()
        es.scroll.return_value = generate_scroll([])

        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        follower = elasticsearch_follow.Follower(es_follow, 'some_index', 60)

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=1)

        es.search.return_value = generate_basic_query_response('id_1', 'line1', timestamp)
        generator = follower.generator()
        assert next(generator)['msg'] == 'line1'
        assert next(generator) is None

        es.search.return_value = generate_basic_query_response('id_2', 'line2', timestamp)
        assert next(generator)['msg'] == 'line2'
        assert next(generator) is None
