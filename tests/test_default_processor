from unittest.mock import Mock

import elasticsearch_follow


class TestDefaultProcessor:
    def test_default_processor(self):
        processor = elasticsearch_follow.DefaultProcessor()
        es_follow = Mock()
        es_follow.get_new_lines.return_value = [{
            'msg': 'line1',
            '@timestamp': '2019-01-01T10:01:00'
        }]
        follower = elasticsearch_follow.Follower(es_follow, 'some_index', 120, processor)

        generator = follower.generator()

        assert next(generator) == '2019-01-01T10:01:00 line1'
