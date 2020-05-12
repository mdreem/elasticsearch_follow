import unittest
from unittest.mock import Mock

import elasticsearch_follow


class TestDefaultProcessor(unittest.TestCase):
    def test_formatting_processor_formats(self):
        processor = elasticsearch_follow.FormattingProcessor(
            format_string="{@timestamp} {msg}"
        )
        es_follow = Mock()
        es_follow.get_new_lines.return_value = [
            {"msg": "line1", "@timestamp": "2019-01-01T10:01:00"}
        ]
        follower = elasticsearch_follow.Follower(
            es_follow, "some_index", 120, processor
        )

        generator = follower.generator()

        self.assertEqual("2019-01-01T10:01:00 line1", next(generator))

    def test_formatting_processor_skips_missing_fields(self):
        processor = elasticsearch_follow.FormattingProcessor(
            format_string="->{@timestamp} {unknown}<-"
        )
        es_follow = Mock()
        es_follow.get_new_lines.return_value = [
            {"msg": "line1", "@timestamp": "2019-01-01T10:01:00"}
        ]
        follower = elasticsearch_follow.Follower(
            es_follow, "some_index", 120, processor
        )

        generator = follower.generator()

        self.assertEqual("->2019-01-01T10:01:00 <-", next(generator))

    def test_formatting_processor_nested_fields(self):
        processor = elasticsearch_follow.FormattingProcessor(
            format_string="{@timestamp} {kv[key_1]} {kv[key_2]} {kv[nested_key][key_3]}"
        )
        es_follow = Mock()
        es_follow.get_new_lines.return_value = [
            {
                "msg": "line1",
                "kv": {
                    "key_1": "value_1",
                    "key_2": "value_2",
                    "nested_key": {"key_3": "value_3"},
                },
                "@timestamp": "2019-01-01T10:01:00",
            }
        ]
        follower = elasticsearch_follow.Follower(
            es_follow, "some_index", 120, processor
        )

        generator = follower.generator()

        self.assertEqual("2019-01-01T10:01:00 value_1 value_2 value_3", next(generator))
