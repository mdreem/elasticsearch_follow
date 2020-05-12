import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from dateutil import tz

import elasticsearch_follow
from tests import generate_query_response, generate_basic_query_response

REFERENCE_TIME = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)


class TestFollower(unittest.TestCase):
    @staticmethod
    def setup():
        datetime_mock = Mock(wraps=datetime)
        now = datetime(year=2019, month=1, day=1, hour=10, minute=0)
        datetime_mock.utcnow = Mock(return_value=now)
        patch(
            "elasticsearch_follow.follower.datetime.datetime", new=datetime_mock
        ).start()
        es = Mock()
        es.scroll.return_value = generate_query_response([])
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        return es, es_follow

    def test_follower_returns_added_entry(self):
        es, es_follow = self.setup()
        follower = elasticsearch_follow.Follower(es_follow, "some_index", 120)

        es.search.return_value = generate_basic_query_response(
            "id_1", "line1", REFERENCE_TIME
        )
        res_first = list(follower.generator())

        es.search.return_value = generate_basic_query_response(
            "id_2", "line2", REFERENCE_TIME
        )
        res_second = list(follower.generator())

        self.assertEqual(len(res_first), 1)
        self.assertEqual(res_first[0]["msg"], "line1")

        self.assertEqual(len(res_second), 1)
        self.assertEqual(res_second[0]["msg"], "line2")

    def test_follower_with_processor_returns_added_entry(self):
        class TestProcessor(elasticsearch_follow.DefaultProcessor):
            def process_line(self, line):
                if line["msg"] == "REMOVE":
                    return None
                else:
                    return "PROCESSED_" + line["msg"]

        es_follow = Mock()
        processor = TestProcessor()
        follower = elasticsearch_follow.Follower(
            es_follow, "some_index", 120, processor=processor
        )

        response_1 = {"msg": "line_1"}
        response_2 = {"msg": "REMOVE"}
        response_3 = {"msg": "line_3"}

        es_follow.get_new_lines.return_value = [response_1, response_2, response_3]

        result = list(follower.generator())

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "PROCESSED_line_1")
        self.assertEqual(result[1], "PROCESSED_line_3")
