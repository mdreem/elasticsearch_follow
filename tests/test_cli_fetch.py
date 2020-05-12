from datetime import datetime

from click.testing import CliRunner
from dateutil import tz

from elasticsearch_follow import cli
from .elasticsearch_integration_base import TestElasticsearchIntegrationBase

PASSWORD = "somePassword"
USERNAME = "someUser"
ES_HOST = "http://localhost:9200"

TIMESTAMP_ONE = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)
TIMESTAMP_ONE_HALF = datetime(
    year=2019, month=1, day=1, hour=10, minute=1, second=30, tzinfo=tz.UTC
)
TIMESTAMP_TWO = datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=tz.UTC)
TIMESTAMP_TWO_HALF = datetime(
    year=2019, month=1, day=1, hour=10, minute=2, second=30, tzinfo=tz.UTC
)
TIMESTAMP_THREE = datetime(year=2019, month=1, day=1, hour=10, minute=3, tzinfo=tz.UTC)


class TestFetchCli(TestElasticsearchIntegrationBase):
    def test_basic_fetch(self):
        self.delete_index("test_index")
        self.insert_line(message="testMessage", timestamp=TIMESTAMP_ONE)

        runner = CliRunner()
        result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                ES_HOST,
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
            ],
        )

        self.assertIn('Connecting to "localhost:9200" with "someUser"', result.output)
        self.assertIn("testMessage", result.output)
        self.assertEqual(0, result.exit_code)

    def test_fetch_with_different_index(self):
        self.delete_index("test_index")
        self.insert_line(message="skipMe", timestamp=TIMESTAMP_ONE, index="test_index")
        self.insert_line(
            message="fetchMe", timestamp=TIMESTAMP_ONE, index="test_index_fetch"
        )

        runner = CliRunner()
        first_result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                ES_HOST,
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
            ],
        )

        second_result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                ES_HOST,
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
                "--index",
                "test_index_fetch",
            ],
        )

        self.assertIn(
            'Connecting to "localhost:9200" with "someUser"', first_result.output
        )
        self.assertIn("skipMe", first_result.output)
        self.assertIn("fetchMe", first_result.output)
        self.assertEqual(0, first_result.exit_code)

        self.assertIn(
            'Connecting to "localhost:9200" with "someUser"', second_result.output
        )
        self.assertNotIn("skipMe", second_result.output)
        self.assertIn("fetchMe", second_result.output)
        self.assertEqual(0, second_result.exit_code)

        self.delete_index("test_index")
        self.delete_index("test_index_fetch")

    def test_fetch_with_format_string(self):
        self.delete_index("test_index")
        self.insert_line(message="formatMe", timestamp=TIMESTAMP_ONE)

        runner = CliRunner()
        result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                ES_HOST,
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
                "--format-string",
                "{@timestamp} -->{message}<--]{unknown}[--",
            ],
        )

        self.assertIn('Connecting to "localhost:9200" with "someUser"', result.output)
        self.assertIn("-->formatMe<--][--", result.output)
        self.assertEqual(0, result.exit_code)

    def test_fetch_with_query(self):
        self.delete_index("test_index")
        self.insert_line(message="findMe", timestamp=TIMESTAMP_ONE)
        self.insert_line(message="doNotFindMe", timestamp=TIMESTAMP_ONE)

        runner = CliRunner()
        result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                ES_HOST,
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
                "--query",
                "message:findMe",
            ],
        )

        self.assertIn('Connecting to "localhost:9200" with "someUser"', result.output)
        self.assertIn("findMe", result.output)
        self.assertNotIn("doNotFindMe", result.output)
        self.assertEqual(0, result.exit_code)

    def test_fetch_with_num_before_and_after(self):
        self.delete_index("test_index")
        self.insert_line(message="firstLine", timestamp=TIMESTAMP_ONE)
        self.insert_line(message="secondLine", timestamp=TIMESTAMP_TWO)
        self.insert_line(message="thirdLine", timestamp=TIMESTAMP_THREE)

        runner = CliRunner()
        result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                ES_HOST,
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
                "--num-after",
                "1",
                "--num-before",
                "1",
            ],
        )

        self.assertIn('Connecting to "localhost:9200" with "someUser"', result.output)
        self.assertIn(
            """#########
2019-01-01T10:01:00+00:00 firstLine
2019-01-01T10:02:00+00:00 secondLine
#########
2019-01-01T10:01:00+00:00 firstLine
2019-01-01T10:02:00+00:00 secondLine
2019-01-01T10:03:00+00:00 thirdLine
#########
2019-01-01T10:02:00+00:00 secondLine
2019-01-01T10:03:00+00:00 thirdLine""",
            result.output,
        )
        self.assertEqual(0, result.exit_code)

    def test_fetch_with_from_and_to_time(self):
        self.delete_index("test_index")
        self.insert_line(message="firstLine", timestamp=TIMESTAMP_ONE)
        self.insert_line(message="secondLine", timestamp=TIMESTAMP_TWO)
        self.insert_line(message="thirdLine", timestamp=TIMESTAMP_THREE)

        runner = CliRunner()
        result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                ES_HOST,
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
                "--from-time",
                TIMESTAMP_ONE_HALF.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "--to-time",
                TIMESTAMP_TWO_HALF.strftime("%Y-%m-%dT%H:%M:%S%z"),
            ],
        )

        self.assertIn('Connecting to "localhost:9200" with "someUser"', result.output)
        self.assertIn("secondLine", result.output)
        self.assertNotIn("firstLine", result.output)
        self.assertNotIn("thirdLine", result.output)
        self.assertEqual(0, result.exit_code)

    def test_port_is_added_with_https_without_given_port(self):
        self.delete_index("test_index")
        self.insert_line(message="testMessage", timestamp=TIMESTAMP_ONE)

        runner = CliRunner()
        result = runner.invoke(
            cli.cli,
            [
                "-v",
                "--connect",
                "https://localhost",
                "--username",
                USERNAME,
                "--password",
                PASSWORD,
                "fetch",
            ],
        )

        print(result.output)
        self.assertIn('Connecting to "localhost" with "someUser"', result.output)
        self.assertIn("Setting port to 443 explicitly.", result.output)

    def test_cookie_option_is_evaluated(self):
        self.delete_index("test_index")
        self.insert_line(message="testMessage", timestamp=TIMESTAMP_ONE)

        runner = CliRunner()
        result = runner.invoke(
            cli.cli, ["-v", "--connect", ES_HOST, "--cookie", "someCookie", "fetch"]
        )

        print(result.output)
        self.assertIn('Connecting to "localhost" via cookie.', result.output)
        self.assertIn("testMessage", result.output)
        self.assertEqual(0, result.exit_code)
