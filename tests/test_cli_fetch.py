from datetime import datetime

from click.testing import CliRunner
from dateutil import tz

from elasticsearch_follow import cli
from .elasticsearch_integration_base import TestElasticsearchIntegrationBase

TIMESTAMP_ONE = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)


class TestFetchCli(TestElasticsearchIntegrationBase):
    def test_basic_fetch(self):
        self.delete_index('test_index')
        self.insert_line(message='testMessage', timestamp=TIMESTAMP_ONE)

        runner = CliRunner()
        result = runner.invoke(cli.cli, ['-v',
                                         '--connect', 'http://localhost:9200',
                                         '--username', 'someUser',
                                         '--password', 'somePassword',
                                         'fetch'])

        self.assertIn('Connecting to "localhost:9200" with "someUser"', result.output)
        self.assertIn('testMessage', result.output)
        self.assertEqual(0, result.exit_code)
