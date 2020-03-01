import os
import time
from datetime import datetime
from multiprocessing import Queue, Process
from signal import SIGINT
from threading import Timer
from unittest.mock import patch

from click.testing import CliRunner
from dateutil import tz

from elasticsearch_follow import cli
from .elasticsearch_integration_base import TestElasticsearchIntegrationBase

PASSWORD = 'somePassword'
USERNAME = 'someUser'
ES_HOST = 'http://localhost:9200'
TIMEOUT = 1.0

TIMESTAMP_ONE = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)
TIMESTAMP_ONE_HALF = datetime(year=2019, month=1, day=1, hour=10, minute=1, second=30, tzinfo=tz.UTC)
TIMESTAMP_TWO = datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=tz.UTC)
TIMESTAMP_TWO_HALF = datetime(year=2019, month=1, day=1, hour=10, minute=2, second=30, tzinfo=tz.UTC)
TIMESTAMP_THREE = datetime(year=2019, month=1, day=1, hour=10, minute=3, tzinfo=tz.UTC)


class TestFetchCli(TestElasticsearchIntegrationBase):
    def tearDown(self):
        self.delete_index('test_index')
        self.delete_index('test_index_fetch')

    @staticmethod
    def fetch_data_from_process(result_queue, run_in_background):
        p = Process(target=run_in_background)
        p.start()
        results = {}
        while p.is_alive():
            time.sleep(0.1)
        else:
            while not result_queue.empty():
                key, value = result_queue.get()
                results[key] = value
        return results

    def run_with_args_and_fetch_results(self, args):
        result_queue = Queue()

        def run_in_background():
            Timer(TIMEOUT, lambda: os.kill(os.getpid(), SIGINT)).start()

            runner = CliRunner()
            result = runner.invoke(cli.cli, args)
            result_queue.put(('exit_code', result.exit_code))
            result_queue.put(('result', result.output))
            result_queue.put(('exception', result.exception))

            print('Exception: {}'.format(result.exception))

        results = self.fetch_data_from_process(result_queue, run_in_background)
        return results

    @patch('follower.datetime.datetime')
    def test_basic_fetch(self, mock_dt):
        mock_dt.utcnow.return_value = TIMESTAMP_ONE_HALF
        mock_dt.now.return_value = TIMESTAMP_ONE_HALF

        self.insert_line(message='testMessage', timestamp=TIMESTAMP_ONE)

        results = self.run_with_args_and_fetch_results(args=['-v',
                                                             '--connect', ES_HOST,
                                                             '--username', USERNAME,
                                                             '--password', PASSWORD,
                                                             'tail'])

        self.assertIn('Connecting to "localhost:9200" with "someUser"', results['result'])
        self.assertIn('testMessage', results['result'])

    @patch('follower.datetime.datetime')
    def test_fetch_with_different_index(self, mock_dt):
        mock_dt.utcnow.return_value = TIMESTAMP_ONE_HALF
        mock_dt.now.return_value = TIMESTAMP_ONE_HALF

        self.insert_line(message='skipMe', timestamp=TIMESTAMP_ONE, index='test_index')
        self.insert_line(message='fetchMe', timestamp=TIMESTAMP_ONE, index='test_index_fetch')

        first_result = self.run_with_args_and_fetch_results(args=['-v',
                                                                  '--connect', ES_HOST,
                                                                  '--username', USERNAME,
                                                                  '--password', PASSWORD,
                                                                  'tail'])

        second_result = self.run_with_args_and_fetch_results(args=['-v',
                                                                   '--connect', ES_HOST,
                                                                   '--username', USERNAME,
                                                                   '--password', PASSWORD,
                                                                   'tail',
                                                                   '--index', 'test_index_fetch'])

        self.assertIn('Connecting to "localhost:9200" with "someUser"', first_result['result'])
        self.assertIn('skipMe', first_result['result'])
        self.assertIn('fetchMe', first_result['result'])

        self.assertIn('Connecting to "localhost:9200" with "someUser"', second_result['result'])
        self.assertNotIn('skipMe', second_result['result'])
        self.assertIn('fetchMe', second_result['result'])

    @patch('follower.datetime.datetime')
    def test_fetch_with_format_string(self, mock_dt):
        mock_dt.utcnow.return_value = TIMESTAMP_ONE_HALF
        mock_dt.now.return_value = TIMESTAMP_ONE_HALF
        self.insert_line(message='formatMe', timestamp=TIMESTAMP_ONE)

        results = self.run_with_args_and_fetch_results(args=['-v',
                                                             '--connect', ES_HOST,
                                                             '--username', USERNAME,
                                                             '--password', PASSWORD,
                                                             'tail',
                                                             '--format-string', '{@timestamp} -->{message}<--'])

        self.assertIn('Connecting to "localhost:9200" with "someUser"', results['result'])
        self.assertIn('-->formatMe<--', results['result'])

    @patch('follower.datetime.datetime')
    def test_fetch_with_query(self, mock_dt):
        mock_dt.utcnow.return_value = TIMESTAMP_ONE_HALF
        mock_dt.now.return_value = TIMESTAMP_ONE_HALF
        self.insert_line(message='findMe', timestamp=TIMESTAMP_ONE)
        self.insert_line(message='doNotFindMe', timestamp=TIMESTAMP_ONE)

        results = self.run_with_args_and_fetch_results(args=['-v',
                                                             '--connect', ES_HOST,
                                                             '--username', USERNAME,
                                                             '--password', PASSWORD,
                                                             'tail',
                                                             '--query', 'message:findMe'])

        self.assertIn('Connecting to "localhost:9200" with "someUser"', results['result'])
        self.assertIn('findMe', results['result'])
        self.assertNotIn('doNotFindMe', results['result'])
