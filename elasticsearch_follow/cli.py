#!/usr/bin/env python3

import time
from urllib.parse import urlparse

import certifi
import click
import elasticsearch

from .elasticsearch_fetch import ElasticsearchFetch
from .elasticsearch_follow import ElasticsearchFollow
from .follower import Follower
from .formatting_processor import FormattingProcessor

CONTEXT_SETTINGS = dict(
    help_option_names=['-h', '--help'],
    auto_envvar_prefix='ES_TAIL'
)


class Config:
    def __init__(self):
        self.password = None
        self.username = None
        self.host = None
        self.verbose = False


pass_config = click.make_pass_decorator(Config, ensure=True)


def initialize_es_instance(connect, username, password, verbose=False):
    if verbose:
        print('Attempting to connect to Elasticsearch...')

    http_auth = None
    if username or password:
        http_auth = (username, password)

    if verbose:
        print('Connecting to "{}" with "{}".'.format(connect.netloc, username))

    es = elasticsearch.Elasticsearch(
        [connect.hostname],
        http_auth=http_auth,
        scheme=connect.scheme,
        port=connect.port,
        ca_certs=certifi.where()
    )

    return es


def parse_url(ctx, param, value):
    if value:
        return urlparse(value)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--username', '-u', help='Username for basic-auth.')
@click.option('--password', '-p', help='Password for basic-auth.')
@click.option('--connect', '-c', required=True, callback=parse_url, help='URL to connect to.')
@click.option('--verbose', '-v', is_flag=True, default=False, help='Print more output.')
@pass_config
def cli(config, username, password, connect, verbose):
    config.username = username
    config.password = password
    config.connect = connect
    config.verbose = verbose


@cli.command()
@click.option('--format-string', '-f', default='{@timestamp} {message}', show_default=True, help='Format of the log output.')
@click.option('--index', '-i', help='Determines which indexes to search. Using wildcards is possible.')
@click.option('--num-after', '-A', default=0, type=int, metavar='<NUM>', help='Print <NUM> lines of trailing context after each match.')
@click.option('--num-before', '-B', default=0, type=int, metavar='<NUM>', help='Print <NUM> lines of leading context before each match.')
@click.option('--query', '-q', type=str, help='The query in the Elasticsearch query language.')
@click.option('--from-time', '-F', type=str, help='From which point in time to start the query. Takes an elasticsearch time format. (e.g. now, now-1h).')
@click.option('--to-time', '-T', type=str, help='From which point in time to start the query. Takes an elasticsearch time format. (e.g. now, now-1h). ')
@pass_config
def fetch(config, format_string, index, num_after, num_before, query, from_time, to_time):
    es = initialize_es_instance(config.connect, config.username, config.password, config.verbose)
    es_fetch = ElasticsearchFetch(elasticsearch=es)
    processor = FormattingProcessor(format_string=format_string)

    entries = es_fetch.search_surrounding(index=index, query_string=query, num_before=num_before, num_after=num_after, from_time=from_time, to_time=to_time)
    for entry in entries:
        if num_before > 0 or num_after > 0:
            print('#########')
        for line in entry:
            print(processor.process_line(line))


@cli.command()
@click.option('--format-string', '-f', default='{@timestamp} {message}', show_default=True, help='Format of the log output.')
@click.option('--index', '-i', help='Determines which indexes to search. Using wildcards is possible.')
@click.option('--query', '-q', type=str, help='The query used to filter lines. Has to be given in the Elasticsearch query language.')
@click.option('--number-of-lines', '-n', default=0, type=int, metavar='<NUM>', help='Print <NUM> lines.')
@click.option('--timedelta', '-t', default=60, type=int, metavar='<SECONDS>', help='Look <SECONDS> seconds into the past to update loglines.')
@pass_config
def tail(config, format_string, index, query, number_of_lines, timedelta):
    es = initialize_es_instance(config.connect, config.username, config.password, config.verbose)

    es_follow = ElasticsearchFollow(es, query_string=query)
    follower = Follower(elasticsearch_follow=es_follow, index=index, time_delta=timedelta, processor=FormattingProcessor(format_string=format_string))

    if number_of_lines > 0:
        for entry in list(follower.generator())[-number_of_lines:]:
            print(entry)

    while True:
        entries = follower.generator()
        for entry in entries:
            if entry:
                print(entry)
        time.sleep(0.1)
