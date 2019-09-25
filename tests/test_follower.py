import elasticsearch_follow


class ElasticsearchTest:
    def __init__(self):
        self.message = ''

    def set_message(self, message):
        self.message = message

    def search(self, index, scroll, doc_type, body):
        print("Called Elasticsearch.search(index={}, scroll={}, doc_type={}, body={})".format(index, scroll, doc_type, body))
        return {'_scroll_id': 'some_scroll_id',
                'hits': {
                    'hits': [{
                        '_id': 'id_' + self.message,
                        '_source': {
                            'msg': self.message,
                            '@timestamp': '2019-01-01T10:01:00'
                        }
                    }]
                }
                }

    def scroll(self, scroll_id, scroll):
        print("Called Elasticsearch.scroll(scroll_id={}, scroll={})".format(scroll_id, scroll))
        return {'_scroll_id': 'some_scroll_id',
                'hits': {
                    'hits': []
                }
                }


class TestFollower:
    def test_main(self):
        es = ElasticsearchTest()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        follower = elasticsearch_follow.Follower(es_follow, 'some_index', 60)

        es.set_message('line1')
        generator = follower.generator()
        assert next(generator)['msg'] == 'line1'
        assert next(generator) is None

        es.set_message('line2')
        assert next(generator)['msg'] == 'line2'
        assert next(generator) is None
