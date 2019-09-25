import elasticsearch_follow


class ElasticsearchTest:
    def search(self, index, scroll, doc_type, body):
        print("Called Elasticsearch.search(index={}, scroll={}, doc_type={}, body={})".format(index, scroll, doc_type, body))
        return {'_scroll_id': 'some_scroll_id',
                'hits': {
                    'hits': [{
                        '_id': 'id_1',
                        '_source': {
                            'msg': 'line1',
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


class TestMain:
    def test_main(self):
        es = ElasticsearchTest()
        es_follow = elasticsearch_follow.ElasticsearchFollow(es)
        new_lines = es_follow.get_new_lines('my_index', None)
        assert len(new_lines) == 1
        assert 'msg' in new_lines[0]
        assert new_lines[0]['msg'] == 'line1'
