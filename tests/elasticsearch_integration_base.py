from elasticsearch import Elasticsearch


class TestElasticsearchIntegrationBase:
    @staticmethod
    def find_hit(hits, message):
        return next((hit for hit in hits if hit['_source']['message'] == message), None)

    def insert_line(self, message, timestamp):
        es = Elasticsearch(["http://localhost:9200"])
        es.info()

        doc = {
            'message': message,
            '@timestamp': timestamp,
        }

        res = es.index(index='test_index', body=doc)
        print('Inserting line: {}'.format(res))

        query = {
            'size': 10000,
            'query': {
                'match_all': {}
            }
        }
        while True:
            res = es.search(index='test_index', body=query)
            hits = res['hits']['hits']
            if hits and self.find_hit(hits, message):
                print('Found ({}): {}'.format(message, hits))
                break
        print('Check: {}'.format(res['hits']['hits']))

    @staticmethod
    def delete_index(name):
        es = Elasticsearch(["http://localhost:9200"])
        es.indices.delete(index=name, ignore=[400, 404])