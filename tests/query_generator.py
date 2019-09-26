def generate_basic_query_response(entry_id, message, timestamp):
    return {'_scroll_id': 'some_scroll_id',
            'hits': {
                'hits': [{
                    '_id': entry_id,
                    '_source': {
                        'msg': message,
                        '@timestamp': timestamp.strftime("%Y-%m-%dT%H:%M:%S")
                    }
                }]
            }
            }


def generate_scroll(hits):
    return {'_scroll_id': 'some_scroll_id',
            'hits': {
                'hits': hits
            }
            }
