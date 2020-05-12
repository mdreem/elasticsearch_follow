def generate_basic_query_response(entry_id, message, timestamp, with_timezone=True):
    return {
        "_scroll_id": "some_scroll_id",
        "hits": {
            "hits": [generate_hit_entry(entry_id, message, timestamp, with_timezone)]
        },
    }


def generate_query_response(hits):
    return {"_scroll_id": "some_scroll_id", "hits": {"hits": hits}}


def generate_hit_entry(entry_id, message, timestamp, with_timezone=True):
    return {
        "_id": entry_id,
        "_source": {
            "msg": message,
            "@timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S%Z")
            if with_timezone
            else timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        },
    }
