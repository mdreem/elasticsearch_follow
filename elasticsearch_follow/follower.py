import datetime

from dateutil import tz


class Follower:
    def __init__(self, elasticsearch_follow, index, time_delta=60, processor=None):
        self.elasticsearch_follow = elasticsearch_follow
        self.index = index
        self.time_delta = time_delta
        self.processor = processor

    def generator(self):
        now = datetime.datetime.utcnow()
        now = now.replace(tzinfo=tz.UTC)
        delta = datetime.timedelta(seconds=self.time_delta)

        while True:
            lines = self.elasticsearch_follow.get_new_lines(self.index, now - delta)
            self.elasticsearch_follow.prune_before(now - delta)

            if not lines:
                yield None

            for line in lines:
                if self.processor:
                    yield self.processor.process_line(line)
                yield line
