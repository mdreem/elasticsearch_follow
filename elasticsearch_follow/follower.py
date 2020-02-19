import datetime

from dateutil import tz


class Follower:
    def __init__(self, elasticsearch_follow, index, time_delta=60, processor=None):
        """
        :param elasticsearch_follow: The instance of ElasticsearchFollow to use for yielding new lines.
        :param index: The index to use to fetch data.
        :param time_delta: Denotes how many seconds to look into the past when fetching lines.
        :param processor: The log processor which should be used to process the lines before yielding them.
        """
        self.elasticsearch_follow = elasticsearch_follow
        self.index = index
        self.time_delta = time_delta
        self.processor = processor

    def generator(self):
        """
        Creates a generator which will yield new lines until the most recent query has no more lines.
        :return: A generator.
        """
        now = datetime.datetime.utcnow()
        now = now.replace(tzinfo=tz.UTC)
        delta = datetime.timedelta(seconds=self.time_delta)

        for line in self.elasticsearch_follow.get_new_lines(self.index, now - delta):
            self.elasticsearch_follow.prune_before(now - delta)

            if self.processor:
                processed_line = self.processor.process_line(line)
                if processed_line:
                    yield processed_line
            else:
                yield line
