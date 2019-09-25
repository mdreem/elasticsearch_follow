import datetime


class Follower:
    def __init__(self, elasticsearch_follow, index, time_delta):
        self.elasticsearch_follow = elasticsearch_follow
        self.index = index
        self.time_delta = time_delta

    def generator(self):
        now = datetime.datetime.utcnow()
        delta = datetime.timedelta(seconds=self.time_delta)

        while True:
            lines = self.elasticsearch_follow.get_new_lines(self.index, now - delta)
            if not lines:
                yield None

            for line in lines:
                yield line
