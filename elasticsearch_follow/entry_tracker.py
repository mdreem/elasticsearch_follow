import heapq
from functools import total_ordering

import pytz


@total_ordering
class Entry:
    def __init__(self, timestamp, entry_id):
        self.timestamp = timestamp
        self.entry_id = entry_id

    def __eq__(self, other):
        return (self.timestamp, self.entry_id) == (other.timestamp, other.entry_id)

    def __lt__(self, other):
        return self.timestamp < other.timestamp


class EntryTracker:
    def __init__(self):
        self.added_entries = set()
        self.entries_by_timestamp = []

    def _update_entries_by_timestamp(self, entry_id, entry_timestamp):
        if not entry_timestamp.tzinfo:
            entry_timestamp = pytz.utc.localize(entry_timestamp)
        heapq.heappush(self.entries_by_timestamp, Entry(timestamp=entry_timestamp, entry_id=entry_id))

    def prune_before(self, timestamp):
        """
        Removes All entries before  ``timestamp`` from the internal buffer.
        :param timestamp: All entries in the internal before this timestamp will be removed.
        """
        while len(self.entries_by_timestamp) > 0:
            oldest_entry = heapq.heappop(self.entries_by_timestamp)
            if oldest_entry.timestamp <= timestamp:
                self.added_entries.remove(oldest_entry.entry_id)
            else:
                heapq.heappush(self.entries_by_timestamp, oldest_entry)
                return

    def add(self, entry_id, entry_timestamp):
        self.added_entries.add(entry_id)
        self._update_entries_by_timestamp(entry_id, entry_timestamp)

    def __contains__(self, key):
        return key in self.added_entries
