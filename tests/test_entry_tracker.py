import heapq
import unittest
from datetime import datetime

import pytz
from dateutil import tz

import elasticsearch_follow.entry_tracker

BEFORE_REFERENCE_TIME = datetime(
    year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC
)
REFERENCE_TIME = datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=tz.UTC)
AFTER_REFERENCE_TIME = datetime(
    year=2019, month=1, day=1, hour=10, minute=3, tzinfo=tz.UTC
)


class TestEntryTracker(unittest.TestCase):
    def test_entry_tracker_pruning(self):
        entry_tracker = elasticsearch_follow.entry_tracker.EntryTracker()

        entry_tracker.add("id-1", BEFORE_REFERENCE_TIME)
        entry_tracker.add("id-2", AFTER_REFERENCE_TIME)

        self.assertIn("id-1", entry_tracker)
        self.assertIn("id-2", entry_tracker)
        self.assertEqual(len(entry_tracker.added_entries), 2)
        self.assertEqual(len(entry_tracker.entries_by_timestamp), 2)

        entry_tracker.prune_before(REFERENCE_TIME)

        self.assertNotIn("id-1", entry_tracker)
        self.assertIn("id-2", entry_tracker)
        self.assertEqual(len(entry_tracker.added_entries), 1)
        self.assertEqual(len(entry_tracker.entries_by_timestamp), 1)

    def test_entry_tracker_adding_in_order_by_timestamp(self):
        entry_tracker = elasticsearch_follow.entry_tracker.EntryTracker()

        entry_tracker.add("id-1", BEFORE_REFERENCE_TIME)
        entry_tracker.add("id-2", AFTER_REFERENCE_TIME)

        self.assertEqual(
            heapq.heappop(entry_tracker.entries_by_timestamp),
            elasticsearch_follow.entry_tracker.Entry(BEFORE_REFERENCE_TIME, "id-1"),
        )
        self.assertEqual(
            heapq.heappop(entry_tracker.entries_by_timestamp),
            elasticsearch_follow.entry_tracker.Entry(AFTER_REFERENCE_TIME, "id-2"),
        )

    def test_entry_tracker_adding_reverse_order_by_timestamp(self):
        entry_tracker = elasticsearch_follow.entry_tracker.EntryTracker()

        entry_tracker.add("id-1", AFTER_REFERENCE_TIME)
        entry_tracker.add("id-2", BEFORE_REFERENCE_TIME)

        self.assertEqual(
            heapq.heappop(entry_tracker.entries_by_timestamp),
            elasticsearch_follow.entry_tracker.Entry(BEFORE_REFERENCE_TIME, "id-2"),
        )
        self.assertEqual(
            heapq.heappop(entry_tracker.entries_by_timestamp),
            elasticsearch_follow.entry_tracker.Entry(AFTER_REFERENCE_TIME, "id-1"),
        )

    def test_add_entry_without_timezone(self):
        entry_tracker = elasticsearch_follow.entry_tracker.EntryTracker()

        timestamp = datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=None)

        entry_tracker.add("id-1", timestamp)

        entry = heapq.heappop(entry_tracker.entries_by_timestamp)

        self.assertEqual(entry.timestamp.tzinfo, pytz.UTC)
