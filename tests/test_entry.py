from datetime import datetime

from dateutil import tz

import elasticsearch_follow.entry_tracker

BEFORE_REFERENCE_TIME = datetime(year=2019, month=1, day=1, hour=10, minute=1, tzinfo=tz.UTC)
REFERENCE_TIME = datetime(year=2019, month=1, day=1, hour=10, minute=2, tzinfo=tz.UTC)
AFTER_REFERENCE_TIME = datetime(year=2019, month=1, day=1, hour=10, minute=3, tzinfo=tz.UTC)


class TestEntryTracker:
    def test_entry_equality(self):
        entry_one = elasticsearch_follow.entry_tracker.Entry(REFERENCE_TIME, 'id-1')
        another_entry_one = elasticsearch_follow.entry_tracker.Entry(REFERENCE_TIME, 'id-1')

        assert entry_one == another_entry_one

    def test_entry_unequal_by_timestamp(self):
        entry_one = elasticsearch_follow.entry_tracker.Entry(BEFORE_REFERENCE_TIME, 'id-1')
        another_entry_one = elasticsearch_follow.entry_tracker.Entry(AFTER_REFERENCE_TIME, 'id-1')

        assert entry_one != another_entry_one

    def test_entry_unequal_by_id(self):
        entry_one = elasticsearch_follow.entry_tracker.Entry(REFERENCE_TIME, 'id-1')
        another_entry_one = elasticsearch_follow.entry_tracker.Entry(REFERENCE_TIME, 'id-2')

        assert entry_one != another_entry_one

    def test_sorting_only_by_timestamp(self):
        earlier_entry_one = elasticsearch_follow.entry_tracker.Entry(BEFORE_REFERENCE_TIME, 'B')
        later_entry_one = elasticsearch_follow.entry_tracker.Entry(AFTER_REFERENCE_TIME, 'A')

        assert earlier_entry_one < later_entry_one

        earlier_entry_two = elasticsearch_follow.entry_tracker.Entry(BEFORE_REFERENCE_TIME, 'A')
        later_entry_two = elasticsearch_follow.entry_tracker.Entry(AFTER_REFERENCE_TIME, 'B')

        assert earlier_entry_two < later_entry_two
