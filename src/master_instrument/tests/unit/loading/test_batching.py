"""Unit tests for batching.py - Batch strategies and date range handling."""

import pytest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, Mock, patch
from sqlalchemy import Column, Integer, String, Date, text
from sqlalchemy.orm import declarative_base

from master_instrument.etl.loading.batching import (
    DateRange,
    IntervalUnit,
    VolumeBasedStrategy,
    FixedIntervalStrategy,
)


# Test fixtures
Base = declarative_base()


class SampleModel(Base):
    __tablename__ = "sample"
    __table_args__ = {"schema": "test"}
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    event_date = Column(Date)


class TestDateRange:
    """Tests for DateRange dataclass."""
    
    def test_creation_with_dates(self):
        """Should create DateRange with date objects."""
        start = date(2023, 1, 1)
        end = date(2023, 12, 31)
        
        dr = DateRange(start=start, end=end)
        
        assert dr.start == start
        assert dr.end == end
    
    
    def test_equality(self):
        """Should compare DateRange objects correctly."""
        dr1 = DateRange(start=date(2023, 1, 1), end=date(2023, 12, 31))
        dr2 = DateRange(start=date(2023, 1, 1), end=date(2023, 12, 31))
        dr3 = DateRange(start=date(2023, 1, 1), end=date(2023, 6, 30))
        
        assert dr1 == dr2
        assert dr1 != dr3


class TestIntervalUnit:
    """Tests for IntervalUnit enum."""
    
    def test_day_interval(self):
        """Should create timedelta for days."""
        delta = IntervalUnit.DAY.get_delta(5)
        assert delta == timedelta(days=5)
    
    def test_week_interval(self):
        """Should create timedelta for weeks."""
        delta = IntervalUnit.WEEK.get_delta(2)
        assert delta == timedelta(weeks=2)




class TestBatchStrategyEdgeCases:
    """Tests for edge cases in batch strategies."""
    
    def test_date_range_to_tuple(self):
        """Should convert DateRange to tuple of strings."""
        dr = DateRange(start=date(2023, 1, 1), end=date(2023, 12, 31))
        result = dr.to_tuple()
        assert result == ("2023-01-01", "2023-12-31")
    
    def test_date_range_from_start_and_delta(self):
        """Should create date range from start and timedelta."""
        start = date(2023, 1, 1)
        delta = timedelta(days=30)
        
        dr = DateRange.from_start_and_delta(start, delta)
        
        assert dr.start == date(2023, 1, 1)
        assert dr.end == date(2023, 1, 30)  # 30 days, inclusive
"""Unit tests for batch strategies."""

from unittest.mock import MagicMock
from datetime import date, timedelta
from master_instrument.etl.loading.batching import (
    VolumeBasedStrategy,
    FixedIntervalStrategy,
    IntervalUnit,
    DateRange,
    DateRangeWithCount,
    BatchAccumulator,
    DateRangeRepository,
)


class TestIntervalUnit:
    """Tests for IntervalUnit enum."""
    
    def test_day_delta(self):
        """Should return timedelta for days."""
        delta = IntervalUnit.DAY.get_delta(5)
        assert delta == timedelta(days=5)
    
    def test_week_delta(self):
        """Should return timedelta for weeks."""
        delta = IntervalUnit.WEEK.get_delta(2)
        assert delta == timedelta(weeks=2)


class TestDateRange:
    """Tests for DateRange dataclass."""
    
    def test_to_tuple(self):
        """Should convert to string tuple."""
        dr = DateRange(date(2024, 1, 1), date(2024, 1, 31))
        assert dr.to_tuple() == ("2024-01-01", "2024-01-31")
    
    def test_from_start_and_delta_day(self):
        """Should create range from start + delta (day)."""
        start = date(2024, 1, 1)
        delta = timedelta(days=30)
        dr = DateRange.from_start_and_delta(start, delta)
        
        assert dr.start == date(2024, 1, 1)
        assert dr.end == date(2024, 1, 30)  # 30 days - 1 day
    
    def test_from_start_and_delta_week(self):
        """Should create range from start + delta (week)."""
        start = date(2024, 1, 1)
        delta = timedelta(weeks=2)
        dr = DateRange.from_start_and_delta(start, delta)
        
        assert dr.start == date(2024, 1, 1)
        assert dr.end == date(2024, 1, 14)  # 14 days - 1 day = 13 days span
    
    def test_shift(self):
        """Should shift range forward."""
        dr = DateRange(date(2024, 1, 1), date(2024, 1, 10))
        shifted = dr.shift()
        
        assert shifted.start == date(2024, 1, 11)
        assert shifted.end == date(2024, 1, 20)
    
    def test_is_end_date_before(self):
        """Should check if end date is before max."""
        dr = DateRange(date(2024, 1, 1), date(2024, 1, 10))
        
        assert dr.is_end_date_before(date(2024, 1, 15)) is True
        assert dr.is_end_date_before(date(2024, 1, 10)) is False
        assert dr.is_end_date_before(date(2024, 1, 5)) is False
    
    def test_cap_at_no_change(self):
        """Should not change if already within max."""
        dr = DateRange(date(2024, 1, 1), date(2024, 1, 10))
        capped = dr.cap_at(date(2024, 1, 15))
        
        assert capped.start == date(2024, 1, 1)
        assert capped.end == date(2024, 1, 10)
    
    def test_cap_at_with_change(self):
        """Should cap end date if exceeds max."""
        dr = DateRange(date(2024, 1, 1), date(2024, 1, 20))
        capped = dr.cap_at(date(2024, 1, 15))
        
        assert capped.start == date(2024, 1, 1)
        assert capped.end == date(2024, 1, 15)


class TestDateRangeWithCount:
    """Tests for DateRangeWithCount dataclass."""
    
    def test_start_new(self):
        """Should create new range with single date."""
        drwc = DateRangeWithCount.start_new(date(2024, 1, 5), 1000)
        
        assert drwc.range.start == date(2024, 1, 5)
        assert drwc.range.end == date(2024, 1, 5)
        assert drwc.row_count == 1000
    
    def test_can_add_true(self):
        """Should return true if can add rows without exceeding target."""
        drwc = DateRangeWithCount.start_new(date(2024, 1, 1), 6000)
        
        assert drwc.can_add(3000, target_rows=10000) is True
    
    def test_can_add_false(self):
        """Should return false if adding rows exceeds target."""
        drwc = DateRangeWithCount.start_new(date(2024, 1, 1), 6000)
        
        assert drwc.can_add(5000, target_rows=10000) is False
    
    def test_can_add_exact_target(self):
        """Should return true if exactly at target."""
        drwc = DateRangeWithCount.start_new(date(2024, 1, 1), 6000)
        
        assert drwc.can_add(4000, target_rows=10000) is True
    
    def test_extend_to(self):
        """Should extend range and add rows."""
        drwc = DateRangeWithCount.start_new(date(2024, 1, 1), 1000)
        extended = drwc.extend_to(date(2024, 1, 5), 2000)
        
        assert extended.range.start == date(2024, 1, 1)
        assert extended.range.end == date(2024, 1, 5)
        assert extended.row_count == 3000


class TestBatchAccumulator:
    """Tests for BatchAccumulator dataclass."""
    
    def test_create(self):
        """Should create empty accumulator."""
        acc = BatchAccumulator.create(target_rows=10000)
        
        assert acc.current is None
        assert acc.target_rows == 10000
        assert acc.completed_ranges == ()
    
    def test_process_first_date(self):
        """Should start new range on first date."""
        acc = BatchAccumulator.create(target_rows=10000)
        new_acc = acc.process(date(2024, 1, 1), 1000)
        
        assert new_acc.current is not None
        assert new_acc.current.range.start == date(2024, 1, 1)
        assert new_acc.current.range.end == date(2024, 1, 1)
        assert new_acc.current.row_count == 1000
        assert len(new_acc.completed_ranges) == 0
    
    def test_process_extend_within_target(self):
        """Should extend current range if within target."""
        acc = BatchAccumulator.create(target_rows=10000)
        acc = acc.process(date(2024, 1, 1), 3000)
        acc = acc.process(date(2024, 1, 2), 4000)
        
        assert acc.current is not None
        assert acc.current.range.start == date(2024, 1, 1)
        assert acc.current.range.end == date(2024, 1, 2)
        assert acc.current.row_count == 7000
        assert len(acc.completed_ranges) == 0
    
    def test_process_close_batch_when_exceeds(self):
        """Should close batch and start new when exceeding target."""
        acc = BatchAccumulator.create(target_rows=10000)
        acc = acc.process(date(2024, 1, 1), 6000)
        acc = acc.process(date(2024, 1, 2), 3000)
        acc = acc.process(date(2024, 1, 3), 8000)  # 9000 + 8000 > 10000 → close
        
        assert acc.current is not None
        assert acc.current.range.start == date(2024, 1, 3)
        assert acc.current.range.end == date(2024, 1, 3)
        assert acc.current.row_count == 8000
        assert len(acc.completed_ranges) == 1
        assert acc.completed_ranges[0] == DateRange(date(2024, 1, 1), date(2024, 1, 2))
    
    def test_finalize_with_current(self):
        """Should include current range in finalized batches."""
        acc = BatchAccumulator.create(target_rows=10000)
        acc = acc.process(date(2024, 1, 1), 6000)
        acc = acc.process(date(2024, 1, 2), 3000)
        
        ranges = acc.finalize()
        
        assert len(ranges) == 1
        assert ranges[0] == DateRange(date(2024, 1, 1), date(2024, 1, 2))
    
    def test_finalize_without_current(self):
        """Should return only completed ranges if no current."""
        acc = BatchAccumulator.create(target_rows=10000)
        
        ranges = acc.finalize()
        
        assert ranges == []
    
    def test_finalize_multiple_batches(self):
        """Should return all batches including current."""
        acc = BatchAccumulator.create(target_rows=10000)
        acc = acc.process(date(2024, 1, 1), 8000)
        acc = acc.process(date(2024, 1, 2), 5000)  # 8k+5k=13k > 10k → Close batch 1
        acc = acc.process(date(2024, 1, 3), 7000)
        acc = acc.process(date(2024, 1, 4), 6000)  # 7k+6k=13k > 10k → Close batch 2
        acc = acc.process(date(2024, 1, 5), 2000)
        
        ranges = acc.finalize()
        
        # Batch 1: [Jan 1]
        # Batch 2: [Jan 2]
        # Batch 3: [Jan 3]
        # Batch 4: [Jan 4, Jan 5]
        assert len(ranges) == 4
        assert ranges[0] == DateRange(date(2024, 1, 1), date(2024, 1, 1))
        assert ranges[1] == DateRange(date(2024, 1, 2), date(2024, 1, 2))
        assert ranges[2] == DateRange(date(2024, 1, 3), date(2024, 1, 3))
        assert ranges[3] == DateRange(date(2024, 1, 4), date(2024, 1, 5))


class TestDateRangeRepository:
    """Tests for DateRangeRepository."""
    
    def test_get_min_max_with_data(self):
        """Should return min and max dates."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchone.return_value = (
            date(2024, 1, 1), 
            date(2024, 12, 31)
        )
        
        min_date, max_date = DateRangeRepository.get_min_max(
            mock_engine, "test.table", "date_col"
        )
        
        assert min_date == date(2024, 1, 1)
        assert max_date == date(2024, 12, 31)
    
    def test_get_min_max_empty_table(self):
        """Should return None for empty table."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchone.return_value = None
        
        min_date, max_date = DateRangeRepository.get_min_max(
            mock_engine, "test.table", "date_col"
        )
        
        assert min_date is None
        assert max_date is None
    
    def test_get_counts_by_date(self):
        """Should return date counts."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value = [
            (date(2024, 1, 1), 1000),
            (date(2024, 1, 2), 2000),
            (date(2024, 1, 3), 1500),
        ]
        
        counts = DateRangeRepository.get_counts_by_date(
            mock_engine, "test.table", "date_col"
        )
        
        assert len(counts) == 3
        assert counts[0] == (date(2024, 1, 1), 1000)
        assert counts[1] == (date(2024, 1, 2), 2000)
        assert counts[2] == (date(2024, 1, 3), 1500)


class TestVolumeBasedStrategy:
    """Tests for VolumeBasedStrategy integration."""
    
    def test_generates_single_batch_under_target(self):
        """Should generate single batch when total rows < target."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value = [
            (date(2024, 1, 1), 1000),
            (date(2024, 1, 2), 2000),
            (date(2024, 1, 3), 3000),
        ]
        
        strategy = VolumeBasedStrategy(target_rows=10000)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert len(batches) == 1
        assert batches[0] == ("2024-01-01", "2024-01-03")
    
    def test_splits_into_multiple_batches(self):
        """Should split into multiple batches when exceeding target."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value = [
            (date(2024, 1, 1), 6000),
            (date(2024, 1, 2), 3000),
            (date(2024, 1, 3), 8000),  # 9000 + 8000 > 10000 → close batch
            (date(2024, 1, 4), 2000),
        ]
        
        strategy = VolumeBasedStrategy(target_rows=10000)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert len(batches) == 2
        assert batches[0] == ("2024-01-01", "2024-01-02")
        assert batches[1] == ("2024-01-03", "2024-01-04")
    
    def test_handles_empty_table(self):
        """Should return empty list for empty table."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value = []
        
        strategy = VolumeBasedStrategy(target_rows=10000)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert batches == []
    
    def test_single_date_exceeds_target(self):
        """Should handle single date with rows > target."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value = [
            (date(2024, 1, 1), 15000),  # Exceeds 10k target
            (date(2024, 1, 2), 5000),
        ]
        
        strategy = VolumeBasedStrategy(target_rows=10000)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert len(batches) == 2
        assert batches[0] == ("2024-01-01", "2024-01-01")
        assert batches[1] == ("2024-01-02", "2024-01-02")


class TestFixedIntervalStrategy:
    """Tests for FixedIntervalStrategy integration."""
    
    def test_generates_daily_batches(self):
        """Should generate daily batches."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchone.return_value = (
            date(2024, 1, 1),
            date(2024, 1, 5)
        )
        
        strategy = FixedIntervalStrategy(interval=1, unit=IntervalUnit.DAY)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert len(batches) == 5
        assert batches[0] == ("2024-01-01", "2024-01-01")
        assert batches[4] == ("2024-01-05", "2024-01-05")
    
    def test_generates_weekly_batches(self):
        """Should generate weekly batches."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchone.return_value = (
            date(2024, 1, 1),
            date(2024, 1, 31)
        )
        
        strategy = FixedIntervalStrategy(interval=1, unit=IntervalUnit.WEEK)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert len(batches) == 5
        assert batches[0] == ("2024-01-01", "2024-01-07")
        assert batches[-1][1] == "2024-01-31"  # Capped at max
    
    def test_generates_30_day_batches(self):
        """Should generate 30-day batches."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchone.return_value = (
            date(2024, 1, 1),
            date(2024, 3, 31)
        )
        
        strategy = FixedIntervalStrategy(interval=30, unit=IntervalUnit.DAY)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert len(batches) == 4  # 90 days / 30 = 3, + 1 partial
        assert batches[0] == ("2024-01-01", "2024-01-30")
        assert batches[1] == ("2024-01-31", "2024-02-29")
        assert batches[-1][1] == "2024-03-31"
    
    def test_handles_empty_table(self):
        """Should return empty list for empty table."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchone.return_value = (None, None)
        
        strategy = FixedIntervalStrategy(interval=30, unit=IntervalUnit.DAY)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert batches == []
    
    def test_single_day_range(self):
        """Should handle single day range."""
        mock_engine = MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchone.return_value = (
            date(2024, 1, 1),
            date(2024, 1, 1)
        )
        
        strategy = FixedIntervalStrategy(interval=30, unit=IntervalUnit.DAY)
        batches = strategy.generate_batches(mock_engine, "test.table", "date_col")
        
        assert len(batches) == 1
        assert batches[0] == ("2024-01-01", "2024-01-01")
