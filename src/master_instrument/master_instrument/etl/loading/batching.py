"""Batch generation strategies for data loading."""

from typing import Protocol
from datetime import date, timedelta
from dataclasses import dataclass
from enum import Enum
from sqlalchemy.engine import Engine
from sqlalchemy import text


class BatchStrategy(Protocol):
    
    def generate_batches(
        self, 
        engine: Engine, 
        table: str, 
        date_column: str
    ) -> list[tuple[str, str]]:
        ...


class IntervalUnit(Enum):
    """Supported interval units for fixed interval batching."""
    DAY = "day"
    WEEK = "week"
    
    def get_delta(self, interval: int) -> timedelta:
        """Get timedelta for the interval."""
        if self == IntervalUnit.DAY:
            return timedelta(days=interval)
        else:  # WEEK
            return timedelta(weeks=interval)


@dataclass
class DateRange:
    start: date
    end: date
    
    def to_tuple(self) -> tuple[str, str]:
        return (self.start.strftime("%Y-%m-%d"), self.end.strftime("%Y-%m-%d"))
    
    @classmethod
    def from_start_and_delta(cls, start: date, delta: timedelta) -> "DateRange":
        """Create date range from start date and timedelta."""
        one_day = IntervalUnit.DAY.get_delta(1)
        end = start + delta - one_day
        return cls(start, end)
    
    def shift(self) -> "DateRange":
        interval_days = (self.end - self.start).days
        next_start = self.end + timedelta(days=1)
        next_end = next_start + timedelta(days=interval_days)
        return DateRange(next_start, next_end)
    
    def is_end_date_before(self, max_date: date) -> bool:
        return self.end < max_date
    
    def cap_at(self, max_date: date) -> "DateRange":
        if self.end > max_date:
            return DateRange(self.start, max_date)
        return self


@dataclass(frozen=True)
class DateRangeWithCount:
    range: DateRange
    row_count: int
    
    @classmethod
    def start_new(cls, start_date: date, row_count: int) -> "DateRangeWithCount":
        return cls(DateRange(start_date, start_date), row_count)
    
    def can_add(self, row_count: int, target_rows: int) -> bool:
        return self.row_count + row_count <= target_rows
    
    def extend_to(self, end_date: date, additional_rows: int) -> "DateRangeWithCount":
        extended_range = DateRange(self.range.start, end_date)
        return DateRangeWithCount(extended_range, self.row_count + additional_rows)


@dataclass(frozen=True)
class BatchAccumulator:
    current: DateRangeWithCount | None
    target_rows: int
    completed_ranges: tuple[DateRange, ...]
    
    @classmethod
    def create(cls, target_rows: int) -> "BatchAccumulator":
        return cls(None, target_rows, ())
    
    def process(self, current_date: date, row_count: int) -> "BatchAccumulator":
        if self.current is None:
            new_current = DateRangeWithCount.start_new(current_date, row_count)
            return BatchAccumulator(new_current, self.target_rows, self.completed_ranges)
        elif self.current.can_add(row_count, self.target_rows):
            new_current = self.current.extend_to(current_date, row_count)
            return BatchAccumulator(new_current, self.target_rows, self.completed_ranges)
        else:
            new_completed = self.completed_ranges + (self.current.range,)
            new_current = DateRangeWithCount.start_new(current_date, row_count)
            return BatchAccumulator(new_current, self.target_rows, new_completed)
    
    def finalize(self) -> list[DateRange]:
        if self.current:
            return list(self.completed_ranges) + [self.current.range]
        return list(self.completed_ranges)


class DateRangeRepository:
    
    @staticmethod
    def get_min_max(engine: Engine, table: str, date_column: str) -> tuple[date | None, date | None]:
        query = text(f"""
            SELECT MIN({date_column})::date, MAX({date_column})::date
            FROM {table}
        """)
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return (result[0], result[1]) if result and result[0] else (None, None)
    
    @staticmethod
    def get_min(engine: Engine, table: str, date_column: str) -> date | None:
        """Get minimum date only - faster than get_min_max when only MIN needed."""
        query = text(f"SELECT MIN({date_column})::date FROM {table}")
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result[0] if result else None
    
    @staticmethod
    def get_max(engine: Engine, table: str, date_column: str) -> date | None:
        """Get maximum date only - faster than get_min_max when only MAX needed."""
        query = text(f"SELECT MAX({date_column})::date FROM {table}")
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result[0] if result else None
    
    @staticmethod
    def get_counts_by_date(engine: Engine, table: str, date_column: str) -> list[tuple[date, int]]:
        query = text(f"""
            SELECT {date_column}::date, COUNT(*)::bigint
            FROM {table}
            GROUP BY {date_column}::date
            ORDER BY {date_column}::date
        """)
        with engine.connect() as conn:
            return [(row[0], row[1]) for row in conn.execute(query)]


class VolumeBasedStrategy:
    """Generate batches based on row volume to reach target size."""
    
    def __init__(self, target_rows: int = 100000):
        self.target_rows = target_rows
    
    def __str__(self) -> str:
        return f"VolumeBasedStrategy(target_rows={self.target_rows:,})"
    
    def generate_batches(
        self, 
        engine: Engine, 
        table: str, 
        date_column: str
    ) -> list[tuple[str, str]]:
        date_counts = DateRangeRepository.get_counts_by_date(engine, table, date_column)
        
        if not date_counts:
            return []
        
        return self._create_batches_from_counts(date_counts)
    
    def _create_batches_from_counts(self, date_counts: list[tuple[date, int]]) -> list[tuple[str, str]]:
        accumulator = BatchAccumulator.create(self.target_rows)
        
        for current_date, row_count in date_counts:
            accumulator = accumulator.process(current_date, row_count)
        
        ranges = accumulator.finalize()
        return [r.to_tuple() for r in ranges]


class FixedIntervalStrategy:
    """Generate batches based on fixed time intervals.
    
    Args:
        interval: Number of time units per batch
        unit: Time unit (DAY, WEEK, MONTH, YEAR)
        min_date: Optional minimum date (skips DB scan if provided)
        max_date: Optional maximum date (skips DB scan if provided, defaults to today)
    """
    
    def __init__(
        self, 
        interval: int = 1, 
        unit: IntervalUnit = IntervalUnit.DAY,
        min_date: str | date | None = None,
        max_date: str | date | None = None
    ):
        self.interval = interval
        self.unit = unit
        self._min_date = self._parse_date(min_date) if min_date else None
        self._max_date = self._parse_date(max_date) if max_date else None
    
    @staticmethod
    def _parse_date(value: str | date) -> date:
        """Parse date from string or return date as-is."""
        if isinstance(value, date):
            return value
        return date.fromisoformat(value)
    
    def __str__(self) -> str:
        return f"FixedIntervalStrategy(interval={self.interval}, unit={self.unit.value})"
    
    def generate_batches(
        self, 
        engine: Engine, 
        table: str, 
        date_column: str
    ) -> list[tuple[str, str]]:
        # Use provided dates - NO DB scan if dates are provided
        # max_date=None means "today", not "fetch from DB"
        min_date: date | None
        max_date: date | None
        
        if self._min_date and self._max_date:
            # Both provided - no DB scan
            min_date, max_date = self._min_date, self._max_date
        elif self._min_date:
            # Min provided, max not provided → use today (NO DB scan)
            min_date = self._min_date
            max_date = date.today()
        elif self._max_date:
            # Only max provided - need to fetch min from DB
            min_date = DateRangeRepository.get_min(engine, table, date_column)
            max_date = self._max_date
        else:
            # Neither provided - fetch both from DB
            min_date, max_date = DateRangeRepository.get_min_max(engine, table, date_column)
        
        if not min_date or not max_date:
            return []
        
        ranges = self._create_intervals(min_date, max_date)
        return [r.to_tuple() for r in ranges]
    
    def _create_intervals(self, start: date, end: date) -> list[DateRange]:
        ranges: list[DateRange] = []
        delta = self.unit.get_delta(self.interval)
        
        current_range = DateRange.from_start_and_delta(start, delta).cap_at(end)
        ranges.append(current_range)
        
        while current_range.is_end_date_before(end):
            current_range = current_range.shift().cap_at(end)
            ranges.append(current_range)
        
        return ranges
