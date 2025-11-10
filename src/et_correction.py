from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Mapping, Sequence

import pandas as pd


@dataclass(frozen=True)
class KcPeriod:
    """Single crop-coefficient segment."""
    name: str
    value: float
    start: str
    end: str | None = None

    @classmethod
    def from_spec(cls, spec: Mapping[str, object]) -> "KcPeriod":
        """Create from dict-like config entries."""
        return cls(
            name=str(spec["name"]),
            value=float(spec["value"]),
            start=pd.to_datetime(spec["start"], dayfirst=True),
            end=pd.to_datetime(spec.get("end"), dayfirst=True) if spec.get("end") is not None else None,
        )

class ETCorrection:
    """Builds crop-coefficient correction curves from Kc periods."""

    def __init__(
        self,
        periods: Sequence[KcPeriod | Mapping[str, object]],
        season_end: datetime | str | None = None,
    ) -> None:

        if not periods:
            raise ValueError("At least one KcPeriod is required.")

        normalized = [p if isinstance(p, KcPeriod) else KcPeriod.from_spec(p) for p in periods]
        normalized.sort(key=lambda p: p.start)

        self._periods = self._attach_end_dates(normalized, season_end)

    @property
    def dataframe(self) -> pd.DataFrame:
        """Tabular view of the correction periods."""
        return pd.DataFrame(self._periods)

    def as_daily_series(
        self,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
    ) -> pd.Series:
        """Return a daily step series spanning [start, end)."""

        frame = self.dataframe

        start_ts = pd.Timestamp(start) if start else frame["start"].min().normalize()
        end_ts = pd.Timestamp(end) if end else frame["end"].max().normalize()

        if start_ts.tzinfo is not None:
            frame['start'] = frame['start'].dt.tz_localize(start_ts.tzinfo)
            frame['end'] = frame['end'].dt.tz_localize(start_ts.tzinfo)

        daily_index = pd.date_range(start_ts, end_ts, freq="D")
        step_points = frame.set_index("start")["value"]
        kc = step_points.reindex(daily_index, method="pad").rename("kc")

        return kc

    def as_dayofyear_series(
        self, start: int, end: int, anchor_year: int
    ) -> pd.Series:
        """Return a daily step series spanning [start, end)."""

        start = date(anchor_year, 1, 1) + timedelta(days=start - 1)
        end = date(anchor_year, 1, 1) + timedelta(days=end - 1)

        daily = self.as_daily_series(start, end)
        daily.index = daily.index.dayofyear

        return daily

    def to_series(
        self, target_index: pd.Index, anchor_year: int | None = None
    ) -> pd.Series:
        """Align correction factors to any monotonic index."""
        
        if isinstance(target_index, pd.DatetimeIndex):
            daily = self.as_daily_series(target_index.min().normalize(), target_index.max().normalize())
            return daily.reindex(target_index, method="pad").rename("kc")
        elif isinstance(target_index, pd.RangeIndex):
            doy_series = self.as_dayofyear_series(target_index.min(), target_index.max(), anchor_year)
            return doy_series.reindex(target_index, method="pad").rename("kc")

        raise TypeError("target_index must be a pandas DatetimeIndex or RangeIndex.")

    def apply_to(
        self, frame: pd.DataFrame, column: str,
    ) -> pd.DataFrame:
        """Multiply ET0 values in `column` by the correction curve."""

        kc = self.to_series(frame.index)
        corrected = frame.copy()
        corrected["kc"] = kc
        corrected[f"{column}_corrected"] = corrected[column] * kc

        return corrected

    @staticmethod
    def _attach_end_dates(
        periods: Sequence[KcPeriod],
        season_end: datetime | str | None,
    ) -> list[dict[str, object]]:

        season_end_ts = pd.to_datetime(season_end, dayfirst = True) if season_end else None

        resolved: list[dict[str, object]] = []
        for idx, period in enumerate(periods):
            next_start = periods[idx + 1].start if idx + 1 < len(periods) else None
            end = period.end or next_start or season_end_ts

            if end is None:
                end = period.start.replace(day = 31, month = 12)

            resolved.append(
                {
                    "name": period.name,
                    "value": period.value,
                    "start": period.start,
                    "end": pd.Timestamp(end),
                }
            ) #store as dict to facilitate transformation to dataframe
        return resolved

if __name__ == '__main__':
    periods = [
    {"name": "Kc_ini", "value": 0.30, "start": "01-04-2024"},
    {"name": "Kc_mid", "value": 1.10, "start": "01-06-2024"},
    {"name": "Kc_end", "value": 0.65, "start": "01-07-2024"},
    ]

    corrector = ETCorrection(periods, season_end="01-10-2024")
    kc = corrector.to_series(pd.date_range("20240101", "20241030"))
    kc_doy = corrector.to_series(pd.RangeIndex(1, 365), anchor_year=2024)

    print(kc)