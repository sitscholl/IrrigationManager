import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from datetime import datetime

from src.database.models import Irrigation

class FieldIrrigation:
    
    def __init__(self, field_id: str, dates: list[datetime], amounts: list[float]):


        dates_re = []
        for date in dates:
            if not isinstance(date, datetime):
                try:
                    date = pd.to_datetime(date)
                except Exception as e:
                    raise ValueError(f'Error transforming input dates to datetime for FieldIrrigation: {e}')
            dates_re.append(date)

        if len(self.dates) != len(self.amounts):
            raise ValueError(f'Dates and amounts must have the same length. Got {len(self.dates)} dates and {len(self.amounts)} amounts.')

        self.field_id = field_id
        self.dates = dates_re
        self.amounts = amounts

    @staticmethod
    def from_list(irrigation_events: list[Irrigation]):
        field_id = set(i.field_id for i in irrigation_events)
        if len(field_id) > 1:
            raise ValueError('Multiple fields found. Cannot initialize FieldIrrigation from list of irrigation events')
        if len(field_id) == 0:
            raise ValueError("No field_id found in list of irrigation events.")

        field_id = list(field_id)[0]
        irrigation_dates = [i.date for i in irrigation_events]
        irrigation_amounts = [i.amount for i in irrigation_events]

        return FieldIrrigation(field_id, irrigation_dates, irrigation_amounts)

    def to_dataframe(self, index: pd.DatetimeIndex, fill_value = 0.0):

        if not isinstance(index, pd.DatetimeIndex):
            raise ValueError('Index must be a pandas DatetimeIndex.')
        # if not is_datetime64_any_dtype(index):
        #     raise ValueError(f"index must contain datetime values. Got {index.dtype} instead.")

        irr_df = pd.DataFrame(
            {
                "date": self.dates,
                "amount": self.amounts,
            }
        ).set_index('date')

        irr_df = irr_df.sort_index()
        target_tz = index.tz
        if target_tz is not None:
            if irr_df.index.tz is None:
                irr_df.index = irr_df.index.tz_localize(target_tz)
            else:
                irr_df.index = irr_df.index.tz_convert(target_tz)
        elif irr_df.index.tz is not None:
            irr_df.index = irr_df.index.tz_localize(None)

        daily = irr_df['amount'].fillna(0.0).groupby(irr_df.index.normalize()).sum()
        aligned = daily.reindex(index.normalize(), fill_value=fill_value)
        aligned.index = index
        return aligned.astype(float)