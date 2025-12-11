import pandas as pd

from src.database.models import Irrigation

class FieldIrrigation:
    
    def __init__(self):
        pass

    def from_list(self, irrigation_events: list[Irrigation]):
        pass

    def align_with_index(self, index: pd.Series, fill_value = 0):
        pass